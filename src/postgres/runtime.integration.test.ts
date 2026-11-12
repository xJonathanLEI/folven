import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import { classifyFailure } from "./failures.js";
import { createPostgresRuntime } from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const rename = defineEvent({
	name: "author.rename",
	version: 1,
	stream: author,
	schema: z.strictObject({
		id: z.string(),
		expectedName: z.string(),
		name: z.string(),
	}),
	route: (payload) => ({ id: payload.id }),
});
const options = { maxWait: 5_000, timeout: 10_000 };

test("dispatch commits event and command state together, journals normal rejection, and rolls back throws", async ({
	database,
}) => {
	const client = database.client("dispatch");
	await client.user.create({
		data: { id: "u1", email: "author@example.test", name: "Original" },
	});
	const failure = new Error("Synthetic handler failure after mutation");
	let shouldThrow = false;
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope, event }) => {
			const user = await db.user.findUniqueOrThrow({
				where: scope.user.unique,
			});
			if (user.name !== event.expectedName)
				return { status: "rejected" as const };
			const saved = await db.user.update({
				where: scope.user.unique,
				data: { name: event.name },
				select: { name: true },
			});
			if (shouldThrow) throw failure;
			return { status: "renamed" as const, saved };
		},
	});
	const registry = definitions.registry({
		streams: [author],
		events: [rename],
		handlers: [handler],
	});
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		transaction: options,
	});
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	const input = { id: "u1", expectedName: "Original", name: "Renamed" };
	expect(await runtime.dispatch(rename, input)).toEqual({
		status: "renamed",
		saved: { name: "Renamed" },
	});
	expect(await runtime.dispatch(rename, input)).toEqual({ status: "rejected" });
	expect(await runtime.dispatch(rename, input)).toEqual({ status: "rejected" });
	// Normal dispatch has one reservation and no journal lookup per attempt.
	expect(queries.filter((query) => query.includes('"event_log"'))).toHaveLength(
		3,
	);
	expect(queries.filter((query) => query.includes('"outgoing_event"'))).toEqual(
		[],
	);
	expect(
		queries
			.filter((query) => query.includes('"event_log"'))
			.every((query) => query.startsWith("INSERT")),
	).toBe(true);
	const history = z
		.array(
			z.object({
				occurrence_id: z.uuid(),
				stream_sequence: z.string(),
				payload: z.string(),
			}),
		)
		.parse(
			(
				await database.admin.query<Record<string, unknown>>(
					`SELECT occurrence_id, stream_sequence, payload FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
				)
			).rows,
		);
	expect(history.map((row) => row.stream_sequence)).toEqual(["1", "2", "3"]);
	expect(new Set(history.map((row) => row.occurrence_id)).size).toBe(3);
	expect(history.map((row) => row.payload)).toEqual([
		JSON.stringify(input),
		JSON.stringify(input),
		JSON.stringify(input),
	]);
	shouldThrow = true;
	await expect(
		runtime.dispatch(rename, {
			...input,
			expectedName: "Renamed",
			name: "Rolled back",
		}),
	).rejects.toBe(failure);
	expect(
		await client.user.findUnique({
			where: { id: "u1" },
			select: { name: true },
		}),
	).toEqual({ name: "Renamed" });
	expect(
		(
			await database.admin.query<Record<string, unknown>>(
				`SELECT occurrence_id FROM "${database.journalSchema}".event_log`,
			)
		).rows,
	).toHaveLength(3);
});

test("dispatch returns only after a real deferred commit completes", async ({
	database,
}) => {
	const client = database.client("committing");
	const gateClient = database.client("gate");
	await client.user.createMany({
		data: [
			{ id: "u1", email: "author@example.test", name: "Original" },
			{ id: "gate", email: "gate@example.test", name: "Commit gate" },
		],
	});
	// Test-only deferred trigger: a row lock delays COMMIT *after* the callback
	// has returned. No execution lock or trigger is part of the runtime's DDL.
	await database.admin.query(`
		CREATE FUNCTION "${database.commandSchema}".hold_commit() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM id FROM "${database.commandSchema}".users WHERE id = 'gate' FOR UPDATE;
			RETURN NEW;
		END;
		$$;
		CREATE CONSTRAINT TRIGGER hold_commit AFTER UPDATE ON "${database.commandSchema}".users
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.id = 'u1')
		EXECUTE FUNCTION "${database.commandSchema}".hold_commit();
	`);
	const locked = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const callbackReturned = Promise.withResolvers<void>();
	const gate = settle(
		gateClient.$transaction(
			async (tx) => {
				await tx.$queryRawUnsafe(
					`SELECT id FROM "${database.commandSchema}".users WHERE id = $1 FOR UPDATE`,
					"gate",
				);
				locked.resolve();
				await release.promise;
			},
			{ timeout: 12_000 },
		),
	);
	const result = { callback: () => 7n };
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope, event, emit }) => {
			await db.user.update({
				where: scope.user.unique,
				data: { name: event.name },
			});
			emit(rename, { ...event, expectedName: event.name, name: "Later" });
			callbackReturned.resolve();
			return result;
		},
	});
	const registry = definitions.registry({
		streams: [author],
		events: [rename],
		handlers: [handler],
	});
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		transaction: options,
	});
	let returned = false;
	let pending: ReturnType<typeof settle<typeof result>> | undefined;
	try {
		await bounded(locked.promise);
		pending = settle(
			runtime
				.dispatch(rename, {
					id: "u1",
					expectedName: "Original",
					name: "Committed",
				})
				.then((value) => {
					returned = true;
					return value;
				}),
		);
		await bounded(callbackReturned.promise);
		await database.waitForBlock("gate", "committing", "COMMIT");
		expect(returned).toBe(false);
		expect(
			(
				await database.admin.query(
					`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event`,
				)
			).rows,
		).toEqual([]);
		expect(
			(
				await database.admin.query(
					`SELECT name FROM "${database.commandSchema}".users WHERE id = 'u1'`,
				)
			).rows,
		).toEqual([{ name: "Original" }]);
		expect(
			(
				await database.admin.query(
					`SELECT occurrence_id FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([]);
		release.resolve();
		expect(await bounded(gate)).toMatchObject({ status: "fulfilled" });
		const completed = await bounded(pending);
		expect(completed.status).toBe("fulfilled");
		if (completed.status !== "fulfilled")
			throw new Error("Deferred commit failed");
		expect(completed.value).toBe(result);
		expect(completed.value.callback()).toBe(7n);
		expect(returned).toBe(true);
		expect(
			(
				await database.admin.query(
					`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event`,
				)
			).rows,
		).toEqual([{ channel_sequence: "1" }]);
		expect(
			(
				await database.admin.query(
					`SELECT name FROM "${database.commandSchema}".users WHERE id = 'u1'`,
				)
			).rows,
		).toEqual([{ name: "Committed" }]);
		expect(
			(
				await database.admin.query(
					`SELECT stream_sequence FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ stream_sequence: "1" }]);
	} finally {
		release.resolve();
		await Promise.all([gate, pending]);
	}
});

test("a real row-lock deadlock is an aborted transaction, not an uncertain acknowledgment", async ({
	database,
}) => {
	const client = database.client("deadlock");
	const external = database.client("external");
	await client.user.createMany({
		data: ["a", "b"].map((id) => ({
			id,
			email: `${id}@example.test`,
			name: id,
			age: 0,
		})),
	});
	const allUsers = definitions.stream({
		name: "all-users",
		key: z.strictObject({}),
		owns: { user: { kind: "table" } },
	});
	const touch = defineEvent({
		name: "users.touch",
		version: 1,
		stream: allUsers,
		schema: z.strictObject({}),
		route: () => ({}),
	});
	const externalLocked = Promise.withResolvers<void>();
	const handlerLocked = Promise.withResolvers<void>();
	const releaseHandler = Promise.withResolvers<void>();
	const failures: unknown[] = [];
	let attempts = 0;
	const handler = defineHandler({
		event: touch,
		handle: async ({ db, scope }) => {
			attempts++;
			try {
				await db.user.update({
					where: { ...scope.user.where, id: "a" },
					data: { age: { increment: 1 } },
				});
				handlerLocked.resolve();
				await releaseHandler.promise;
				await db.user.update({
					where: { ...scope.user.where, id: "b" },
					data: { age: { increment: 1 } },
				});
			} catch (error) {
				failures.push(error);
				throw error;
			}
		},
	});
	const registry = definitions.registry({
		streams: [allUsers],
		events: [touch],
		handlers: [handler],
	});
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		transaction: options,
	});
	// An independent administrative transaction takes the same rows in reverse
	// order. Either PostgreSQL-selected victim is acceptable; never choose it by timing.
	const other = settle(
		external.$transaction(
			async (tx) => {
				await tx.$queryRawUnsafe(
					`SELECT id FROM "${database.commandSchema}".users WHERE id = $1 FOR UPDATE`,
					"b",
				);
				externalLocked.resolve();
				await handlerLocked.promise;
				await tx.$queryRawUnsafe(
					`SELECT id FROM "${database.commandSchema}".users WHERE id = $1 FOR UPDATE`,
					"a",
				);
			},
			{ timeout: 12_000 },
		),
	);
	let pending: ReturnType<typeof settle<void>> | undefined;
	try {
		await bounded(externalLocked.promise);
		pending = settle(runtime.dispatch(touch, {}));
		await bounded(handlerLocked.promise);
		await database.waitForBlock("deadlock", "external", "SELECT id");
		releaseHandler.resolve();
		const externalOutcome = await bounded(other);
		if (externalOutcome.status === "rejected") {
			failures.push(externalOutcome.reason);
			expect(externalOutcome.reason).toMatchObject({
				code: "P2010",
				meta: { driverAdapterError: { cause: { originalCode: "40P01" } } },
			});
		}
		expect(await bounded(pending)).toMatchObject({ status: "fulfilled" });
		expect(failures).toHaveLength(1);
		for (const failure of failures)
			expect(classifyFailure(failure, "handler")).toBe("retry");
		expect(attempts).toBe(externalOutcome.status === "rejected" ? 1 : 2);
		expect(
			await client.user.findMany({
				select: { age: true },
				orderBy: { id: "asc" },
			}),
		).toEqual([{ age: 1 }, { age: 1 }]);
		expect(
			(
				await database.admin.query(
					`SELECT stream_sequence FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ stream_sequence: "1" }]);
	} finally {
		handlerLocked.resolve();
		releaseHandler.resolve();
		await Promise.all([other, pending]);
	}
});

test("business-table uniqueness is not retried or mistaken for journal contention", async ({
	database,
}) => {
	const client = database.client("business-unique");
	await client.user.createMany({
		data: [
			{ id: "u1", email: "one@example.test", name: "One" },
			{ id: "u2", email: "two@example.test", name: "Two" },
		],
	});
	const invoked = vi.fn();
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope }) => {
			invoked();
			return db.user.update({
				where: scope.user.unique,
				data: { email: "two@example.test" },
			});
		},
	});
	const registry = definitions.registry({
		streams: [author],
		events: [rename],
		handlers: [handler],
	});
	await expect(
		createPostgresRuntime({
			client,
			registry,
			schema: database.journalSchema,
		}).dispatch(rename, {
			id: "u1",
			expectedName: "One",
			name: "Ignored",
		}),
	).rejects.toMatchObject({ code: "P2002" });
	expect(invoked).toHaveBeenCalledTimes(1);
	expect(
		(
			await database.admin.query<Record<string, unknown>>(
				`SELECT occurrence_id FROM "${database.journalSchema}".event_log`,
			)
		).rows,
	).toEqual([]);
	expect(
		await client.user.findUnique({
			where: { id: "u1" },
			select: { email: true },
		}),
	).toEqual({ email: "one@example.test" });
});
