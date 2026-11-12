import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import {
	createDefinitions,
	defineEvent,
	defineHandler,
	type EventEmitter,
} from "../index.js";
import {
	createPostgresRuntime,
	EmissionClosedError,
	EmissionFailedError,
	type OutgoingEnvelope,
	type PostgresPublication,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});

test("publication shares source visibility and rollback; emitting ingestion publishes once after commit", async ({
	database,
}) => {
	const client = database.client("publication");
	const observer = database.client("observer");
	await client.user.create({
		data: { id: "a", email: "a@example.test", name: "Original", age: 0 },
	});
	// A generic publisher's test-owned table, not a scheduler or Folven DDL.
	await database.admin.query(`
		CREATE TABLE "${database.journalSchema}".publication (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			outgoing_references jsonb NOT NULL
		)
	`);
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope, event, emit }) => {
			await db.user.update({
				where: scope.user.unique,
				data: { age: { increment: 1 }, name: event.name },
			});
			emit(comment, input);
			return "renamed";
		},
	});
	let fail = true;
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const notified = vi.fn<() => Promise<void>>(async () => {
		// Independent observer sees all three writes before notification.
		expect(
			await observer.user.findUnique({ where: { id: "a" } }),
		).toMatchObject({ name: "Renamed", age: 1 });
		for (const table of ["event_log", "outgoing_event", "publication"]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${database.journalSchema}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 1 }]);
		}
	});
	const prepare = vi.fn<PostgresPublication["prepare"]>(({ source }) => ({
		sql: `INSERT INTO "${database.journalSchema}".publication (outgoing_references)
			SELECT "${source.payloadColumn}"::jsonb FROM "${source.relation}" RETURNING id::text`,
		parameters: [],
		async validate(rows, count) {
			expect(rows).toHaveLength(count);
			if (fail) {
				entered.resolve();
				await bounded(release.promise);
				throw new Error("Synthetic publication failure after staging");
			}
			return notified;
		},
	}));
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({
			streams: [author, comments],
			events: [rename, comment],
			handlers: [handler],
		}),
		publication: { prepare },
	});
	const failed = settle(runtime.dispatch(rename, { id: "a", name: "Renamed" }));
	try {
		await bounded(entered.promise);
		expect(
			await observer.user.findUnique({ where: { id: "a" } }),
		).toMatchObject({ name: "Original", age: 0 });
		for (const table of ["event_log", "outgoing_event", "publication"]) {
			expect(
				(
					await database.admin.query(
						`SELECT * FROM "${database.journalSchema}"."${table}"`,
					)
				).rows,
			).toEqual([]);
		}
	} finally {
		release.resolve();
		await failed;
	}
	expect(await failed).toMatchObject({
		status: "rejected",
		reason: new Error("Synthetic publication failure after staging"),
	});
	expect(notified).not.toHaveBeenCalled();
	expect(await observer.user.findUnique({ where: { id: "a" } })).toMatchObject({
		name: "Original",
		age: 0,
	});
	for (const table of ["event_log", "outgoing_event", "publication"]) {
		expect(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}"."${table}"`,
				)
			).rows,
		).toEqual([]);
	}
	fail = false;
	const envelope = {
		source: { streamName: "post", streamKey: '{"id":"p"}' },
		target: { streamName: "author", streamKey: '{"id":"a"}' },
		channelSequence: "1",
		eventName: rename.name,
		eventVersion: rename.version,
		payloadFormat: "json-v1",
		encodedInput: JSON.stringify({ id: "a", name: "Renamed" }),
	} satisfies OutgoingEnvelope;
	expect(await runtime.ingest(envelope)).toEqual({ status: "processed" });
	expect(await runtime.ingest(envelope)).toEqual({
		status: "alreadyProcessed",
	});
	expect(await runtime.ingest({ ...envelope, channelSequence: "3" })).toEqual({
		status: "gap",
		expectedSequence: "2",
	});
	expect(prepare).toHaveBeenCalledTimes(2);
	expect(notified).toHaveBeenCalledTimes(1);
	// Runtime intentionally swallows notification failures; surface assertions.
	await notified.mock.results[0]?.value;
});
const comments = definitions.stream({
	name: "comment",
	key: z.strictObject({ postId: z.string(), number: z.int() }),
	owns: {
		comment: {
			kind: "unique",
			selector: "postId_number",
			fields: { postId: "postId", number: "number" },
		},
	},
});
const rename = defineEvent({
	name: "author.rename",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), name: z.string() }),
	route: (payload) => ({ id: payload.id }),
});
const commentSchema = z.strictObject({
	postId: z.string(),
	number: z.int(),
	body: z.string().trim().min(1),
	at: z.iso.datetime().transform((value) => new Date(value)),
});
const route = vi.fn((payload: z.output<typeof commentSchema>) => {
	expect(payload.at).toBeInstanceOf(Date);
	return { postId: payload.postId, number: payload.number };
});
const comment = defineEvent({
	name: "comment.add",
	version: 1,
	stream: comments,
	schema: commentSchema,
	route,
});
const commentV2 = defineEvent({
	name: "comment.add",
	version: 2,
	stream: comments,
	schema: commentSchema,
	route,
});
const input = {
	postId: "post",
	number: 1,
	body: " Original ",
	at: "2026-01-01T00:00:00.000Z",
};
const outputRows = z.array(
	z.object({
		source_stream_name: z.string(),
		source_stream_key: z.string(),
		target_stream_name: z.string(),
		target_stream_key: z.string(),
		channel_sequence: z.string(),
		event_name: z.string(),
		event_version: z.string(),
		payload_format: z.literal("json-v1"),
		payload: z.string(),
		acknowledged: z.boolean(),
	}),
);

test("emission snapshots transformed input without SQL, batches channel order and never calls a local target", async ({
	database,
}) => {
	const client = database.client("source");
	await client.user.create({
		data: { id: "a", email: "a@example.test", name: "Original" },
	});
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	const targetInvoked = vi.fn<() => void>();
	const target = defineHandler({
		event: comment,
		handle: () => targetInvoked(),
	});
	let savedEmitter: EventEmitter | undefined;
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope, event, emit }) => {
			savedEmitter = emit;
			await db.user.update({
				where: scope.user.unique,
				data: { name: event.name },
			});
			const before = [...queries];
			const mutable = { ...input };
			emit(comment, mutable);
			mutable.postId = "other";
			mutable.body = "Changed";
			emit(comment, mutable);
			emit(commentV2, input);
			expect(queries).toEqual(before);
			return { value: () => 7n };
		},
	});
	// The first transition knows only the target's metadata. The second also
	// installs its handler locally; neither is permission to execute it.
	const registered = {
		streams: [author, comments],
		events: [rename, comment, commentV2],
	} as const;
	for (const registry of [
		definitions.registry({ ...registered, handlers: [handler] }),
		definitions.registry({ ...registered, handlers: [handler, target] }),
	]) {
		const runtime = createPostgresRuntime({
			client,
			schema: database.journalSchema,
			registry,
		});
		route.mockClear();
		expect(
			(await runtime.dispatch(rename, { id: "a", name: "Renamed" })).value(),
		).toBe(7n);
		expect(route).toHaveBeenCalledTimes(3);
		expect(() => savedEmitter?.(comment, input)).toThrow(EmissionClosedError);
	}
	expect(targetInvoked).not.toHaveBeenCalled();
	expect(await client.comment.count()).toBe(0);
	expect(await client.user.findUnique({ where: { id: "a" } })).toMatchObject({
		name: "Renamed",
	});
	const rows = outputRows.parse(
		(
			await database.admin.query(
				`SELECT * FROM "${database.journalSchema}".outgoing_event ORDER BY target_stream_key, channel_sequence`,
			)
		).rows,
	);
	expect(
		rows.map((row) => [
			row.target_stream_key,
			row.channel_sequence,
			row.event_version,
			row.payload,
		]),
	).toEqual([
		[
			'{"number":1,"postId":"other"}',
			"1",
			"1",
			JSON.stringify({ ...input, postId: "other", body: "Changed" }),
		],
		[
			'{"number":1,"postId":"other"}',
			"2",
			"1",
			JSON.stringify({ ...input, postId: "other", body: "Changed" }),
		],
		...["1", "2", "3", "4"].map((sequence, index) => [
			'{"number":1,"postId":"post"}',
			sequence,
			index % 2 === 0 ? "1" : "2",
			JSON.stringify(input),
		]),
	]);
	for (const row of rows) {
		expect(row).toMatchObject({
			source_stream_name: "author",
			source_stream_key: '{"id":"a"}',
			target_stream_name: "comment",
			event_name: "comment.add",
			payload_format: "json-v1",
			acknowledged: false,
		});
	}
	// One append and one set-based flush per transition, without read-back.
	expect(
		queries.filter((query) => query.includes('"outgoing_event"')),
	).toHaveLength(2);
	expect(queries.filter((query) => query.includes('"event_log"'))).toHaveLength(
		2,
	);
});

test.for(["throw", "schema", "flush", "commit"] as const)(
	"%s failure rolls back source state, journal and outputs, closes the emitter and consumes no positions",
	async (failureKind, { database }) => {
		const client = database.client("rollback");
		await client.user.create({
			data: { id: "a", email: "a@example.test", name: "Original" },
		});
		if (failureKind === "flush" || failureKind === "commit") {
			// Administrative fault injection, not a runtime trigger. The deferred
			// variant fails only after the handler and output insertion succeeded.
			await database.admin.query(`
				CREATE FUNCTION "${database.journalSchema}".reject_output() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN
					RAISE EXCEPTION 'Synthetic output failure' USING ERRCODE = '23514';
				END; $$;
				CREATE CONSTRAINT TRIGGER reject_output AFTER INSERT
				ON "${database.journalSchema}".outgoing_event
				DEFERRABLE INITIALLY ${failureKind === "commit" ? "DEFERRED" : "IMMEDIATE"}
				FOR EACH ROW EXECUTE FUNCTION "${database.journalSchema}".reject_output();
			`);
		}
		let fail = true;
		let savedEmitter: EventEmitter | undefined;
		const handlerFailure = new Error("Synthetic handler failure");
		const invoked = vi.fn();
		const handler = defineHandler({
			event: rename,
			handle: async ({ db, scope, event, emit }) => {
				invoked();
				savedEmitter = emit;
				await db.user.update({
					where: scope.user.unique,
					data: { name: event.name },
				});
				emit(comment, input);
				if (!fail) return;
				if (failureKind === "throw") throw handlerFailure;
				// Deliberately swallow invalid emissions: finish must still refuse
				// the entire transition, including the preceding valid output.
				try {
					if (failureKind === "schema") emit(comment, { ...input, body: "" });
				} catch {
					return;
				}
			},
		});
		const runtime = createPostgresRuntime({
			client,
			schema: database.journalSchema,
			registry: definitions.registry({
				streams: [author, comments],
				events: [rename, comment],
				handlers: [handler],
			}),
		});
		const result = runtime.dispatch(rename, { id: "a", name: "Renamed" });
		if (failureKind === "throw")
			await expect(result).rejects.toBe(handlerFailure);
		else if (failureKind === "flush")
			await expect(result).rejects.toMatchObject({
				code: "P2010",
				meta: { driverAdapterError: { cause: { originalCode: "23514" } } },
			});
		else if (failureKind === "commit")
			await expect(result).rejects.toMatchObject({
				name: "DriverAdapterError",
				cause: { originalCode: "23514" },
			});
		else await expect(result).rejects.toBeInstanceOf(EmissionFailedError);
		expect(invoked).toHaveBeenCalledTimes(1);
		expect(() => savedEmitter?.(comment, input)).toThrow(EmissionClosedError);
		expect(await client.user.findUnique({ where: { id: "a" } })).toMatchObject({
			name: "Original",
		});
		for (const table of ["event_log", "outgoing_event"]) {
			expect(
				(
					await database.admin.query(
						`SELECT * FROM "${database.journalSchema}"."${table}"`,
					)
				).rows,
			).toEqual([]);
		}
		if (failureKind === "flush" || failureKind === "commit") {
			await database.admin.query(
				`DROP TRIGGER reject_output ON "${database.journalSchema}".outgoing_event`,
			);
		}
		fail = false;
		await runtime.dispatch(rename, { id: "a", name: "Renamed" });
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
					`SELECT stream_sequence FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ stream_sequence: "1" }]);
	},
);

test("a known commit abort discards the flushed batch and retries with fresh payload and collector", async ({
	database,
}) => {
	const client = database.client("retry");
	await client.user.create({
		data: { id: "a", email: "a@example.test", name: "Original", age: 0 },
	});
	// PostgreSQL sequences intentionally survive rollback: exactly the first
	// commit aborts *after* output insertion. No production trigger is installed.
	await database.admin.query(`
		CREATE SEQUENCE "${database.journalSchema}".commit_attempt;
		CREATE FUNCTION "${database.journalSchema}".abort_once() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			IF nextval('"${database.journalSchema}".commit_attempt') = 1 THEN
				RAISE EXCEPTION 'Synthetic serialization abort' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END; $$;
		CREATE CONSTRAINT TRIGGER abort_once AFTER INSERT
		ON "${database.journalSchema}".outgoing_event DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION "${database.journalSchema}".abort_once();
	`);
	const emitters: EventEmitter[] = [];
	const handler = defineHandler({
		event: rename,
		handle: async ({ db, scope, event, emit }) => {
			for (const previous of emitters)
				expect(() => previous(comment, input)).toThrow(EmissionClosedError);
			emitters.push(emit);
			expect(event.name).toBe("Renamed");
			await db.user.update({
				where: scope.user.unique,
				data: { age: { increment: 1 }, name: event.name },
			});
			emit(comment, { ...input, body: `Attempt ${emitters.length}` });
			event.name = "Mutation must not survive retry";
			return { value: () => 7n };
		},
	});
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({
			streams: [author, comments],
			events: [rename, comment],
			handlers: [handler],
		}),
	});
	const queries: Array<{ query: string; params: string }> = [];
	client.$on("query", ({ query, params }) => queries.push({ query, params }));
	expect(
		(await runtime.dispatch(rename, { id: "a", name: "Renamed" })).value(),
	).toBe(7n);
	expect(emitters).toHaveLength(2);
	expect(emitters[0]).not.toBe(emitters[1]);
	for (const emit of emitters)
		expect(() => emit(comment, input)).toThrow(EmissionClosedError);
	expect(await client.user.findUnique({ where: { id: "a" } })).toMatchObject({
		name: "Renamed",
		age: 1,
	});
	const rows = outputRows.parse(
		(
			await database.admin.query(
				`SELECT * FROM "${database.journalSchema}".outgoing_event`,
			)
		).rows,
	);
	expect(rows).toHaveLength(1);
	expect(rows[0]).toMatchObject({
		channel_sequence: "1",
		payload: JSON.stringify({ ...input, body: "Attempt 2" }),
	});
	const appends = queries.filter(({ query }) => query.includes('"event_log"'));
	expect(appends).toHaveLength(2);
	expect(appends[0]?.params).toBe(appends[1]?.params);
	expect(
		queries.filter(({ query }) => query.includes('"outgoing_event"')),
	).toHaveLength(2);
	expect(
		(
			await database.admin.query(
				`SELECT stream_sequence FROM "${database.journalSchema}".event_log`,
			)
		).rows,
	).toEqual([{ stream_sequence: "1" }]);
});
