import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	bounded,
	settle,
	test,
	type provisionTestDatabase,
} from "../../tests/postgres.js";
import { createEffectRunner, EffectExecutionError } from "../effects/index.js";
import {
	createDefinitions,
	defineEffect,
	defineEffectHandler,
	defineEvent,
	defineHandler,
	eventInput,
	type EffectReceipt,
} from "../index.js";
import {
	createPostgresEffectStore,
	createPostgresRuntime,
	effectId,
	type PostgresPublication,
	type PostgresRawClient,
} from "./index.js";
import type {
	PostgresBorrowedTransaction,
	PostgresTransactionHost,
} from "./types.js";

const definitions = createDefinitions<Pick<Prisma.TransactionClient, "user">>();
const users = definitions.stream({
	name: "users",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const index = defineEffect({
	name: "user.index",
	version: 1,
	schema: z.object({
		id: z.string(),
		at: z.string().transform((value) => new Date(value)),
	}),
});
const request = defineEvent({
	name: "user.request",
	version: 1,
	stream: users,
	schema: z.object({ id: z.string() }),
	route: ({ id }) => ({ id }),
});
const route = vi.fn(({ id }: { id: string }) => ({ id }));
const indexed = defineEvent({
	name: "user.indexed",
	version: 1,
	stream: users,
	schema: z.object({ id: z.string().trim(), value: z.string() }),
	route,
});
const failed = defineEvent({
	name: "user.indexFailed",
	version: 1,
	stream: users,
	schema: z.object({ id: z.string(), reason: z.string() }),
	route: ({ id }) => ({ id }),
});
const metadata = {
	streams: [users],
	events: [request, indexed, failed],
	effects: [index],
} as const;
const producer = definitions.registry({
	...metadata,
	handlers: [
		defineHandler({
			event: request,
			handle({ event, requestEffect }) {
				requestEffect(index, { id: event.id, at: "2026-01-01" });
			},
		}),
	],
});
const completionRegistry = definitions.registry(metadata);
type Database = Awaited<ReturnType<typeof provisionTestDatabase>>;

async function register(database: Database, id = "u1") {
	const client = database.client(`source-${id}`);
	await createPostgresRuntime({
		client,
		registry: producer,
		schema: database.journalSchema,
	}).dispatch(request, { id });
	const store = createPostgresEffectStore({
		client,
		registry: completionRegistry,
		schema: database.journalSchema,
	});
	const read = await store.read({
		source: { streamName: "users", streamKey: JSON.stringify({ id }) },
		effectSequence: "1",
	});
	if (read.status !== "outstanding")
		throw new Error("Expected newly registered effect");
	return read.effect;
}

function runner(
	store: ReturnType<typeof createPostgresEffectStore>,
	handle: (effectId: string) => EffectReceipt | Promise<EffectReceipt>,
) {
	return createEffectRunner({
		store,
		registry: definitions.registry({
			...metadata,
			effectHandlers: [
				defineEffectHandler({
					effect: index,
					handle({ effect, effectId }) {
						expect(effect.at).toEqual(new Date("2026-01-01"));
						return handle(effectId);
					},
				}),
			],
		}),
	});
}

/** Reference-only synthetic publication; no scheduler or private Bellows SQL. */
async function publication(database: Database) {
	await database.admin.query(
		`CREATE TABLE "${database.journalSchema}".completion_tasks (id bigint GENERATED ALWAYS AS IDENTITY, payload text NOT NULL)`,
	);
	const notify = vi.fn(async () => {});
	const validate = vi.fn((rows: readonly unknown[], count: number) => {
		const ids = z.array(z.object({ id: z.string() })).parse(rows);
		if (ids.length !== count || new Set(ids.map(({ id }) => id)).size !== count)
			throw new Error("Invalid publication");
		return notify;
	});
	return {
		notify,
		validate,
		publication: {
			prepare({ source }) {
				return {
					sql: `INSERT INTO "${database.journalSchema}".completion_tasks (payload) SELECT "${source.payloadColumn}" FROM "${source.relation}" RETURNING id::text`,
					parameters: [],
					validate,
				};
			},
		} satisfies PostgresPublication,
	};
}

async function count(
	database: Database,
	table: "pending_occurrence" | "completion_tasks",
) {
	return z
		.object({ count: z.number() })
		.parse(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM "${database.journalSchema}"."${table}"`,
				)
			).rows[0],
		).count;
}

test("decoded external execution prepares heterogeneous receipts without destination implementations and delivers independent occurrences", async ({
	database,
}) => {
	const effect = await register(database);
	const client = database.client("executor");
	const pub = await publication(database);
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	const store = createPostgresEffectStore({
		client,
		registry: completionRegistry,
		schema: database.journalSchema,
		publication: pub.publication,
	});
	const followup = { id: " u1 ", value: "indexed", extra: "original input" };
	const external = vi.fn(async (id: string) => {
		expect(id).toBe(effectId(effect.reference));
		expect(queries.some((query) => query === "BEGIN")).toBe(false);
		// This is the same single-connection client: it could not run here if
		// Folven had opened a transaction around the external handler.
		await client.user.count();
		return {
			events: [
				eventInput(indexed, followup),
				eventInput(failed, { id: "u2", reason: "business refusal" }),
			],
		};
	});
	pub.notify.mockImplementation(async () => {
		expect((await store.read(effect.reference)).status).toBe(
			"alreadyCompleted",
		);
		expect(await count(database, "pending_occurrence")).toBe(2);
		expect(await count(database, "completion_tasks")).toBe(2);
		throw new Error("Notification failure must not undo completion");
	});
	expect(await runner(store, external).attempt(effect.reference)).toEqual({
		status: "completed",
	});
	expect(external).toHaveBeenCalledTimes(1);
	expect(pub.notify).toHaveBeenCalledTimes(1);
	expect(
		queries.filter((query) => query.startsWith("WITH observed")),
	).toHaveLength(1);
	expect(
		queries.filter(
			(query) =>
				query.includes("INSERT INTO") && query.includes('"pending_occurrence"'),
		),
	).toHaveLength(1);
	const sink = vi.fn();
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({
			...metadata,
			handlers: [
				defineHandler({
					event: indexed,
					handle({ event }) {
						sink(event);
					},
				}),
				defineHandler({
					event: failed,
					handle({ event }) {
						sink(event);
					},
				}),
			],
		}),
	});
	const page = await runtime.outbox.readPendingOccurrences();
	expect(page.envelopes).toHaveLength(2);
	for (const envelope of page.envelopes) {
		expect(Object.keys(envelope).sort()).toEqual([
			"encodedInput",
			"eventName",
			"eventVersion",
			"occurrenceId",
			"payloadFormat",
			"target",
		]);
		if (envelope.eventName === indexed.name)
			expect(envelope.encodedInput).toBe(JSON.stringify(followup));
		expect(await runtime.ingest(envelope)).toEqual({ status: "processed" });
		await runtime.outbox.acknowledge(envelope);
	}
	expect(sink).toHaveBeenCalledWith({ id: "u1", value: "indexed" });
	expect(sink).toHaveBeenCalledWith({ id: "u2", reason: "business refusal" });
	const logs = await database.admin.query<Record<string, unknown>>(
		`SELECT incoming_source_stream_name, incoming_source_stream_key, incoming_channel_sequence FROM "${database.journalSchema}".event_log`,
	);
	expect(logs.rows).toHaveLength(3);
	for (const row of logs.rows)
		expect(Object.values(row)).toEqual([null, null, null]);
	expect(await count(database, "completion_tasks")).toBe(2);
});

test("empty completion deletes only its outstanding row, skips staging, and completed redelivery needs no definition", async ({
	database,
}) => {
	const effect = await register(database);
	const client = database.client("empty");
	await createPostgresRuntime({
		client,
		registry: producer,
		schema: database.journalSchema,
	}).dispatch(request, { id: "u1" });
	const pub = await publication(database);
	const store = createPostgresEffectStore({
		client,
		registry: completionRegistry,
		schema: database.journalSchema,
		publication: pub.publication,
	});
	const later = { ...effect.reference, effectSequence: "2" };
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	expect(await runner(store, () => ({ events: [] })).attempt(later)).toEqual({
		status: "completed",
	});
	expect((await store.read(effect.reference)).status).toBe("outstanding");
	expect(pub.validate).not.toHaveBeenCalled();
	expect(
		queries.filter((query) => query.includes('"pending_occurrence"')),
	).toEqual([]);
	const retired = createEffectRunner({
		store,
		registry: definitions.registry({ streams: [], events: [] }),
	});
	expect(await retired.attempt(later)).toEqual({ status: "alreadyCompleted" });
	await expect(retired.attempt(effect.reference)).rejects.toMatchObject({
		stage: "definition",
		externalWork: "notInvoked",
	});
	await expect(
		retired.attempt({ ...later, effectSequence: "3" }),
	).rejects.toMatchObject({ stage: "read" });
});

test("handler and receipt failures retain outstanding work without automatic external retries", async ({
	database,
}) => {
	const effect = await register(database);
	const client = database.client("failures");
	const store = createPostgresEffectStore({
		client,
		registry: completionRegistry,
		schema: database.journalSchema,
	});
	const external = vi.fn(() => {
		throw { code: "P2034", secret: "never expose" };
	});
	await expect(
		runner(store, external).attempt(effect.reference),
	).rejects.toMatchObject({ stage: "handler", externalWork: "mayHaveRun" });
	expect(external).toHaveBeenCalledTimes(1);
	const unregistered = defineEvent({ ...indexed });
	const receipts: EffectReceipt[] = [
		{ events: [eventInput(unregistered, { id: "u1", value: "x" })] },
		// @ts-expect-error Malformed schema input from an untyped implementation.
		{ events: [eventInput(indexed, { id: "u1", value: 4 })] },
		// @ts-expect-error Untyped implementation omitted the required receipt.
		undefined,
		// @ts-expect-error Untyped implementation invented an imperative output.
		{ events: [], emit: () => {} },
		// @ts-expect-error Event descriptions must come from eventInput.
		{ events: [{ event: indexed, input: { id: "u1", value: "x" } }] },
	];
	for (const receipt of receipts) {
		const failure = await runner(store, () => receipt)
			.attempt(effect.reference)
			.catch((error: unknown) => error);
		expect(failure).toBeInstanceOf(EffectExecutionError);
		expect(failure).toMatchObject({ stage: "receipt" });
		expect((await store.read(effect.reference)).status).toBe("outstanding");
		expect(await count(database, "pending_occurrence")).toBe(0);
	}
});

test("overlapping external attempts may run twice but only one receipt commits", async ({
	database,
}) => {
	const effect = await register(database);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let calls = 0;
	const attempts = ["one", "two"].map((label) => {
		const store = createPostgresEffectStore({
			client: database.client(label),
			registry: completionRegistry,
			schema: database.journalSchema,
		});
		return settle(
			runner(store, async () => {
				calls++;
				if (calls === 2) entered.resolve();
				await release.promise;
				return { events: [eventInput(indexed, { id: "u1", value: label })] };
			}).attempt(effect.reference),
		);
	});
	try {
		await bounded(entered.promise);
	} finally {
		release.resolve();
	}
	const outcomes = await Promise.all(attempts);
	expect(outcomes.map((outcome) => outcome.status)).toEqual([
		"fulfilled",
		"fulfilled",
	]);
	expect(
		outcomes
			.flatMap((outcome) =>
				outcome.status === "fulfilled" ? [outcome.value.status] : [],
			)
			.sort(),
	).toEqual(["alreadyCompleted", "completed"]);
	expect(calls).toBe(2);
	expect(await count(database, "pending_occurrence")).toBe(1);
});

test.for([2, 3])(
	"real deferred aborts (%i) retry only completion with fixed bytes, routes and UUIDs",
	async (aborts, { database }) => {
		const effect = await register(database);
		const client = database.client("retry");
		const observer = createPostgresEffectStore({
			client: database.client("observer"),
			registry: completionRegistry,
			schema: database.journalSchema,
		});
		const pub = await publication(database);
		await database.admin.query(`
		CREATE SEQUENCE "${database.journalSchema}".abort_attempt;
		CREATE FUNCTION "${database.journalSchema}".abort_completion() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF nextval('"${database.journalSchema}".abort_attempt') <= ${aborts} THEN
				RAISE EXCEPTION 'Synthetic deferred serialization failure' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END; $$;
		CREATE CONSTRAINT TRIGGER abort_completion AFTER INSERT ON "${database.journalSchema}".pending_occurrence
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "${database.journalSchema}".abort_completion()`);
		const attempts: string[] = [];
		client.$on("query", ({ query, params }) => {
			if (
				query.includes("INSERT INTO") &&
				query.includes('"pending_occurrence"')
			)
				attempts.push(params);
		});
		const publication_: PostgresPublication = {
			prepare(options) {
				const fragment = pub.publication.prepare(options);
				return {
					...fragment,
					async validate(rows, count_) {
						expect((await observer.read(effect.reference)).status).toBe(
							"outstanding",
						);
						expect(await count(database, "pending_occurrence")).toBe(0);
						expect(await count(database, "completion_tasks")).toBe(0);
						return fragment.validate(rows, count_);
					},
				};
			},
		};
		const store = createPostgresEffectStore({
			client,
			registry: completionRegistry,
			schema: database.journalSchema,
			publication: publication_,
		});
		const input = { id: " u1 ", value: "original", extra: "retained" };
		const handle = vi.fn(() => ({ events: [eventInput(indexed, input)] }));
		route.mockClear();
		const result = await runner(store, handle)
			.attempt(effect.reference)
			.catch((error: unknown) => error);
		if (aborts === 3) {
			expect(result).toBeInstanceOf(EffectExecutionError);
			if (!(result instanceof EffectExecutionError) || !result.proposal)
				throw new Error("Expected retryable prepared completion");
			expect(result.stage).toBe("completionAborted");
			expect((await observer.read(effect.reference)).status).toBe(
				"outstanding",
			);
			expect(await count(database, "pending_occurrence")).toBe(0);
			expect(pub.notify).not.toHaveBeenCalled();
			input.id = "changed";
			input.value = "changed";
			expect(await store.complete(result.proposal)).toEqual({
				status: "completed",
			});
		} else expect(result).toEqual({ status: "completed" });
		expect(attempts).toHaveLength(aborts + 1);
		expect(new Set(attempts).size).toBe(1);
		expect(handle).toHaveBeenCalledTimes(1);
		expect(route).toHaveBeenCalledTimes(1);
		expect(pub.notify).toHaveBeenCalledTimes(1);
		expect(await count(database, "pending_occurrence")).toBe(1);
		expect(await count(database, "completion_tasks")).toBe(1);
	},
);

test("invalid publication rolls back deletion and both insertion lanes; the same proposal can be retried deliberately", async ({
	database,
}) => {
	const effect = await register(database);
	const client = database.client("rollback");
	const pub = await publication(database);
	let valid = false;
	const store = createPostgresEffectStore({
		client,
		registry: completionRegistry,
		schema: database.journalSchema,
		publication: {
			prepare(options) {
				const fragment = pub.publication.prepare(options);
				return {
					...fragment,
					validate(rows, count_) {
						return fragment.validate(valid ? rows : [], count_);
					},
				};
			},
		},
	});
	const handle = vi.fn(() => ({
		events: [eventInput(indexed, { id: "u1", value: "x" })],
	}));
	const failure = await runner(store, handle)
		.attempt(effect.reference)
		.catch((error: unknown) => error);
	if (!(failure instanceof EffectExecutionError) || !failure.proposal)
		throw new Error("Expected captured completion");
	expect((await store.read(effect.reference)).status).toBe("outstanding");
	expect(await count(database, "pending_occurrence")).toBe(0);
	expect(await count(database, "completion_tasks")).toBe(0);
	expect(pub.notify).not.toHaveBeenCalled();
	valid = true;
	expect(await store.complete(failure.proposal)).toEqual({
		status: "completed",
	});
	expect(handle).toHaveBeenCalledTimes(1);
});

test("simulated lost commit response never reruns external work; reusing its proposal cannot publish a second receipt", async ({
	database,
}) => {
	const effect = await register(database);
	const client = database.client("lost-response");
	const pub = await publication(database);
	let lose = true;
	let transactions = 0;
	const host: PostgresTransactionHost<PostgresBorrowedTransaction> &
		PostgresRawClient = {
		$connect: () => client.$connect(),
		$disconnect: () => client.$disconnect(),
		$queryRawUnsafe: (sql, ...values) => client.$queryRawUnsafe(sql, ...values),
		async $transaction(run, options) {
			transactions++;
			const value = await client.$transaction(run, options);
			if (lose) {
				lose = false;
				throw new Error("Simulated lost response, not a real network fault");
			}
			return value;
		},
	};
	const store = createPostgresEffectStore({
		client: host,
		registry: completionRegistry,
		schema: database.journalSchema,
		publication: pub.publication,
	});
	const handle = vi.fn(() => ({
		events: [eventInput(indexed, { id: "u1", value: "x" })],
	}));
	const failure = await runner(store, handle)
		.attempt(effect.reference)
		.catch((error: unknown) => error);
	if (!(failure instanceof EffectExecutionError) || !failure.proposal)
		throw new Error("Expected captured uncertain completion");
	expect(failure.stage).toBe("completionUnknown");
	expect(transactions).toBe(1);
	expect(pub.notify).not.toHaveBeenCalled();
	expect(await count(database, "pending_occurrence")).toBe(1);
	expect(await count(database, "completion_tasks")).toBe(1);
	expect(await store.complete(failure.proposal)).toEqual({
		status: "alreadyCompleted",
	});
	expect(await runner(store, handle).attempt(effect.reference)).toEqual({
		status: "alreadyCompleted",
	});
	expect(handle).toHaveBeenCalledTimes(1);
	expect(await count(database, "pending_occurrence")).toBe(1);
	expect(await count(database, "completion_tasks")).toBe(1);
	expect(pub.notify).not.toHaveBeenCalled();
});
