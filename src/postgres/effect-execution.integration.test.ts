import { randomUUID } from "node:crypto";
import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	bounded,
	settle,
	test,
	type provisionTestDatabase,
} from "../../tests/postgres.js";
import {
	createDefinitions,
	defineEvent,
	defineEffect,
	defineEffectHandler,
	defineHandler,
	type EffectRequester,
	type EventEmitter,
} from "../index.js";
import { createEffectStorage } from "./effect-storage.js";
import {
	createPostgresRuntime,
	EffectRequestClosedError,
	EffectRequestFailedError,
	EmissionClosedError,
	EffectResultError,
	PublicationResultError,
	parseEffectReference,
	type PostgresPublication,
	type PostgresEffectPublication,
	type PostgresRawClient,
} from "./index.js";
import type { PostgresTransactionHost } from "./types.js";

type CommandClient = Pick<Prisma.TransactionClient, "user" | "post">;
const definitions = createDefinitions<CommandClient>();
const users = definitions.stream({
	name: "users",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const change = defineEvent({
	name: "user.change",
	version: 1,
	stream: users,
	schema: z.strictObject({ id: z.string(), name: z.string() }),
	route: ({ id }) => ({ id }),
});
const posts = definitions.stream({
	name: "posts",
	key: z.strictObject({ id: z.string() }),
	owns: { post: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const changed = defineEvent({
	name: "post.authorChanged",
	version: 1,
	stream: posts,
	schema: z.object({ id: z.string(), name: z.string() }),
	route: ({ id }) => ({ id }),
});
const index = defineEffect({
	name: "post.index",
	version: 1,
	schema: z.object({
		id: z.string(),
		text: z.string().min(1),
		at: z.string().transform((value) => new Date(value)),
	}),
});
const message = defineEffect({
	name: "user.message",
	version: 2,
	schema: z.object({ id: z.string() }),
});
const metadata = {
	streams: [users, posts],
	events: [change, changed],
	effects: [index, message],
} as const;
const input = {
	id: "post-1",
	text: "Original",
	at: "2026-01-01",
	extra: "exact input",
};
const command = { id: "u1", name: "Changed" };
type Database = Awaited<ReturnType<typeof provisionTestDatabase>>;

/** Test-owned scheduling table: validates generic fragments, not a scheduler. */
async function publications(database: Database) {
	await database.admin
		.query(`CREATE TABLE "${database.journalSchema}".publication (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, lane text NOT NULL, payload text NOT NULL
	)`);
	function lane(name: "delivery" | "effect") {
		const notify = vi.fn<(ids: readonly string[]) => Promise<void>>(
			async () => {},
		);
		const prepare = vi.fn<PostgresPublication["prepare"]>(
			({ source, parameterOffset }) => ({
				sql: `INSERT INTO "${database.journalSchema}".publication (lane, payload)
				SELECT $${parameterOffset + 1}::text, "${source.payloadColumn}" FROM "${source.relation}"
				RETURNING id::text, lane`,
				parameters: [name],
				validate(rows, count) {
					const receipts = z
						.array(
							z.strictObject({
								id: z.string().regex(/^[1-9][0-9]*$/),
								lane: z.literal(name),
							}),
						)
						.parse(rows);
					if (
						receipts.length !== count ||
						new Set(receipts.map(({ id }) => id)).size !== count
					)
						throw new PublicationResultError("Invalid test publication count.");
					return () => notify(receipts.map(({ id }) => id));
				},
			}),
		);
		return { prepare, notify };
	}
	const delivery = lane("delivery");
	const effect = lane("effect");
	return {
		delivery,
		effect,
		publication: { prepare: delivery.prepare } satisfies PostgresPublication,
		effectPublication: {
			kind: "effect",
			prepare: effect.prepare,
		} satisfies PostgresEffectPublication,
	};
}

async function expectEmpty(database: Database) {
	for (const table of [
		"event_log",
		"outgoing_event",
		"outstanding_effect",
		"effect_frontier",
		"publication",
	]) {
		expect(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}"."${table}"`,
				)
			).rows,
		).toEqual([]);
	}
}

test("mixed state, outputs, effects and both task lanes become visible atomically; notifications are independent", async ({
	database,
}) => {
	const client = database.client("source");
	const observer = database.client("observer");
	await client.user.create({
		data: { id: "u1", email: "u1@example.test", name: "Original" },
	});
	const pub = await publications(database);
	const external = vi.fn(() => ({ events: [] }));
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const handler = defineHandler({
		event: change,
		async handle({ db, scope, emit, requestEffect, event }) {
			await db.user.update({
				where: scope.user.unique,
				data: { name: event.name },
			});
			emit(changed, { id: "post-1", name: event.name });
			requestEffect(index, input);
			requestEffect(message, { id: event.id });
			return { value: () => 7n };
		},
	});
	const effectPublication: PostgresEffectPublication = {
		kind: "effect",
		prepare(options) {
			const fragment = pub.effectPublication.prepare(options);
			return {
				...fragment,
				async validate(rows, count) {
					const action = await fragment.validate(rows, count);
					entered.resolve();
					await bounded(release.promise);
					return action;
				},
			};
		},
	};
	const visible: Array<{ name: string | undefined; effects: number }> = [];
	const taskCounts: number[] = [];
	pub.delivery.notify.mockImplementation(async () => {
		visible.push({
			name: (await observer.user.findUnique({ where: { id: "u1" } }))?.name,
			effects: (
				await createEffectStorage(database.journalSchema).readPending(observer)
			).effects.length,
		});
		throw new Error("Synthetic failed notification");
	});
	pub.effect.notify.mockImplementation(async () => {
		const [row] = z
			.tuple([z.object({ count: z.number() })])
			.parse(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${database.journalSchema}".publication`,
					)
				).rows,
			);
		taskCounts.push(row.count);
	});
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({
			...metadata,
			handlers: [handler],
			effectHandlers: [
				defineEffectHandler({ effect: index, handle: external }),
			],
		}),
		publication: pub.publication,
		effectPublication,
	});
	const running = settle(runtime.dispatch(change, command));
	try {
		await bounded(entered.promise);
		await expectEmpty(database);
		expect(
			await observer.user.findUnique({ where: { id: "u1" } }),
		).toMatchObject({ name: "Original" });
		expect(pub.delivery.notify).not.toHaveBeenCalled();
		expect(pub.effect.notify).not.toHaveBeenCalled();
	} finally {
		release.resolve();
		await running;
	}
	const result = await running;
	expect(result.status).toBe("fulfilled");
	if (result.status === "fulfilled") expect(result.value.value()).toBe(7n);
	expect(external).not.toHaveBeenCalled();
	expect(pub.delivery.notify).toHaveBeenCalledOnce();
	expect(pub.effect.notify).toHaveBeenCalledOnce();
	expect(visible).toEqual([{ name: "Changed", effects: 2 }]);
	expect(taskCounts).toEqual([3]);
	const { effects } = await createEffectStorage(
		database.journalSchema,
	).readPending(client);
	expect(
		effects.map((effect) => [
			effect.reference.effectSequence,
			effect.effectName,
			effect.encodedInput,
		]),
	).toEqual([
		["1", index.name, JSON.stringify(input)],
		["2", message.name, '{"id":"u1"}'],
	]);
	const tasks = z
		.array(z.object({ payload: z.string() }))
		.parse(
			(
				await database.admin.query(
					`SELECT payload FROM "${database.journalSchema}".publication WHERE lane = 'effect'`,
				)
			).rows,
		);
	expect(
		new Set(
			tasks.map(({ payload }) => parseEffectReference(JSON.parse(payload))),
		),
	).toEqual(new Set(effects.map(({ reference }) => reference)));
	expect(pub.delivery.prepare.mock.calls[0]?.[0].parameterOffset).toBe(6);
	expect(pub.effect.prepare.mock.calls[0]?.[0]).toEqual({
		source: { relation: "effect_inputs", payloadColumn: "encoded_payload" },
		parameterOffset: 7,
	});
});

test("real query logs measure zero empty flushes and one batched flush for every output lane and publisher combination", async ({
	database,
}) => {
	const client = database.client("counts");
	const pub = await publications(database);
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	let serial = 0;
	for (const count of [0, 1, 10, 128]) {
		for (const lane of ["events", "effects", "mixed"]) {
			for (const publishers of ["none", "delivery", "effect", "both"]) {
				pub.delivery.prepare.mockClear();
				pub.effect.prepare.mockClear();
				const handler = defineHandler({
					event: change,
					handle({ emit, requestEffect, event }) {
						for (let i = 0; i < count; i++) {
							if (lane !== "effects")
								emit(changed, { id: `p${i % 3}`, name: event.name });
							if (lane !== "events")
								requestEffect(index, { ...input, text: `${i}` });
						}
					},
				});
				const runtime = createPostgresRuntime({
					client,
					schema: database.journalSchema,
					registry: definitions.registry({ ...metadata, handlers: [handler] }),
					...(publishers === "delivery" || publishers === "both"
						? { publication: pub.publication }
						: {}),
					...(publishers === "effect" || publishers === "both"
						? { effectPublication: pub.effectPublication }
						: {}),
				});
				queries.length = 0;
				const id = `source-${serial++}`;
				await runtime.dispatch(change, { id, name: "Batch" });
				// Transaction control and journal reservation are not output flushes.
				expect(
					queries.filter((sql) => sql.startsWith("WITH captured")),
				).toHaveLength(count === 0 ? 0 : 1);
				expect(
					queries.filter((sql) => sql.includes('"event_log"')),
				).toHaveLength(1);
				const writes = queries.filter(
					(sql) =>
						sql.includes('"outstanding_effect"') ||
						sql.includes('"outgoing_event"') ||
						sql.includes('".publication'),
				);
				expect(writes).toHaveLength(count === 0 ? 0 : 1);
				expect(pub.delivery.prepare).toHaveBeenCalledTimes(
					count > 0 &&
						lane !== "effects" &&
						(publishers === "delivery" || publishers === "both")
						? 1
						: 0,
				);
				expect(pub.effect.prepare).toHaveBeenCalledTimes(
					count > 0 &&
						lane !== "events" &&
						(publishers === "effect" || publishers === "both")
						? 1
						: 0,
				);
				const rows = (
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${database.journalSchema}".outstanding_effect WHERE source_stream_key = $1`,
						[JSON.stringify({ id })],
					)
				).rows;
				expect(rows).toEqual([{ count: lane === "events" ? 0 : count }]);
			}
		}
	}
});

test.for([
	"handler",
	"caught-request",
	"registration-result",
	"publication-result",
	"commit",
] as const)(
	"%s failure rolls back both lanes, state, frontier and tasks without consuming positions",
	async (failure, { database }) => {
		const client = database.client("rollback");
		await client.user.create({
			data: { id: "u1", email: "u1@example.test", name: "Original" },
		});
		const pub = await publications(database);
		let fail = true;
		let saved: EffectRequester | undefined;
		if (failure === "commit") {
			await database.admin
				.query(`CREATE FUNCTION "${database.journalSchema}".reject_effect() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Synthetic failure' USING ERRCODE = '23514'; END; $$;
				CREATE CONSTRAINT TRIGGER reject_effect AFTER INSERT ON "${database.journalSchema}".outstanding_effect
				DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "${database.journalSchema}".reject_effect()`);
		}
		// Preserve all real delegates and query execution; corrupt only returned
		// framework metadata to prove validation occurs before commit.
		const host: PostgresTransactionHost<CommandClient & PostgresRawClient> &
			PostgresRawClient = {
			$connect: () => client.$connect(),
			$disconnect: () => client.$disconnect(),
			$queryRawUnsafe: (sql, ...values) =>
				client.$queryRawUnsafe(sql, ...values),
			$transaction: (run, options) =>
				client.$transaction(async (tx) => {
					if (failure !== "registration-result" || !fail) return run(tx);
					return run({
						user: tx.user,
						post: tx.post,
						async $queryRawUnsafe(sql, ...values) {
							const result = await tx.$queryRawUnsafe(sql, ...values);
							if (!sql.startsWith("WITH captured")) return result;
							const [row] = z
								.tuple([
									z.strictObject({
										references: z.array(z.unknown()),
										publications: z.array(z.unknown()),
										overflow: z.boolean(),
										effects: z.unknown(),
										effectPublications: z.array(z.unknown()),
									}),
								])
								.parse(result);
							// Real mixed writes, but corrupted returned registration metadata.
							return [{ ...row, effects: { status: "accepted" } }];
						},
					});
				}, options),
		};
		const handler = defineHandler({
			event: change,
			async handle({ db, scope, event, emit, requestEffect }) {
				saved = requestEffect;
				await db.user.update({
					where: scope.user.unique,
					data: { name: event.name },
				});
				emit(changed, { id: "post-1", name: event.name });
				requestEffect(index, input);
				if (fail && failure === "handler")
					throw new Error("Synthetic handler failure");
				if (fail && failure === "caught-request") {
					try {
						requestEffect(index, { ...input, text: "" });
					} catch {
						/* finish must refuse */
					}
				}
			},
		});
		const effectPublication: PostgresEffectPublication = {
			kind: "effect",
			prepare(options) {
				const fragment = pub.effectPublication.prepare(options);
				return {
					...fragment,
					validate(rows, count) {
						return fragment.validate(
							fail && failure === "publication-result" ? [] : rows,
							count,
						);
					},
				};
			},
		};
		const runtime = createPostgresRuntime({
			client: host,
			schema: database.journalSchema,
			registry: definitions.registry({ ...metadata, handlers: [handler] }),
			publication: pub.publication,
			effectPublication,
		});
		const outcome = runtime.dispatch(change, command);
		if (failure === "caught-request")
			await expect(outcome).rejects.toBeInstanceOf(EffectRequestFailedError);
		else if (failure === "registration-result")
			await expect(outcome).rejects.toBeInstanceOf(EffectResultError);
		else if (failure === "publication-result")
			await expect(outcome).rejects.toBeInstanceOf(PublicationResultError);
		else await expect(outcome).rejects.toThrow();
		await expectEmpty(database);
		expect(await client.user.findUnique({ where: { id: "u1" } })).toMatchObject(
			{ name: "Original" },
		);
		expect(() => saved?.(index, input)).toThrow(EffectRequestClosedError);
		expect(pub.delivery.notify).not.toHaveBeenCalled();
		expect(pub.effect.notify).not.toHaveBeenCalled();
		if (failure === "commit")
			await database.admin.query(
				`DROP TRIGGER reject_effect ON "${database.journalSchema}".outstanding_effect`,
			);
		fail = false;
		await runtime.dispatch(change, command);
		const stored = await createEffectStorage(
			database.journalSchema,
		).readPending(client);
		expect(
			stored.effects.map(({ reference }) => reference.effectSequence),
		).toEqual(["1"]);
		expect(
			(
				await database.admin.query(
					`SELECT registered_through FROM "${database.journalSchema}".effect_frontier`,
				)
			).rows,
		).toEqual([{ registered_through: "1" }]);
	},
);

test("a real known commit abort closes both old collectors and retains only successful-attempt publications", async ({
	database,
}) => {
	const client = database.client("retry");
	await client.user.create({
		data: { id: "u1", email: "u1@example.test", name: "Original", age: 0 },
	});
	const pub = await publications(database);
	await database.admin
		.query(`CREATE SEQUENCE "${database.journalSchema}".attempt;
		CREATE FUNCTION "${database.journalSchema}".abort_once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			IF nextval('"${database.journalSchema}".attempt') = 1 THEN
				RAISE EXCEPTION 'Synthetic serialization abort' USING ERRCODE = '40001';
			END IF; RETURN NEW; END; $$;
		CREATE CONSTRAINT TRIGGER abort_once AFTER INSERT ON "${database.journalSchema}".outstanding_effect
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "${database.journalSchema}".abort_once()`);
	const collectors: Array<{
		emit: EventEmitter;
		requestEffect: EffectRequester;
	}> = [];
	const handler = defineHandler({
		event: change,
		async handle({ db, scope, event, emit, requestEffect }) {
			for (const old of collectors) {
				expect(() => old.emit(changed, { id: "p1", name: "late" })).toThrow(
					EmissionClosedError,
				);
				expect(() => old.requestEffect(index, input)).toThrow(
					EffectRequestClosedError,
				);
			}
			collectors.push({ emit, requestEffect });
			await db.user.update({
				where: scope.user.unique,
				data: { age: { increment: 1 } },
			});
			expect(event.name).toBe("Changed");
			emit(changed, { id: "p1", name: `Attempt ${collectors.length}` });
			requestEffect(index, { ...input, text: `Attempt ${collectors.length}` });
			event.name = "mutated";
		},
	});
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({ ...metadata, handlers: [handler] }),
		publication: pub.publication,
		effectPublication: pub.effectPublication,
	});
	await runtime.dispatch(change, command);
	expect(collectors).toHaveLength(2);
	expect(await client.user.findUnique({ where: { id: "u1" } })).toMatchObject({
		age: 1,
	});
	const { effects } = await createEffectStorage(
		database.journalSchema,
	).readPending(client);
	expect(effects).toHaveLength(1);
	expect(effects[0]).toMatchObject({
		reference: { effectSequence: "1" },
		encodedInput: JSON.stringify({ ...input, text: "Attempt 2" }),
	});
	expect(pub.delivery.prepare).toHaveBeenCalledTimes(2);
	expect(pub.effect.prepare).toHaveBeenCalledTimes(2);
	expect(pub.delivery.notify).toHaveBeenCalledOnce();
	expect(pub.effect.notify).toHaveBeenCalledOnce();
	const rows = z
		.array(z.object({ id: z.string(), lane: z.enum(["delivery", "effect"]) }))
		.parse(
			(
				await database.admin.query(
					`SELECT id::text, lane FROM "${database.journalSchema}".publication`,
				)
			).rows,
		);
	for (const lane of ["delivery", "effect"] as const) {
		expect(pub[lane].notify.mock.calls[0]?.[0]).toEqual(
			rows.filter((row) => row.lane === lane).map((row) => row.id),
		);
	}
	expect(
		(
			await database.admin.query(
				`SELECT payload FROM "${database.journalSchema}".outgoing_event`,
			)
		).rows,
	).toEqual([{ payload: '{"id":"p1","name":"Attempt 2"}' }]);
});

test("channel and independent ingestion collect effects only for admitted transitions", async ({
	database,
}) => {
	const client = database.client("ingress");
	const pub = await publications(database);
	const handled = vi.fn();
	const handler = defineHandler({
		event: change,
		handle({ requestEffect }) {
			handled();
			requestEffect(index, input);
			requestEffect(message, { id: "u1" });
		},
	});
	const runtime = createPostgresRuntime({
		client,
		schema: database.journalSchema,
		registry: definitions.registry({ ...metadata, handlers: [handler] }),
		effectPublication: pub.effectPublication,
	});
	const fields = {
		target: { streamName: users.name, streamKey: '{"id":"u1"}' },
		eventName: change.name,
		eventVersion: 1,
		payloadFormat: "json-v1",
		encodedInput: JSON.stringify(command),
	} as const;
	const channel = {
		...fields,
		source: { streamName: "external", streamKey: '{"id":"source"}' },
		channelSequence: "1",
	};
	expect(await runtime.ingest({ ...channel, channelSequence: "2" })).toEqual({
		status: "gap",
		expectedSequence: "1",
	});
	for (const envelope of [channel, { ...fields, occurrenceId: randomUUID() }]) {
		expect(await runtime.ingest(envelope)).toEqual({ status: "processed" });
		expect(await runtime.ingest(envelope)).toEqual({
			status: "alreadyProcessed",
		});
	}
	expect(handled).toHaveBeenCalledTimes(2);
	const { effects } = await createEffectStorage(
		database.journalSchema,
	).readPending(client);
	expect(effects.map(({ reference }) => reference.effectSequence)).toEqual([
		"1",
		"2",
		"3",
		"4",
	]);
	expect(pub.effect.prepare).toHaveBeenCalledTimes(2);
	expect(pub.effect.notify).toHaveBeenCalledTimes(2);
});
