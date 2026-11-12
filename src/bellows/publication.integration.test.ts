import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import {
	createPostgresRuntime,
	createPostgresPublisher,
	ChannelSequenceOverflowError,
	PublicationResultError,
	type OutgoingEnvelope,
	type PostgresPublication,
} from "../postgres/index.js";
import { createBellowsDelivery, type BellowsDeliveryOptions } from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const user = definitions.stream({
	name: "user",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});

test.for([0, 1, 10, 128])(
	"caller handoff composes %i independent occurrences and real Bellows tasks in zero or one execution",
	async (count, { database }) => {
		const schema = await database.installBellows();
		const client = database.client("independent-batch");
		await client.webhook.create({ data: { id: "webhook" } });
		const onTasksCommitted = vi
			.fn<NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>>()
			.mockRejectedValue(new Error("Synthetic notification failure"));
		const onDiagnostic = vi.fn();
		const delivery = createBellowsDelivery({
			schema,
			onTasksCommitted,
			onDiagnostic,
		});
		const prepare = vi.fn<PostgresPublication["prepare"]>((options) =>
			delivery.publication.prepare(options),
		);
		const publisher = createPostgresPublisher({
			registry: definitions.registry({
				streams: [user],
				events: [renamed],
				handlers: [],
			}),
			schema: database.journalSchema,
			publication: { prepare },
		});
		const prepared = Array.from({ length: count }, (_, index) =>
			publisher.prepare(renamed, { id: `u${index}`, name: 'Exact 🦉 "\n' }),
		);
		const queries: string[] = [];
		client.$on("query", ({ query }) => queries.push(query));
		const handoff = (rollback: boolean) =>
			client.$transaction(async (transaction) => {
				const consumed = await transaction.webhook.updateMany({
					where: { id: "webhook", handedOff: false },
					data: { handedOff: true },
				});
				if (consumed.count !== 1) throw new Error("Already handed off");
				const before = queries.length;
				const staged = await publisher.stage(transaction, prepared);
				expect(queries.slice(before)).toHaveLength(count ? 1 : 0);
				expect(onTasksCommitted).not.toHaveBeenCalled();
				if (rollback) throw new Error("Synthetic outer rollback");
				return staged;
			});
		const persistence = () =>
			queries.filter(
				(sql) =>
					sql.includes("pending_occurrence") || sql.includes("bellows_tasks"),
			);
		await expect(handoff(true)).rejects.toThrow("Synthetic outer rollback");
		expect(persistence()).toHaveLength(count ? 1 : 0);
		expect(prepare).toHaveBeenCalledTimes(count ? 1 : 0);
		for (const [namespace, table] of [
			[database.journalSchema, "pending_occurrence"],
			[schema, "bellows_tasks"],
		]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
		expect(
			await client.webhook.findUniqueOrThrow({ where: { id: "webhook" } }),
		).toMatchObject({ handedOff: false });
		queries.length = 0;
		prepare.mockClear();
		const staged = await handoff(false);
		expect(persistence()).toHaveLength(count ? 1 : 0);
		expect(prepare).toHaveBeenCalledTimes(count ? 1 : 0);
		expect(onTasksCommitted).not.toHaveBeenCalled();
		const rows = z
			.array(z.object({ task_id: z.string(), payload_json: z.string() }))
			.parse(
				(
					await database.admin.query(
						`SELECT task_id::text, payload_json FROM "${schema}".bellows_tasks`,
					)
				).rows,
			);
		expect(rows).toHaveLength(count);
		expect(
			rows.map((row) => delivery.task.codec.decode(row.payload_json)),
		).toEqual(expect.arrayContaining([...staged.references]));
		const source = createPostgresRuntime({
			client,
			registry: definitions.registry({
				streams: [user],
				events: [],
				handlers: [],
			}),
			schema: database.journalSchema,
		});
		for (const row of rows) {
			const reference = delivery.task.codec.decode(row.payload_json);
			expect(Object.keys(reference)).toEqual(["occurrenceId"]);
			const { envelope } = await source.outbox.read(reference);
			expect(envelope.eventName).toBe(renamed.name);
			expect(envelope.encodedInput).toContain("Exact 🦉");
		}
		expect(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ count: 0 }]);
		queries.length = 0;
		await expect(staged.notifyCommitted()).resolves.toBeUndefined();
		expect(queries).toEqual([]);
		expect(onTasksCommitted).toHaveBeenCalledTimes(count ? 1 : 0);
		expect(onDiagnostic).toHaveBeenCalledTimes(count);
		expect(
			await client.webhook.findUniqueOrThrow({ where: { id: "webhook" } }),
		).toMatchObject({ handedOff: true });
		expect(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM "${schema}".bellows_tasks`,
				)
			).rows,
		).toEqual([{ count }]);
	},
);

test("caller handoff is invisible before commit and a simulated lost commit response never notifies", async ({
	database,
}) => {
	const schema = await database.installBellows();
	const client = database.client("uncertain-handoff");
	const observer = database.client("handoff-observer");
	await client.webhook.create({ data: { id: "webhook" } });
	const onTasksCommitted = vi.fn();
	const delivery = createBellowsDelivery({ schema, onTasksCommitted });
	const publisher = createPostgresPublisher({
		registry: definitions.registry({
			streams: [user],
			events: [renamed],
			handlers: [],
		}),
		schema: database.journalSchema,
		publication: delivery.publication,
	});
	const prepared = publisher.prepare(renamed, {
		id: "u",
		name: "Changed later",
	});
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const operation = settle(
		(async () => {
			// Narrow response-loss double around a REAL outer commit. The owner never
			// receives a successful transaction result, so retains no action to notify.
			await client.$transaction(async (transaction) => {
				const consumed = await transaction.webhook.updateMany({
					where: { id: "webhook", handedOff: false },
					data: { handedOff: true },
				});
				if (consumed.count !== 1) throw new Error("Already handed off");
				const staged = await publisher.stage(transaction, [prepared]);
				entered.resolve();
				await bounded(release.promise);
				return staged;
			});
			throw new Error("Synthetic lost commit response");
		})(),
	);
	try {
		await bounded(entered.promise);
		expect(
			await observer.webhook.findUniqueOrThrow({ where: { id: "webhook" } }),
		).toMatchObject({ handedOff: false });
		for (const [namespace, table] of [
			[database.journalSchema, "pending_occurrence"],
			[schema, "bellows_tasks"],
		]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
		expect(onTasksCommitted).not.toHaveBeenCalled();
	} finally {
		release.resolve();
		await operation;
	}
	expect(await operation).toMatchObject({
		status: "rejected",
		reason: new Error("Synthetic lost commit response"),
	});
	expect(
		await observer.webhook.findUniqueOrThrow({ where: { id: "webhook" } }),
	).toMatchObject({ handedOff: true });
	for (const [namespace, table] of [
		[database.journalSchema, "pending_occurrence"],
		[schema, "bellows_tasks"],
	]) {
		expect(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
				)
			).rows,
		).toEqual([{ count: 1 }]);
	}
	expect(onTasksCommitted).not.toHaveBeenCalled();
});

test.for([0, 1, 10, 128])(
	"dispatch and emitting ingress persist %i outputs and tasks in one execution (none for zero)",
	async (count, { database }) => {
		const schema = await database.installBellows();
		const client = database.client("batch");
		const onTasksCommitted =
			vi.fn<NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>>();
		const delivery = createBellowsDelivery({ schema, onTasksCommitted });
		const prepare = vi.fn<PostgresPublication["prepare"]>((options) =>
			delivery.publication.prepare(options),
		);
		const queries: string[] = [];
		client.$on("query", ({ query }) => queries.push(query));
		const id = '雪 "author"';
		await client.user.create({
			data: { id, email: "batch@example.test", name: "Original", age: 0 },
		});
		const emitBatch = defineEvent({
			name: "user.emit",
			version: 1,
			stream: user,
			schema: z.strictObject({
				id: z.string(),
				count: z.number().int().nonnegative(),
			}),
			route: ({ id }) => ({ id }),
		});
		const payload = (index: number) => ({
			id: index % 2 ? 'target "雪"' : "target '\\",
			name: 'Exact 🦉 "\n',
		});
		const runtime = createPostgresRuntime({
			client,
			schema: database.journalSchema,
			publication: { prepare },
			registry: definitions.registry({
				streams: [user],
				events: [emitBatch, renamed],
				handlers: [
					defineHandler({
						event: emitBatch,
						async handle({ db, scope, event, emit }) {
							await db.user.update({
								where: scope.user.unique,
								data: { age: { increment: 1 } },
							});
							for (let index = 0; index < event.count; index++)
								emit(renamed, payload(index));
						},
					}),
				],
			}),
		});
		// Exercise bigint JSON encoding independently of JS numbers on the larger batch.
		if (count === 128) {
			await runtime.dispatch(emitBatch, { id, count: 2 });
			await database.admin.query(
				`UPDATE "${database.journalSchema}".outgoing_event SET channel_sequence = 9007199254740992`,
			);
			// Remove only this test's initial tasks; their references predate the administrative tail edit.
			await database.admin.query(`DELETE FROM "${schema}".bellows_tasks`);
		}
		queries.length = 0;
		prepare.mockClear();
		onTasksCommitted.mockClear();
		const input = { id, count };
		await runtime.dispatch(emitBatch, input);
		const persistenceQueries = () =>
			queries.filter((sql) => sql.startsWith("WITH captured"));
		expect(persistenceQueries()).toHaveLength(count ? 1 : 0);
		const incoming = {
			source: { streamName: "post", streamKey: '{"id":"p"}' },
			target: { streamName: "user", streamKey: JSON.stringify({ id }) },
			channelSequence: "1",
			eventName: emitBatch.name,
			eventVersion: 1,
			payloadFormat: "json-v1",
			encodedInput: JSON.stringify(input),
		} satisfies OutgoingEnvelope;
		expect(await runtime.ingest(incoming)).toEqual({ status: "processed" });
		expect(persistenceQueries()).toHaveLength(count ? 2 : 0);
		expect(await runtime.ingest(incoming)).toEqual({
			status: "alreadyProcessed",
		});
		expect(await runtime.ingest({ ...incoming, channelSequence: "3" })).toEqual(
			{ status: "gap", expectedSequence: "2" },
		);
		expect(persistenceQueries()).toHaveLength(count ? 2 : 0);
		expect(prepare).toHaveBeenCalledTimes(count ? 2 : 0);
		expect(onTasksCommitted).toHaveBeenCalledTimes(count ? 2 : 0);
		// No hidden per-reference execution: one journal insert and admission read are
		// separate costs, but every SQL statement touching tasks is the combined one.
		expect(queries.filter((sql) => sql.includes("bellows_tasks"))).toEqual(
			persistenceQueries(),
		);
		const tasks = z
			.array(z.object({ payload_json: z.string() }))
			.parse(
				(
					await database.admin.query(
						`SELECT payload_json FROM "${schema}".bellows_tasks`,
					)
				).rows,
			);
		expect(tasks).toHaveLength(count * 2);
		const identities = new Set<string>();
		for (const task of tasks) {
			const reference = delivery.task.codec.decode(task.payload_json);
			if ("occurrenceId" in reference) throw new Error("Expected channel task");
			identities.add(JSON.stringify(reference));
			expect(reference.source).toEqual(incoming.target);
			if (count === 128)
				expect(BigInt(reference.channelSequence)).toBeGreaterThan(
					9007199254740992n,
				);
			const { envelope } = await runtime.outbox.read(reference);
			const expected = [payload(0), payload(1)].find(
				(value) =>
					JSON.stringify({ id: value.id }) === reference.target.streamKey,
			);
			expect(expected).toBeDefined();
			expect(envelope.encodedInput).toBe(JSON.stringify(expected));
		}
		expect(identities.size).toBe(count * 2);
		if (count === 128) {
			await database.admin
				.query(`UPDATE "${database.journalSchema}".outgoing_event
			SET channel_sequence = 9223372036854775807
			WHERE channel_sequence = (SELECT MAX(channel_sequence) FROM "${database.journalSchema}".outgoing_event)`);
			await expect(
				runtime.dispatch(emitBatch, { id, count: 2 }),
			).rejects.toBeInstanceOf(ChannelSequenceOverflowError);
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${schema}".bellows_tasks`,
					)
				).rows,
			).toEqual([{ count: count * 2 }]);
			expect(
				await client.user.findUniqueOrThrow({ where: { id } }),
			).toMatchObject({ age: 3 });
			expect(onTasksCommitted).toHaveBeenCalledTimes(2);
		}
	},
);

test.for(["missing", "extra", "malformed"] as const)(
	"invalid %s publication receipts abort source state, journal, outputs and tasks",
	async (failure, { database }) => {
		const schema = await database.installBellows();
		const client = database.client("invalid-result");
		await client.user.create({
			data: {
				id: "u",
				email: "rollback@example.test",
				name: "Original",
				age: 0,
			},
		});
		const onTasksCommitted = vi.fn();
		const delivery = createBellowsDelivery({ schema, onTasksCommitted });
		const runtime = createPostgresRuntime({
			client,
			registry,
			schema: database.journalSchema,
			publication: {
				prepare(options) {
					const fragment = delivery.publication.prepare(options);
					return {
						...fragment,
						validate(rows, count) {
							const corrupted =
								failure === "missing"
									? rows.slice(1)
									: failure === "extra"
										? [...rows, { task_id: "999" }]
										: [{ task_id: "private malformed ID" }, ...rows.slice(1)];
							return fragment.validate(corrupted, count);
						},
					};
				},
			},
		});
		await expect(
			runtime.dispatch(rename, { id: "u", name: "Changed" }),
		).rejects.toBeInstanceOf(PublicationResultError);
		expect(
			await client.user.findUniqueOrThrow({ where: { id: "u" } }),
		).toMatchObject({ name: "Original", age: 0 });
		for (const [namespace, table] of [
			[database.journalSchema, "event_log"],
			[database.journalSchema, "outgoing_event"],
			[schema, "bellows_tasks"],
		]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
		expect(onTasksCommitted).not.toHaveBeenCalled();
	},
);

test("task-side SQL numeric failures are not attributed to channel exhaustion and roll back the whole transition", async ({
	database,
}) => {
	const schema = await database.installBellows();
	const client = database.client("task-failure");
	await client.user.create({
		data: { id: "u", email: "failure@example.test", name: "Original", age: 0 },
	});
	await database.admin.query(`
		CREATE FUNCTION "${schema}".fail_publication() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'Synthetic task-side numeric failure' USING ERRCODE = '22003'; END; $$;
		CREATE TRIGGER fail_publication BEFORE INSERT ON "${schema}".bellows_tasks
		FOR EACH ROW EXECUTE FUNCTION "${schema}".fail_publication();
	`);
	const delivery = createBellowsDelivery({ schema });
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication: delivery.publication,
	});
	await expect(
		runtime.dispatch(rename, { id: "u", name: "Changed" }),
	).rejects.toMatchObject({
		code: "P2010",
		meta: { driverAdapterError: { cause: { originalCode: "22003" } } },
	});
	expect(
		await client.user.findUniqueOrThrow({ where: { id: "u" } }),
	).toMatchObject({ age: 0 });
	for (const [namespace, table] of [
		[database.journalSchema, "event_log"],
		[database.journalSchema, "outgoing_event"],
		[schema, "bellows_tasks"],
	]) {
		expect(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
				)
			).rows,
		).toEqual([{ count: 0 }]);
	}
});

test("a real deferred abort discards provisional Bellows IDs and only prompts the committed retry", async ({
	database,
}) => {
	const schema = await database.installBellows();
	const client = database.client("retry");
	const observer = database.client("observer");
	await client.user.create({
		data: { id: "u", email: "u@example.test", name: "Original", age: 0 },
	});
	// Administrative fault injection: the sequence survives rollback, the tasks do not.
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
	const onTasksCommitted =
		vi.fn<NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>>();
	const delivery = createBellowsDelivery({ schema, onTasksCommitted });
	const attempts: string[][] = [];
	const staged = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication: {
			prepare(options) {
				const fragment = delivery.publication.prepare(options);
				return {
					...fragment,
					async validate(rows, count) {
						const action = await fragment.validate(rows, count);
						attempts.push(
							z
								.array(z.object({ task_id: z.string() }))
								.parse(rows)
								.map((row) => row.task_id)
								.sort(),
						);
						if (attempts.length === 2) {
							staged.resolve();
							await bounded(release.promise);
						}
						return action;
					},
				};
			},
		},
	});
	const dispatched = settle(
		runtime.dispatch(rename, { id: "u", name: "Retried" }),
	);
	try {
		await bounded(staged.promise);
		expect(attempts).toHaveLength(2);
		expect(onTasksCommitted).not.toHaveBeenCalled();
		expect(
			await observer.user.findUniqueOrThrow({ where: { id: "u" } }),
		).toMatchObject({ age: 0, name: "Original" });
		for (const [namespace, table] of [
			[database.journalSchema, "event_log"],
			[database.journalSchema, "outgoing_event"],
			[schema, "bellows_tasks"],
		]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${namespace}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
	} finally {
		release.resolve();
		await dispatched;
	}
	expect(await dispatched).toEqual({ status: "fulfilled", value: "renamed" });
	const rows = z
		.array(z.object({ task_id: z.string() }))
		.parse(
			(
				await database.admin.query(
					`SELECT task_id::text FROM "${schema}".bellows_tasks ORDER BY task_id`,
				)
			).rows,
		);
	const finalIds = rows.map((row) => row.task_id).sort();
	expect(finalIds).toHaveLength(2);
	expect(attempts[1]).toEqual(finalIds);
	expect(attempts[0]?.every((id) => !finalIds.includes(id))).toBe(true);
	expect(onTasksCommitted).toHaveBeenCalledExactlyOnceWith(
		expect.arrayContaining(
			finalIds.map((id) => ({
				taskId: Number(id),
				taskName: delivery.task.name,
			})),
		),
	);
	expect(
		await observer.user.findUniqueOrThrow({ where: { id: "u" } }),
	).toMatchObject({ age: 1, name: "Retried" });
	expect((await runtime.outbox.readPending()).envelopes).toHaveLength(1);
	expect(
		(
			await database.admin.query(
				`SELECT count(*)::int AS count FROM "${database.journalSchema}".outgoing_event`,
			)
		).rows,
	).toEqual([{ count: 2 }]);
	expect(
		(
			await database.admin.query(
				`SELECT count(*)::int AS count FROM "${database.journalSchema}".event_log`,
			)
		).rows,
	).toEqual([{ count: 1 }]);
});
const rename = defineEvent({
	name: "user.rename",
	version: 1,
	stream: user,
	schema: z.strictObject({ id: z.string(), name: z.string() }),
	route: ({ id }) => ({ id }),
});
const renamed = defineEvent({
	name: "user.renamed",
	version: 1,
	stream: user,
	schema: rename.schema,
	route: rename.route,
});
const registry = definitions.registry({
	streams: [user],
	events: [rename, renamed],
	handlers: [
		defineHandler({
			event: rename,
			handle: async ({ db, scope, event, emit }) => {
				await db.user.update({
					where: scope.user.unique,
					data: { name: event.name, age: { increment: 1 } },
				});
				emit(renamed, event);
				emit(renamed, event);
				return "renamed";
			},
		}),
	],
});

test("real Bellows publication shares transaction visibility and rollback, including emitting ingestion", async ({
	database,
}) => {
	const schema = await database.installBellows();
	const client = database.client("publisher");
	const observer = database.client("observer");
	await client.user.create({
		data: { id: "u", email: "u@example.test", name: "Original", age: 0 },
	});
	const onTasksCommitted =
		vi.fn<NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>>();
	const delivery = createBellowsDelivery({ schema, onTasksCommitted });
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let fail = true;
	const publication: PostgresPublication = {
		prepare(options) {
			const fragment = delivery.publication.prepare(options);
			return {
				...fragment,
				async validate(rows, count) {
					const action = await fragment.validate(rows, count);
					if (fail) {
						entered.resolve();
						await bounded(release.promise);
						throw new Error("Synthetic failure after Bellows publication");
					}
					return action;
				},
			};
		},
	};
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication,
	});
	const tables = [
		`"${database.journalSchema}"."event_log"`,
		`"${database.journalSchema}"."outgoing_event"`,
		`"${schema}"."bellows_tasks"`,
	];
	const pending = settle(
		runtime.dispatch(rename, { id: "u", name: "Changed" }),
	);
	try {
		await bounded(entered.promise);
		expect(
			await observer.user.findUnique({ where: { id: "u" } }),
		).toMatchObject({ name: "Original", age: 0 });
		for (const table of tables) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM ${table}`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
		expect(onTasksCommitted).not.toHaveBeenCalled();
	} finally {
		release.resolve();
		await pending;
	}
	expect(await pending).toMatchObject({
		status: "rejected",
		reason: new Error("Synthetic failure after Bellows publication"),
	});
	for (const table of tables) {
		expect(
			(
				await database.admin.query(
					`SELECT count(*)::int AS count FROM ${table}`,
				)
			).rows,
		).toEqual([{ count: 0 }]);
	}
	expect(await observer.user.findUnique({ where: { id: "u" } })).toMatchObject({
		name: "Original",
		age: 0,
	});
	expect(onTasksCommitted).not.toHaveBeenCalled();

	fail = false;
	onTasksCommitted.mockImplementation(async (receipts) => {
		// Independent observer sees committed rows before any caller notification.
		const tasks = await database.admin.query<{ task_id: string }>(
			`SELECT task_id::text FROM "${schema}"."bellows_tasks" WHERE task_id = ANY($1::bigint[])`,
			[receipts.map((receipt) => receipt.taskId)],
		);
		expect(tasks.rows).toHaveLength(2);
	});
	expect(await runtime.dispatch(rename, { id: "u", name: "Changed" })).toBe(
		"renamed",
	);
	expect(await observer.user.findUnique({ where: { id: "u" } })).toMatchObject({
		name: "Changed",
		age: 1,
	});
	const incoming = {
		source: { streamName: "post", streamKey: '{"id":"p"}' },
		target: { streamName: "user", streamKey: '{"id":"u"}' },
		channelSequence: "1",
		eventName: rename.name,
		eventVersion: rename.version,
		payloadFormat: "json-v1",
		encodedInput: JSON.stringify({ id: "u", name: "Ingested" }),
	} satisfies OutgoingEnvelope;
	expect(await runtime.ingest(incoming)).toEqual({ status: "processed" });
	expect(await runtime.ingest(incoming)).toEqual({
		status: "alreadyProcessed",
	});
	expect(onTasksCommitted).toHaveBeenCalledTimes(2);
	// Notification errors cannot reject commits; surface any assertion failures.
	for (const result of onTasksCommitted.mock.results) await result.value;
	const rows = z
		.array(
			z.strictObject({
				task_id: z.string(),
				task_name: z.string(),
				payload_json: z.string(),
				callback_id: z.null(),
			}),
		)
		.parse(
			(
				await database.admin.query(
					`SELECT task_id::text, task_name, payload_json, callback_id FROM "${schema}"."bellows_tasks" ORDER BY task_id`,
				)
			).rows,
		);
	expect(rows).toHaveLength(4);
	expect(onTasksCommitted.mock.calls.flatMap(([receipts]) => receipts)).toEqual(
		expect.arrayContaining(
			rows.map((row) => ({
				taskId: Number(row.task_id),
				taskName: row.task_name,
			})),
		),
	);
	const sequences: string[] = [];
	for (const row of rows) {
		const reference = delivery.task.codec.decode(row.payload_json);
		if ("occurrenceId" in reference) throw new Error("Expected channel task");
		sequences.push(reference.channelSequence);
		expect(reference).toMatchObject({
			source: incoming.target,
			target: incoming.target,
		});
		const retained = await runtime.outbox.read(reference);
		expect(retained.envelope.eventName).toBe(renamed.name);
		expect(retained.acknowledged).toBe(false);
	}
	expect(sequences.sort()).toEqual(["1", "2", "3", "4"]);
	expect(
		(
			await database.admin.query(
				`SELECT count(*)::int AS count FROM "${database.journalSchema}"."event_log"`,
			)
		).rows,
	).toEqual([{ count: 2 }]);
	// Borrowing the transaction never closes the caller's client.
	expect(await client.user.findUnique({ where: { id: "u" } })).toMatchObject({
		name: "Ingested",
		age: 2,
	});
});
