import {
	definePublishTask,
	LeaseLostError,
	runTaskOnce,
	TaskSuccess,
	type TaskExecutionBackend,
	type WorkerFactory,
} from "@xjonathanlei/bellows";
import { PostgresPublishingBackend } from "@xjonathanlei/bellows/backends/postgres-publishing";
import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	bounded,
	settle,
	test,
	type provisionTestDatabase,
} from "../../tests/postgres.js";
import { createEffectRunner, type EffectStore } from "../effects/index.js";
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
	type EffectReference,
	type PostgresPublication,
} from "../postgres/index.js";
import {
	createBellowsDelivery,
	createBellowsEffects,
	type BellowsEffectWorker,
	type BellowsEffectDiagnostic,
	type EffectFailurePolicy,
} from "./index.js";

type Database = Awaited<ReturnType<typeof provisionTestDatabase>>;
const definitions = createDefinitions<Pick<Prisma.TransactionClient, "user">>();
const user = definitions.stream({
	name: "user",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const address = z.strictObject({ id: z.string() });
const message = defineEffect({
	name: "user.message",
	version: 1,
	schema: address,
});
const request = defineEvent({
	name: "user.request",
	version: 1,
	stream: user,
	schema: address.extend({ count: z.int().positive(), emit: z.boolean() }),
	route: ({ id }) => ({ id }),
});
const sent = defineEvent({
	name: "user.sent",
	version: 1,
	stream: user,
	schema: address,
	route: (input) => input,
});
const observed = defineEvent({
	name: "user.observed",
	version: 1,
	stream: user,
	schema: address,
	route: (input) => input,
});
const metadata = {
	streams: [user],
	events: [request, sent, observed],
	effects: [message],
} as const;
const registry = definitions.registry({
	...metadata,
	handlers: [
		defineHandler({
			event: request,
			async handle({ db, scope, event, emit, requestEffect }) {
				await db.user.upsert({
					where: scope.user.unique,
					create: {
						id: scope.user.fields.id,
						email: `${event.id}@example.test`,
						name: "Requested",
						age: 0,
					},
					update: { name: "Requested" },
				});
				for (let i = 0; i < event.count; i++)
					requestEffect(message, { id: event.id });
				if (event.emit) emit(observed, { id: event.id });
			},
		}),
		defineHandler({
			event: sent,
			async handle({ db, scope, event, emit }) {
				await db.user.update({
					where: scope.user.unique,
					data: { age: { increment: 1 } },
				});
				emit(observed, event);
			},
		}),
		defineHandler({
			event: observed,
			async handle({ db, scope }) {
				await db.user.update({
					where: scope.user.unique,
					data: { nickname: "Observed" },
				});
			},
		}),
	],
});

async function setup(database: Database) {
	const schema = await database.installBellows();
	const client = database.client("source");
	const notifications = vi.fn(async () => {
		throw new Error("Synthetic missed notification");
	});
	const diagnostics: BellowsEffectDiagnostic[] = [];
	const delivery = createBellowsDelivery({
		schema,
		onTasksCommitted: notifications,
	});
	const effects = createBellowsEffects({
		schema,
		onTasksCommitted: notifications,
		onDiagnostic(value) {
			diagnostics.push(value);
		},
	});
	const runtime = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication: delivery.publication,
		effectPublication: effects.publication,
	});
	const store = createPostgresEffectStore({
		client: database.client("completion"),
		registry,
		schema: database.journalSchema,
		publication: delivery.publication,
	});
	const host = await database.bellowsHost(schema);
	return {
		schema,
		client,
		delivery,
		effects,
		runtime,
		store,
		host,
		notifications,
		diagnostics,
	};
}

function worker(
	database: Database,
	context: Awaited<ReturnType<typeof setup>>,
	handle: (id: string) => EffectReceipt | Promise<EffectReceipt>,
	onFailure: EffectFailurePolicy = () => ({ action: "manual" }),
	store: EffectStore = context.store,
): BellowsEffectWorker {
	const result = context.effects.createWorkerFactory({
		runner: createEffectRunner({
			store,
			registry: definitions.registry({
				...metadata,
				effectHandlers: [
					defineEffectHandler({
						effect: message,
						handle: ({ effect }) => handle(effect.id),
					}),
				],
			}),
		}),
		onFailure,
	});
	database.drainBeforeDisconnect(() => result.drain());
	return result;
}

async function tasks(database: Database, schema: string) {
	return z
		.array(
			z.strictObject({
				id: z.coerce.number().int().positive().max(Number.MAX_SAFE_INTEGER),
				name: z.string(),
				payload: z.string(),
			}),
		)
		.parse(
			(
				await database.admin.query(
					`SELECT task_id::text AS id, task_name AS name, payload_json AS payload FROM "${schema}".bellows_tasks ORDER BY task_id`,
				)
			).rows,
		);
}

function only<Value>(values: readonly Value[]): Value {
	expect(values).toHaveLength(1);
	const value = values[0];
	if (value === undefined) throw new Error("Expected one value.");
	return value;
}

async function count(
	database: Database,
	table:
		| "outstanding_effect"
		| "effect_frontier"
		| "pending_occurrence"
		| "event_log",
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

async function reschedule(
	context: Awaited<ReturnType<typeof setup>>,
	reference: EffectReference,
) {
	return context.client.$transaction(async (transaction) =>
		PostgresPublishingBackend.fromExecutor(
			{
				async query(sql, parameters) {
					return {
						rows: z
							.array(z.object({ task_id: z.string() }))
							.parse(await transaction.$queryRawUnsafe(sql, ...parameters)),
					};
				},
			},
			{ schema: context.schema },
		).publish(context.effects.task, reference),
	);
}

test("claimed effects and delivery share a host, discover missed notifications and atomically publish batched receipts", async ({
	database,
}) => {
	const context = await setup(database);
	const { client, schema, effects, delivery, runtime, host } = context;
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	const sourceStaged = Promise.withResolvers<void>();
	const releaseSource = Promise.withResolvers<void>();
	const source = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication: delivery.publication,
		effectPublication: {
			kind: "effect",
			prepare(input) {
				const fragment = effects.publication.prepare(input);
				return {
					...fragment,
					async validate(rows, count) {
						const notify = await fragment.validate(rows, count);
						sourceStaged.resolve();
						await bounded(releaseSource.promise);
						return notify;
					},
				};
			},
		},
	});
	const dispatch = settle(
		source.dispatch(request, { id: "u", count: 3, emit: true }),
	);
	try {
		await bounded(sourceStaged.promise);
		expect(await count(database, "outstanding_effect")).toBe(0);
		expect(await count(database, "effect_frontier")).toBe(0);
		expect(await tasks(database, schema)).toEqual([]);
		expect(await database.client("observer").user.count()).toBe(0);
	} finally {
		releaseSource.resolve();
		await dispatch;
	}
	expect((await dispatch).status).toBe("fulfilled");
	expect(
		queries.filter(
			(sql) =>
				sql.includes("INSERT INTO") && sql.includes('"outstanding_effect"'),
		),
	).toHaveLength(1);
	expect(context.notifications).toHaveBeenCalledTimes(2);
	const discovered = await host.discovery.readPage(
		await host.discovery.beginSweep(),
		null,
	);
	expect(discovered).toHaveLength(4);
	const registered = (await tasks(database, schema)).filter(
		({ name }) => name === effects.task.name,
	);
	expect(registered).toHaveLength(3);
	const completionClient = database.client("batch-completion");
	const completionQueries: string[] = [];
	completionClient.$on("query", ({ query }) => completionQueries.push(query));
	const receiptStaged = Promise.withResolvers<void>();
	const releaseReceipt = Promise.withResolvers<void>();
	let pause = true;
	const store = createPostgresEffectStore({
		client: completionClient,
		registry,
		schema: database.journalSchema,
		publication: {
			prepare(input) {
				const fragment = delivery.publication.prepare(input);
				return {
					...fragment,
					async validate(rows, count) {
						const notify = await fragment.validate(rows, count);
						if (pause) {
							receiptStaged.resolve();
							await bounded(releaseReceipt.promise);
						}
						return notify;
					},
				};
			},
		},
	});
	const provider = vi.fn(async (id: string) => {
		// Application-owned dependency; the same single-connection client remains usable.
		await completionClient.user.count();
		return { events: [eventInput(sent, { id }), eventInput(sent, { id })] };
	});
	const effectWorker = worker(database, context, provider, undefined, store);
	const first = registered[0];
	if (!first) throw new Error("Missing effect task");
	const running = settle(
		runTaskOnce(host.execution, effectWorker.factory, 1, {
			type: "task",
			taskId: first.id,
		}),
	);
	try {
		await bounded(receiptStaged.promise);
		expect(await count(database, "outstanding_effect")).toBe(3);
		expect(await count(database, "pending_occurrence")).toBe(0);
		expect(await tasks(database, schema)).toHaveLength(4);
	} finally {
		pause = false;
		releaseReceipt.resolve();
		await running;
	}
	expect(await running).toEqual({
		status: "fulfilled",
		value: { type: "done" },
	});
	expect(
		completionQueries.filter(
			(sql) =>
				sql.includes("INSERT INTO") && sql.includes('"pending_occurrence"'),
		),
	).toHaveLength(1);
	expect(await count(database, "pending_occurrence")).toBe(2);
	for (const row of registered.slice(1)) {
		expect(
			await runTaskOnce(host.execution, effectWorker.factory, 1, {
				type: "task",
				taskId: row.id,
			}),
		).toEqual({ type: "done" });
	}
	expect(provider).toHaveBeenCalledTimes(3);
	expect(
		completionQueries.filter(
			(sql) =>
				sql.includes("INSERT INTO") && sql.includes('"pending_occurrence"'),
		),
	).toHaveLength(3);
	const deliveryWorker = delivery.createWorkerFactory({
		source: runtime.outbox,
		deliver: runtime.ingest,
	});
	database.drainBeforeDisconnect(() => deliveryWorker.drain());
	const unrelated = definePublishTask("unrelated", {
		encode: (v: string) => v,
		decode: (v) => v,
	});
	const unrelatedFactory = {
		task: unrelated,
		build: () => ({
			async process() {
				return TaskSuccess.done(undefined);
			},
		}),
	} satisfies WorkerFactory<typeof unrelated>;
	const unrelatedTask = await client.$transaction((transaction) =>
		PostgresPublishingBackend.fromExecutor(
			{
				async query(sql, parameters) {
					return {
						rows: z
							.array(z.object({ task_id: z.string() }))
							.parse(await transaction.$queryRawUnsafe(sql, ...parameters)),
					};
				},
			},
			{ schema },
		).publish(unrelated, "synthetic"),
	);
	expect(
		await runTaskOnce(host.execution, unrelatedFactory, 1, {
			type: "task",
			taskId: unrelatedTask.taskId,
		}),
	).toEqual({ type: "done" });
	// Two bounded hops, not a scheduler: receipt events emit ordinary channel work.
	for (let hop = 0; hop < 2; hop++) {
		for (const row of await tasks(database, schema)) {
			expect(row.name).toBe(delivery.task.name);
			expect(
				await runTaskOnce(host.execution, deliveryWorker.factory, 1, {
					type: "task",
					taskId: row.id,
				}),
			).toEqual({ type: "done" });
		}
	}
	expect(await tasks(database, schema)).toEqual([]);
	expect(await count(database, "outstanding_effect")).toBe(0);
	expect(
		await client.user.findUniqueOrThrow({ where: { id: "u" } }),
	).toMatchObject({ age: 6, nickname: "Observed" });
});

test("Bellows publication result failures roll back source work and completion; manual handling retains a deliberately reschedulable effect", async ({
	database,
}) => {
	const context = await setup(database);
	const { client, effects, delivery, schema, host } = context;
	const source = createPostgresRuntime({
		client,
		registry,
		schema: database.journalSchema,
		publication: delivery.publication,
		effectPublication: {
			kind: "effect",
			prepare(input) {
				const fragment = effects.publication.prepare(input);
				return {
					...fragment,
					validate: (_rows, count) => fragment.validate([], count),
				};
			},
		},
	});
	await expect(
		source.dispatch(request, { id: "u", count: 1, emit: true }),
	).rejects.toThrow();
	expect(await client.user.count()).toBe(0);
	for (const table of [
		"outstanding_effect",
		"effect_frontier",
		"event_log",
	] as const)
		expect(await count(database, table)).toBe(0);
	expect(await tasks(database, schema)).toEqual([]);
	expect(context.notifications).not.toHaveBeenCalled();
	await context.runtime.dispatch(request, { id: "u", count: 1, emit: false });
	const row = only(await tasks(database, schema));
	const reference = effects.task.codec.decode(row.payload);
	const invalidPublication: PostgresPublication = {
		prepare(input) {
			const fragment = delivery.publication.prepare(input);
			return {
				...fragment,
				validate: (_rows, count) => fragment.validate([], count),
			};
		},
	};
	const store = createPostgresEffectStore({
		client: database.client("invalid-completion"),
		registry,
		schema: database.journalSchema,
		publication: invalidPublication,
	});
	const policy = vi
		.fn<EffectFailurePolicy>()
		.mockReturnValue({ action: "manual" });
	const provider = vi.fn((id: string) => ({
		events: [eventInput(sent, { id })],
	}));
	const effectWorker = worker(database, context, provider, policy, store);
	expect(
		await runTaskOnce(host.execution, effectWorker.factory, 1, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "done" });
	expect(policy.mock.calls[0]?.[0]).toMatchObject({
		stage: "completionUnknown",
		externalWork: "mayHaveRun",
	});
	expect(await count(database, "pending_occurrence")).toBe(0);
	expect((await context.store.read(reference)).status).toBe("outstanding");
	expect(await tasks(database, schema)).toEqual([]);
	expect(context.diagnostics.at(-1)).toMatchObject({
		stage: "completionUnknown",
		status: "manual",
	});
	// The host deliberately chooses external replay; this is not automatic backfill.
	const next = await reschedule(context, reference);
	const repaired = worker(database, context, provider);
	expect(
		await runTaskOnce(host.execution, repaired.factory, 2, {
			type: "task",
			taskId: next.taskId,
		}),
	).toEqual({ type: "done" });
	expect(provider).toHaveBeenCalledTimes(2);
	expect((await context.store.read(reference)).status).toBe("alreadyCompleted");
	expect(await count(database, "pending_occurrence")).toBe(1);
});

test("claimed handler retries are caller-selected; invalid receipts and throwing policies retire only the scheduling task", async ({
	database,
}) => {
	const context = await setup(database);
	await context.runtime.dispatch(request, { id: "u", count: 1, emit: false });
	const row = only(await tasks(database, context.schema));
	const reference = context.effects.task.codec.decode(row.payload);
	const provider = vi.fn<() => EffectReceipt>().mockImplementation(() => {
		throw new Error("Private service failure");
	});
	const deadline = Date.now() + 60_000;
	const policy = vi
		.fn<EffectFailurePolicy>()
		.mockReturnValue({ action: "retryAt", availableFromMs: deadline });
	const retrying = worker(database, context, provider, policy);
	const run = () =>
		runTaskOnce(context.host.execution, retrying.factory, 1, {
			type: "task",
			taskId: row.id,
		});
	expect(await run()).toEqual({ type: "retryAt", availableFromMs: deadline });
	expect(await run()).toEqual({ type: "retryAt", availableFromMs: deadline });
	expect(provider).toHaveBeenCalledTimes(1);
	expect(policy.mock.calls[0]?.[0]).toMatchObject({
		stage: "handler",
		externalWork: "mayHaveRun",
	});
	expect((await context.store.read(reference)).status).toBe("outstanding");
	// Deliberate host rescheduling using the public task; old delayed task still exists.
	const next = await reschedule(context, reference);
	// @ts-expect-error Deliberately malformed external receipt.
	provider.mockReturnValue({ events: [{ invalid: true }] });
	policy.mockImplementation(() => {
		throw new Error("Private policy error");
	});
	expect(
		await runTaskOnce(context.host.execution, retrying.factory, 2, {
			type: "task",
			taskId: next.taskId,
		}),
	).toEqual({ type: "done" });
	expect(policy.mock.calls[1]?.[0]).toMatchObject({
		stage: "receipt",
		externalWork: "mayHaveRun",
	});
	expect(context.diagnostics.at(-1)).toMatchObject({
		stage: "policy",
		status: "manual",
	});
	expect((await context.store.read(reference)).status).toBe("outstanding");
	expect((await tasks(database, context.schema)).map(({ id }) => id)).toEqual([
		row.id,
	]);
});

test("normal Bellows claims exclude a concurrent same-task worker; completion and redelivery bypass policy", async ({
	database,
}) => {
	const context = await setup(database);
	await context.runtime.dispatch(request, { id: "u", count: 1, emit: false });
	const row = only(await tasks(database, context.schema));
	const reference = context.effects.task.codec.decode(row.payload);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const provider = vi.fn(async () => {
		entered.resolve();
		await bounded(release.promise);
		return { events: [] };
	});
	const onFailure = vi
		.fn<EffectFailurePolicy>()
		.mockReturnValue({ action: "manual" });
	const effectWorker = worker(database, context, provider, onFailure);
	const first = settle(
		runTaskOnce(context.host.execution, effectWorker.factory, 1, {
			type: "task",
			taskId: row.id,
		}),
	);
	const competitor = await database.bellowsHost(context.schema);
	try {
		await bounded(entered.promise);
		const second = await runTaskOnce(
			competitor.execution,
			effectWorker.factory,
			2,
			{ type: "task", taskId: row.id },
		);
		expect(second.type).not.toBe("done");
		expect(provider).toHaveBeenCalledTimes(1);
	} finally {
		release.resolve();
		await first;
	}
	expect(await first).toEqual({ status: "fulfilled", value: { type: "done" } });
	expect(onFailure).not.toHaveBeenCalled();
	const next = await reschedule(context, reference);
	const oldImplementationRemoved = context.effects.createWorkerFactory({
		runner: createEffectRunner({ registry, store: context.store }),
		onFailure,
	});
	database.drainBeforeDisconnect(() => oldImplementationRemoved.drain());
	expect(
		await runTaskOnce(
			context.host.execution,
			oldImplementationRemoved.factory,
			3,
			{ type: "task", taskId: next.taskId },
		),
	).toEqual({ type: "done" });
	expect(provider).toHaveBeenCalledTimes(1);
	expect(onFailure).not.toHaveBeenCalled();
});

test("deliberately overlapping stale invocations may repeat external work but publish only one receipt", async ({
	database,
}) => {
	const context = await setup(database);
	await context.runtime.dispatch(request, { id: "u", count: 1, emit: false });
	const row = only(await tasks(database, context.schema));
	const reference = context.effects.task.codec.decode(row.payload);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let calls = 0;
	const provider = vi.fn(async (id: string) => {
		if (++calls === 2) entered.resolve();
		await bounded(release.promise);
		return { events: [eventInput(sent, { id })] };
	});
	const workers = [
		context.store,
		createPostgresEffectStore({
			client: database.client("competitor"),
			registry,
			schema: database.journalSchema,
			publication: context.delivery.publication,
		}),
	].map((store) => worker(database, context, provider, undefined, store));
	// Deliberately bypass claims to model stale/overlapping business promises, not normal scheduling.
	const attempts = workers.map((w, i) =>
		settle(w.factory.build(i + 1).process(row.id, reference)),
	);
	try {
		await bounded(entered.promise);
	} finally {
		release.resolve();
		await Promise.all(attempts);
	}
	for (const attempt of attempts)
		expect(await attempt).toEqual({
			status: "fulfilled",
			value: TaskSuccess.done(undefined),
		});
	expect(provider).toHaveBeenCalledTimes(2);
	expect(await count(database, "pending_occurrence")).toBe(1);
	expect(
		(await tasks(database, context.schema)).filter(
			({ name }) => name === context.delivery.task.name,
		),
	).toHaveLength(1);
	const finalizer = workers[0];
	if (!finalizer) throw new Error("Missing finalizer");
	expect(
		await runTaskOnce(context.host.execution, finalizer.factory, 3, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "done" });
	expect(provider).toHaveBeenCalledTimes(2);
});

test("drain retains external work, completion, policy and diagnostics after simulated lease loss and lost finalization response", async ({
	database,
}) => {
	const context = await setup(database);
	await context.runtime.dispatch(request, { id: "u", count: 1, emit: false });
	const row = only(await tasks(database, context.schema));
	const reference = context.effects.task.codec.decode(row.payload);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const completing = Promise.withResolvers<void>();
	const releaseCompletion = Promise.withResolvers<void>();
	const inPolicy = Promise.withResolvers<void>();
	const releasePolicy = Promise.withResolvers<void>();
	const diagnosing = Promise.withResolvers<void>();
	const releaseDiagnostic = Promise.withResolvers<void>();
	const provider = vi.fn(async (id: string) => {
		entered.resolve();
		await bounded(release.promise);
		return { events: [eventInput(sent, { id })] };
	});
	const effectAdapter = createBellowsEffects({
		schema: context.schema,
		async onDiagnostic() {
			diagnosing.resolve();
			await bounded(releaseDiagnostic.promise);
		},
	});
	const completionStore = createPostgresEffectStore({
		client: database.client("drained-completion"),
		registry,
		schema: database.journalSchema,
		publication: {
			prepare(input) {
				const fragment = context.delivery.publication.prepare(input);
				return {
					...fragment,
					async validate(rows, count) {
						const notify = await fragment.validate(rows, count);
						completing.resolve();
						await bounded(releaseCompletion.promise);
						return notify;
					},
				};
			},
		},
	});
	const effectWorker = worker(
		database,
		{ ...context, effects: effectAdapter },
		provider,
		async (failure) => {
			expect(failure.stage).toBe("completionUnknown");
			expect(failure.proposal).toBeDefined();
			inPolicy.resolve();
			await bounded(releasePolicy.promise);
			return { action: "manual" };
		},
		{
			read: completionStore.read,
			prepareCompletion: completionStore.prepareCompletion,
			async complete(proposal) {
				await completionStore.complete(proposal);
				// Explicit lost-completion-response simulation after a real commit, not a network fault.
				throw new Error("Synthetic lost completion response");
			},
		},
	);
	// Public backend lifecycle double: Bellows stops waiting; the business promise is not cancelled.
	const backend = {
		async claimPublished(task, _workerId, taskId) {
			return {
				taskId,
				taskPayload: task.codec.decode(row.payload),
				leaseExpirationMs: Date.now(),
			};
		},
		async claimEarliestPublished() {
			throw new Error("Unexpected claim");
		},
		async claimSingleton() {
			throw new Error("Unexpected claim");
		},
		async renew() {
			await bounded(entered.promise);
			throw new LeaseLostError();
		},
		async finish() {
			throw new Error("Unexpected finalization");
		},
		async fail() {
			throw new Error("Unexpected finalization");
		},
	} satisfies TaskExecutionBackend;
	const invocation = settle(
		runTaskOnce(backend, effectWorker.factory, 1, {
			type: "task",
			taskId: row.id,
		}),
	);
	const drained = vi.fn();
	let draining: Promise<void> | undefined;
	try {
		await bounded(entered.promise);
		expect(await bounded(invocation)).toEqual({
			status: "fulfilled",
			value: { type: "retry" },
		});
		draining = effectWorker.drain().then(drained);
		expect((await context.store.read(reference)).status).toBe("outstanding");
		expect(drained).not.toHaveBeenCalled();
		release.resolve();
		await bounded(completing.promise);
		expect(drained).not.toHaveBeenCalled();
		// The real completion transaction has staged both deletion and receipt,
		// but an independent reader still sees the uncompleted effect.
		expect((await context.store.read(reference)).status).toBe("outstanding");
		expect(await count(database, "pending_occurrence")).toBe(0);
		releaseCompletion.resolve();
		await bounded(inPolicy.promise);
		expect((await context.store.read(reference)).status).toBe(
			"alreadyCompleted",
		);
		expect(drained).not.toHaveBeenCalled();
		releasePolicy.resolve();
		await bounded(diagnosing.promise);
		expect(drained).not.toHaveBeenCalled();
	} finally {
		release.resolve();
		releaseCompletion.resolve();
		releasePolicy.resolve();
		releaseDiagnostic.resolve();
		await invocation;
		await effectWorker.drain();
		await draining;
	}
	expect(drained).toHaveBeenCalledOnce();
	expect(await count(database, "pending_occurrence")).toBe(1);
	// Real backend with a finish-response loss double. Subsequent redelivery still short-circuits the provider.
	const lostFinish = {
		claimPublished: context.host.execution.claimPublished.bind(
			context.host.execution,
		),
		claimEarliestPublished: context.host.execution.claimEarliestPublished.bind(
			context.host.execution,
		),
		claimSingleton: context.host.execution.claimSingleton.bind(
			context.host.execution,
		),
		renew: context.host.execution.renew.bind(context.host.execution),
		fail: context.host.execution.fail.bind(context.host.execution),
		async finish(...args) {
			await context.host.execution.finish(...args);
			throw new LeaseLostError();
		},
	} satisfies TaskExecutionBackend;
	expect(
		await runTaskOnce(lostFinish, effectWorker.factory, 2, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "retry" });
	expect(
		await runTaskOnce(context.host.execution, effectWorker.factory, 3, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "done" });
	expect(provider).toHaveBeenCalledTimes(1);
	await effectWorker.drain();
});
