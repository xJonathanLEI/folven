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
import type { DeliveryOptions } from "../delivery/index.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import {
	createPostgresRuntime,
	createPostgresPublisher,
	type DeliveryEnvelope,
	type DeliveryRead,
	type DeliveryReference,
	type IngestionResult,
	type OutgoingRead,
	type OutgoingReference,
	type PendingOccurrenceRead,
	type PendingOccurrenceReference,
} from "../postgres/index.js";
import {
	createBellowsDelivery,
	type BellowsDelivery,
	type BellowsDeliveryDiagnostic,
	type BellowsDeliveryOptions,
} from "./index.js";

type Database = Awaited<ReturnType<typeof provisionTestDatabase>>;
const definitions = createDefinitions<Prisma.TransactionClient>();
const post = definitions.stream({
	name: "post",
	key: z.strictObject({ id: z.string() }),
	owns: { post: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const comment = definitions.stream({
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
const input = z.strictObject({
	postId: z.string(),
	number: z.int(),
	body: z.string(),
});
const publish = defineEvent({
	name: "post.publish",
	version: 1,
	stream: post,
	schema: z.strictObject({ id: z.string(), comments: z.array(input) }),
	route: ({ id }) => ({ id }),
});
const add = defineEvent({
	name: "comment.add",
	version: 1,
	stream: comment,
	schema: input,
	route: ({ postId, number }) => ({ postId, number }),
});
const accepted = defineEvent({
	name: "post.commentAccepted",
	version: 1,
	stream: post,
	schema: z.strictObject({ id: z.string() }),
	route: (event) => event,
});
const ingressRegistry = definitions.registry({
	streams: [post, comment],
	events: [add],
	handlers: [],
});
const sourceRegistry = definitions.registry({
	streams: [post, comment],
	events: [publish, add, accepted],
	handlers: [
		defineHandler({
			event: publish,
			handle: async ({ db, scope, event, emit }) => {
				await db.post.update({
					where: scope.post.unique,
					data: { views: { increment: 1 } },
				});
				for (const value of event.comments) emit(add, value);
			},
		}),
		defineHandler({
			event: accepted,
			handle: async ({ db, scope }) => {
				await db.post.update({
					where: scope.post.unique,
					data: { views: { increment: 10 } },
				});
			},
		}),
	],
});
const destinationRegistry = definitions.registry({
	streams: [post, comment],
	events: [add, accepted],
	handlers: [
		defineHandler({
			event: add,
			handle: async ({ db, scope, event, emit }) => {
				// A domain rejection is an ordinary committed handler result, not a throw.
				if (event.body === "rejected") return { accepted: false };
				const previous = await db.comment.findUnique({
					where: scope.comment.unique,
				});
				await db.comment.upsert({
					where: scope.comment.unique,
					create: { ...scope.comment.fields, authorId: "u", body: event.body },
					update: { body: `${previous?.body ?? ""}|${event.body}` },
				});
				emit(accepted, { id: event.postId });
				return { accepted: true };
			},
		}),
	],
});

function bind(
	database: Database,
	adapter: BellowsDelivery,
	options: DeliveryOptions,
) {
	const worker = adapter.createWorkerFactory(options);
	database.drainBeforeDisconnect(() => worker.drain());
	return worker;
}

async function setup(
	database: Database,
	options: Pick<
		BellowsDeliveryOptions,
		"onTasksCommitted" | "onDiagnostic"
	> = {},
) {
	const schema = await database.installBellows();
	const sourceClient = database.client("source");
	const destinationClient = database.client("destination");
	await sourceClient.user.create({
		data: { id: "u", email: "u@example.test", name: "User" },
	});
	await sourceClient.post.create({
		data: {
			id: "p",
			authorId: "u",
			slug: "p",
			title: "Post",
			published: true,
			views: 0,
			createdAt: new Date("2026-01-01T00:00:00Z"),
			tags: [],
		},
	});
	// Fixed past clock makes persisted retry deadlines immediately due to Bellows.
	// Tests can explicitly invoke a later attempt without sleeps or scheduler emulation.
	const outgoing = createBellowsDelivery({
		schema,
		taskName: "post_delivery",
		now: () => 0,
		...options,
	});
	const returning = createBellowsDelivery({
		schema,
		taskName: "comment_delivery",
		now: () => 0,
	});
	const source = createPostgresRuntime({
		client: sourceClient,
		registry: sourceRegistry,
		schema: database.journalSchema,
		publication: outgoing.publication,
	});
	const destination = createPostgresRuntime({
		client: destinationClient,
		registry: destinationRegistry,
		schema: database.otherJournalSchema,
		publication: returning.publication,
	});
	return {
		schema,
		sourceSchema: database.journalSchema,
		sourceClient,
		destinationClient,
		source,
		destination,
		outgoing,
		returning,
	};
}

const taskRow = z.strictObject({
	id: z.coerce.number().int().positive().max(Number.MAX_SAFE_INTEGER),
	name: z.string(),
	payload: z.string(),
});

async function tasks(database: Database, schema: string) {
	return z.array(taskRow).parse(
		(
			await database.admin.query(
				`SELECT task_id::text AS id, task_name AS name, payload_json AS payload
		 FROM "${schema}".bellows_tasks ORDER BY task_id`,
			)
		).rows,
	);
}

function only<Value>(values: readonly Value[]): Value {
	expect(values).toHaveLength(1);
	const value = values[0];
	if (value === undefined) throw new Error("Expected exactly one value.");
	return value;
}

async function emitOne(context: Awaited<ReturnType<typeof setup>>) {
	await context.source.dispatch(publish, {
		id: "p",
		comments: [{ postId: "p", number: 1, body: "First" }],
	});
}

async function stageComments(
	context: Awaited<ReturnType<typeof setup>>,
	values = [{ postId: "p", number: 1, body: "First" }],
) {
	const publisher = createPostgresPublisher({
		registry: ingressRegistry,
		schema: context.sourceSchema,
		publication: context.outgoing.publication,
	});
	const prepared = values.map((value) => publisher.prepare(add, value));
	await context.sourceClient.webhook.create({ data: { id: "webhook" } });
	return context.sourceClient.$transaction(async (transaction) => {
		const consumed = await transaction.webhook.updateMany({
			where: { id: "webhook", handedOff: false },
			data: { handedOff: true },
		});
		if (consumed.count !== 1) throw new Error("Source already consumed");
		return publisher.stage(transaction, prepared);
	});
}

test("silent independent handoff survives runtime restart and shares a host with channel work without predecessor blocking", async ({
	database,
}) => {
	const onTasksCommitted = vi.fn();
	const context = await setup(database, { onTasksCommitted });
	const { source, sourceClient, outgoing, returning, schema } = context;
	await source.dispatch(publish, {
		id: "p",
		comments: [{ postId: "p", number: 2, body: "Channel" }],
	});
	onTasksCommitted.mockClear();
	const staged = await stageComments(context, [
		{ postId: "p", number: 1, body: "poisoned" },
		{ postId: "p", number: 1, body: "Independent" },
	]);
	expect(onTasksCommitted).not.toHaveBeenCalled();
	expect(await sourceClient.comment.count()).toBe(0);
	// Reconstruct both sides without using staging receipts or a wakeup. The source
	// needs no local handlers; Bellows discovery, not a pending-row republisher, recovers.
	const restartedSource = createPostgresRuntime({
		client: database.client("restarted-source"),
		registry: ingressRegistry,
		schema: database.journalSchema,
	});
	const destination = createPostgresRuntime({
		client: database.client("restarted-destination"),
		registry: destinationRegistry,
		schema: database.otherJournalSchema,
		publication: returning.publication,
	});
	const worker = bind(database, outgoing, {
		source: restartedSource.outbox,
		async deliver(envelope) {
			if (input.parse(JSON.parse(envelope.encodedInput)).body === "poisoned") {
				throw new Error("Synthetic independent destination failure");
			}
			return destination.ingest(envelope);
		},
	});
	const host = await database.bellowsHost(schema);
	const discovered = await host.discovery.readPage(
		await host.discovery.beginSweep(),
		null,
	);
	expect(discovered).toHaveLength(3);
	expect(
		discovered.every((candidate) => candidate.taskName === outgoing.task.name),
	).toBe(true);
	const rows = await tasks(database, schema);
	// Fail the first independent occurrence, then accept the other at the SAME
	// destination before processing channel work with this very same factory.
	const ordered = [...rows.slice(1), ...rows.slice(0, 1)];
	for (const [index, row] of ordered.entries()) {
		const result = await runTaskOnce(host.execution, worker.factory, 1, {
			type: "task",
			taskId: row.id,
		});
		expect(result).toEqual(
			index === 0
				? { type: "retryAt", availableFromMs: 5_000 }
				: { type: "done" },
		);
	}
	expect(
		(await sourceClient.comment.findMany({ orderBy: { number: "asc" } })).map(
			({ body }) => body,
		),
	).toEqual(["Independent", "Channel"]);
	for (const [index, reference] of staged.references.entries()) {
		expect((await restartedSource.outbox.read(reference)).acknowledged).toBe(
			index === 1,
		);
	}
	expect(
		(await sourceClient.post.findUniqueOrThrow({ where: { id: "p" } })).views,
	).toBe(1);
	const children = (await tasks(database, schema)).filter(
		(row) => row.name === returning.task.name,
	);
	expect(children).toHaveLength(2);
	const returnWorker = bind(database, returning, {
		source: destination.outbox,
		deliver: source.ingest,
	});
	for (const row of children) {
		const reference = returning.task.codec.decode(row.payload);
		expect(reference).toHaveProperty("channelSequence");
		expect(
			await runTaskOnce(host.execution, returnWorker.factory, 2, {
				type: "task",
				taskId: row.id,
			}),
		).toEqual({ type: "done" });
	}
	expect(
		(await sourceClient.post.findUniqueOrThrow({ where: { id: "p" } })).views,
	).toBe(21);
	expect(await tasks(database, schema)).toHaveLength(1);
	expect(
		(await restartedSource.outbox.readPendingOccurrences()).envelopes,
	).toHaveLength(1);
});

test("fresh Bellows discovery recovers a missed prompt and drives atomic return-channel hops beside an unrelated task", async ({
	database,
}) => {
	const onTasksCommitted = vi.fn<
		NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>
	>(async () => {
		throw new Error("Synthetic unavailable notification host");
	});
	const onDiagnostic = vi.fn<(diagnostic: BellowsDeliveryDiagnostic) => void>();
	const context = await setup(database, { onTasksCommitted, onDiagnostic });
	const { schema, source, outgoing, returning, sourceClient } = context;
	const stagedChild = Promise.withResolvers<void>();
	const releaseChild = Promise.withResolvers<void>();
	const destination = createPostgresRuntime({
		client: context.destinationClient,
		registry: destinationRegistry,
		schema: database.otherJournalSchema,
		publication: {
			prepare(options) {
				const fragment = returning.publication.prepare(options);
				return {
					...fragment,
					async validate(rows, count) {
						const action = await fragment.validate(rows, count);
						stagedChild.resolve();
						await bounded(releaseChild.promise);
						return action;
					},
				};
			},
		},
	});
	await emitOne(context);
	expect(onTasksCommitted).toHaveBeenCalledOnce();
	expect(onDiagnostic).toHaveBeenCalledExactlyOnceWith({
		taskId: only(await tasks(database, schema)).id,
		taskName: outgoing.task.name,
		stage: "notification",
		status: "failed",
	});
	expect(await sourceClient.comment.count()).toBe(0);
	expect(
		(await sourceClient.post.findUniqueOrThrow({ where: { id: "p" } })).views,
	).toBe(1);

	// No host existed at publication. A fresh host discovers the final silent output
	// without any subsequent business mutation or notification receipt.
	const host = await database.bellowsHost(schema);
	const worker = bind(database, outgoing, {
		source: source.outbox,
		deliver: destination.ingest,
	});
	const returnWorker = bind(database, returning, {
		source: destination.outbox,
		deliver: source.ingest,
	});
	const window = await host.discovery.beginSweep();
	const candidate = only(await host.discovery.readPage(window, null));
	expect(candidate.taskName).toBe(outgoing.task.name);
	expect(candidate.isSingleton).toBe(false);
	expect(await host.discovery.readPage(window, candidate.taskId)).toEqual([]);
	const original = only(await tasks(database, schema));
	const discoveredId = taskRow.shape.id.parse(candidate.taskId);
	expect(discoveredId).toBe(original.id);
	const reference = outgoing.task.codec.decode(original.payload);
	const firstHop = settle(
		runTaskOnce(host.execution, worker.factory, 1, {
			type: "task",
			taskId: discoveredId,
		}),
	);
	try {
		await bounded(stagedChild.promise);
		// Independent connections see neither the destination mutation, incoming
		// journal, child envelope nor child task before its ingestion commits.
		expect(await sourceClient.comment.count()).toBe(0);
		expect(await tasks(database, schema)).toEqual([original]);
		expect((await source.outbox.read(reference)).acknowledged).toBe(false);
		for (const table of ["event_log", "outgoing_event"]) {
			expect(
				(
					await database.admin.query(
						`SELECT count(*)::int AS count FROM "${database.otherJournalSchema}"."${table}"`,
					)
				).rows,
			).toEqual([{ count: 0 }]);
		}
	} finally {
		releaseChild.resolve();
		await firstHop;
	}
	expect(await firstHop).toEqual({
		status: "fulfilled",
		value: { type: "done" },
	});
	expect((await source.outbox.read(reference)).acknowledged).toBe(true);
	expect(await sourceClient.comment.findMany()).toMatchObject([
		{ body: "First" },
	]);
	// The first worker only ingests its one envelope. The return hop is still pending.
	expect(
		(await sourceClient.post.findUniqueOrThrow({ where: { id: "p" } })).views,
	).toBe(1);
	const child = only(await tasks(database, schema));
	expect(child.name).toBe(returning.task.name);
	const childReference = returning.task.codec.decode(child.payload);
	expect((await destination.outbox.read(childReference)).acknowledged).toBe(
		false,
	);

	const unrelated = definePublishTask("unrelated", {
		encode: (value: string) => JSON.stringify(z.string().parse(value)),
		decode: (value) => z.string().parse(JSON.parse(value)),
	});
	const received: string[] = [];
	const unrelatedFactory = {
		task: unrelated,
		build: () => ({
			async process(_id, value) {
				received.push(value);
				return TaskSuccess.done(undefined);
			},
		}),
	} satisfies WorkerFactory<typeof unrelated>;
	const unrelatedReceipt = await sourceClient.$transaction(
		async (transaction) => {
			const publisher = PostgresPublishingBackend.fromExecutor(
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
			);
			return publisher.publish(unrelated, "independent");
		},
	);
	// Ordinary host registration chooses an exact factory per name.
	const factories = new Map([
		[outgoing.task.name, worker.factory],
		[returning.task.name, returnWorker.factory],
	]);
	const secondPass = await host.discovery.readPage(
		await host.discovery.beginSweep(),
		null,
	);
	expect(secondPass.map((value) => value.taskName)).toEqual([
		returning.task.name,
		unrelated.name,
	]);
	for (const value of secondPass) {
		const id = z.coerce
			.number()
			.int()
			.positive()
			.max(Number.MAX_SAFE_INTEGER)
			.parse(value.taskId);
		if (value.taskName === unrelated.name) {
			expect(id).toBe(unrelatedReceipt.taskId);
			expect(
				await runTaskOnce(host.execution, unrelatedFactory, 2, {
					type: "task",
					taskId: id,
				}),
			).toEqual({ type: "done" });
		} else {
			const factory = factories.get(value.taskName);
			if (!factory) throw new Error("Unregistered task");
			expect(
				await runTaskOnce(host.execution, factory, 2, {
					type: "task",
					taskId: id,
				}),
			).toEqual({ type: "done" });
		}
	}
	expect(received).toEqual(["independent"]);
	expect(
		(await sourceClient.post.findUniqueOrThrow({ where: { id: "p" } })).views,
	).toBe(11);
	expect((await destination.outbox.read(childReference)).acknowledged).toBe(
		true,
	);
	expect(await tasks(database, schema)).toEqual([]);
	// A redundant invocation of a completed task cannot recreate child tasks.
	expect(
		await worker.factory.build(3).process(original.id, reference),
	).toBeInstanceOf(TaskSuccess);
	expect(await tasks(database, schema)).toEqual([]);
	expect(
		await host.discovery.readPage(await host.discovery.beginSweep(), null),
	).toEqual([]);
	await worker.drain();
	await returnWorker.drain();
	expect(await sourceClient.post.count()).toBe(1);
});

for (const kind of ["channel", "independent"] as const) {
	for (const uncertainty of [
		"destination response",
		"before acknowledgment",
		"after acknowledgment",
	] as const) {
		test(`${kind} same-reference recovery after ${uncertainty} loss preserves one destination mutation and child`, async ({
			database,
		}) => {
			const context = await setup(database);
			const { schema, source, destination, outgoing, sourceClient } = context;
			if (kind === "channel") await emitOne(context);
			else await stageComments(context);
			const row = only(await tasks(database, schema));
			const reference = outgoing.task.codec.decode(row.payload);
			const responses: IngestionResult[] = [];
			let firstDelivery = true;
			let firstAcknowledgment = true;
			const deliver = vi.fn(async (envelope: DeliveryEnvelope) => {
				const result = await destination.ingest(envelope);
				responses.push(result);
				if (firstDelivery && uncertainty === "destination response") {
					firstDelivery = false;
					// Response-loss simulation after a real commit, not a network fault.
					throw new Error("Synthetic lost destination response");
				}
				return result;
			});
			const worker = bind(database, outgoing, {
				source: {
					read: source.outbox.read,
					async acknowledge(envelope) {
						if (
							firstAcknowledgment &&
							uncertainty === "before acknowledgment"
						) {
							firstAcknowledgment = false;
							throw new Error("Synthetic failure before acknowledgment");
						}
						await source.outbox.acknowledge(envelope);
						if (firstAcknowledgment && uncertainty === "after acknowledgment") {
							firstAcknowledgment = false;
							throw new Error("Synthetic lost acknowledgment response");
						}
					},
				},
				deliver,
			});
			const host = await database.bellowsHost(schema);
			expect(
				await runTaskOnce(host.execution, worker.factory, 1, {
					type: "task",
					taskId: row.id,
				}),
			).toEqual({ type: "retryAt", availableFromMs: 5_000 });
			expect((await source.outbox.read(reference)).acknowledged).toBe(
				uncertainty === "after acknowledgment",
			);
			expect(await tasks(database, schema)).toHaveLength(2);
			expect(
				await runTaskOnce(host.execution, worker.factory, 2, {
					type: "task",
					taskId: row.id,
				}),
			).toEqual({ type: "done" });
			expect(responses).toEqual(
				uncertainty === "after acknowledgment"
					? [{ status: "processed" }]
					: [{ status: "processed" }, { status: "alreadyProcessed" }],
			);
			expect(deliver).toHaveBeenCalledTimes(responses.length);
			expect((await source.outbox.read(reference)).acknowledged).toBe(true);
			expect(await sourceClient.comment.findMany()).toMatchObject([
				{ body: "First" },
			]);
			expect(only(await tasks(database, schema)).name).toBe(
				context.returning.task.name,
			);
		});
	}
}

test("later tasks cannot bypass a poisoned head, while an unrelated channel progresses", async ({
	database,
}) => {
	const context = await setup(database);
	const { source, destination, outgoing, schema, sourceClient } = context;
	const diagnostics: BellowsDeliveryDiagnostic[] = [];
	// Real future availability proves that Bellows, not the runner, owns backoff.
	const adapter = createBellowsDelivery({
		schema,
		taskName: outgoing.task.name,
		retryDelayMs: 60_000,
		onDiagnostic: (value) => {
			diagnostics.push(value);
		},
	});
	await source.dispatch(publish, {
		id: "p",
		comments: [
			{ postId: "p", number: 1, body: "poisoned" },
			{ postId: "p", number: 1, body: "later" },
			{ postId: "p", number: 2, body: "independent" },
		],
	});
	const rows = await tasks(database, schema);
	const byBody = new Map<
		string,
		{ id: number; reference: OutgoingReference }
	>();
	for (const row of rows) {
		const reference = outgoing.task.codec.decode(row.payload);
		if ("occurrenceId" in reference) throw new Error("Expected channel task");
		const stored = await source.outbox.read(reference);
		byBody.set(input.parse(JSON.parse(stored.envelope.encodedInput)).body, {
			id: row.id,
			reference,
		});
	}
	const head = byBody.get("poisoned");
	const later = byBody.get("later");
	const independent = byBody.get("independent");
	if (!head || !later || !independent) throw new Error("Missing test task");
	let repaired = false;
	const deliver = vi.fn(async (envelope: DeliveryEnvelope) => {
		if (
			!repaired &&
			"channelSequence" in envelope &&
			envelope.channelSequence === "1" &&
			envelope.target.streamKey === head.reference.target.streamKey
		) {
			throw new Error("Synthetic destination unavailable");
		}
		return destination.ingest(envelope);
	});
	const worker = bind(database, adapter, { source: source.outbox, deliver });
	const host = await database.bellowsHost(schema);
	const run = (id: number) =>
		runTaskOnce(host.execution, worker.factory, 1, {
			type: "task",
			taskId: id,
		});
	expect(await run(later.id)).toMatchObject({ type: "retryAt" });
	expect(deliver).not.toHaveBeenCalled();
	const headOutcome = await run(head.id);
	expect(headOutcome).toMatchObject({ type: "retryAt" });
	expect(await run(head.id)).toEqual(headOutcome);
	expect(deliver).toHaveBeenCalledTimes(1);
	expect(await run(independent.id)).toEqual({ type: "done" });
	expect(deliver).toHaveBeenCalledTimes(2);
	// New emissions in the blocked channel cannot execute the predecessor either.
	await source.dispatch(publish, {
		id: "p",
		comments: [{ postId: "p", number: 1, body: "new" }],
	});
	const newest = only(
		(await tasks(database, schema)).filter(
			(row) =>
				row.name === outgoing.task.name &&
				row.id !== head.id &&
				row.id !== later.id,
		),
	);
	expect(await run(newest.id)).toMatchObject({ type: "retryAt" });
	expect(deliver).toHaveBeenCalledTimes(2);
	expect(diagnostics.map((value) => value.status)).toEqual([
		"predecessor",
		"failed",
		"predecessor",
	]);
	expect(
		(await sourceClient.comment.findMany()).map((value) => value.body),
	).toEqual(["independent"]);
	repaired = true;
	// Administrative time advancement for this test only; no polling or scheduler implementation.
	await database.admin.query(
		`UPDATE "${schema}".bellows_tasks SET available_from_unix_ms = 0 WHERE task_id = ANY($1::bigint[])`,
		[[head.id, later.id, newest.id]],
	);
	for (const id of [head.id, later.id, newest.id])
		expect(await run(id)).toEqual({ type: "done" });
	expect(
		await sourceClient.comment.findUniqueOrThrow({
			where: { postId_number: { postId: "p", number: 1 } },
		}),
	).toMatchObject({ body: "poisoned|later|new" });
	expect((await source.outbox.readPending()).envelopes).toEqual([]);
});

test("competing supplied workers deduplicate an independent occurrence and child publication through independent clients", async ({
	database,
}) => {
	const context = await setup(database);
	const { source, destination, outgoing, returning, schema, sourceClient } =
		context;
	await stageComments(context);
	const row = only(await tasks(database, schema));
	const reference = outgoing.task.codec.decode(row.payload);
	const competitor = createPostgresRuntime({
		client: database.client("competitor"),
		registry: destinationRegistry,
		schema: database.otherJournalSchema,
		publication: returning.publication,
	});
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let reads = 0;
	function read(value: OutgoingReference): Promise<OutgoingRead>;
	function read(
		value: PendingOccurrenceReference,
	): Promise<PendingOccurrenceRead>;
	function read(value: DeliveryReference): Promise<DeliveryRead>;
	async function read(value: DeliveryReference): Promise<DeliveryRead> {
		const retained = await source.outbox.read(value);
		if (++reads === 2) entered.resolve();
		await bounded(release.promise);
		return retained;
	}
	const sourceCapability = {
		read,
		acknowledge: source.outbox.acknowledge,
	};
	const outcomes: IngestionResult[] = [];
	const workers = [destination, competitor].map((target) =>
		bind(database, outgoing, {
			source: sourceCapability,
			async deliver(envelope) {
				const result = await target.ingest(envelope);
				outcomes.push(result);
				return result;
			},
		}),
	);
	// Redundant/stale execution calls bypass claims deliberately; destination dedup is
	// the business correctness boundary even if both invokers reach the worker.
	const attempts = workers.map((worker, index) =>
		settle(worker.factory.build(index + 1).process(row.id, reference)),
	);
	try {
		await bounded(entered.promise);
	} finally {
		release.resolve();
		await Promise.all(attempts);
	}
	for (const attempt of attempts) {
		const result = await attempt;
		expect(result.status).toBe("fulfilled");
		if (result.status === "fulfilled") {
			expect(result.value).toBeInstanceOf(TaskSuccess);
		}
	}
	expect(outcomes.map((value) => value.status).sort()).toEqual([
		"alreadyProcessed",
		"processed",
	]);
	expect(await sourceClient.comment.findMany()).toMatchObject([
		{ body: "First" },
	]);
	expect((await source.outbox.read(reference)).acknowledged).toBe(true);
	expect(
		(await tasks(database, schema)).filter(
			(value) => value.name === returning.task.name,
		),
	).toHaveLength(1);
	const host = await database.bellowsHost(schema);
	const finalizer = bind(database, outgoing, {
		source: source.outbox,
		deliver: destination.ingest,
	});
	expect(
		await runTaskOnce(host.execution, finalizer.factory, 3, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "done" });
	expect(only(await tasks(database, schema)).name).toBe(returning.task.name);
});

test("drain waits for independent ingestion, acknowledgment and diagnostics after simulated lease loss", async ({
	database,
}) => {
	const context = await setup(database);
	const { source, destination, outgoing, schema, sourceClient } = context;
	await stageComments(context);
	const row = only(await tasks(database, schema));
	const reference = outgoing.task.codec.decode(row.payload);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const acknowledging = Promise.withResolvers<void>();
	const releaseAcknowledgment = Promise.withResolvers<void>();
	const diagnosing = Promise.withResolvers<void>();
	const releaseDiagnostic = Promise.withResolvers<void>();
	const adapter = createBellowsDelivery({
		schema,
		taskName: outgoing.task.name,
		now: () => 0,
		async onDiagnostic() {
			diagnosing.resolve();
			await bounded(releaseDiagnostic.promise);
		},
	});
	let loseAcknowledgment = true;
	const worker = bind(database, adapter, {
		source: {
			read: source.outbox.read,
			async acknowledge(envelope) {
				acknowledging.resolve();
				await bounded(releaseAcknowledgment.promise);
				await source.outbox.acknowledge(envelope);
				if (loseAcknowledgment) {
					loseAcknowledgment = false;
					throw new Error("Synthetic lost acknowledgment response");
				}
			},
		},
		async deliver(envelope) {
			entered.resolve();
			await bounded(release.promise);
			return destination.ingest(envelope);
		},
	});
	// Public backend lifecycle double, not real lease timing or platform termination.
	// The actual runtime stops waiting when renewal reports lease loss.
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
		async fail() {
			throw new Error("Unexpected finalization");
		},
		async finish() {
			throw new Error("Unexpected finalization");
		},
	} satisfies TaskExecutionBackend;
	const invocation = settle(
		runTaskOnce(backend, worker.factory, 1, { type: "task", taskId: row.id }),
	);
	const drained = vi.fn();
	let draining: Promise<void> | undefined;
	try {
		await bounded(entered.promise);
		expect(await bounded(invocation)).toEqual({
			status: "fulfilled",
			value: { type: "retry" },
		});
		draining = worker.drain().then(drained);
		await Promise.resolve();
		expect(drained).not.toHaveBeenCalled();
		expect(await sourceClient.comment.count()).toBe(0);
		release.resolve();
		await bounded(acknowledging.promise);
		expect(drained).not.toHaveBeenCalled();
		expect((await source.outbox.read(reference)).acknowledged).toBe(false);
		releaseAcknowledgment.resolve();
		await bounded(diagnosing.promise);
		expect(drained).not.toHaveBeenCalled();
		expect((await source.outbox.read(reference)).acknowledged).toBe(true);
	} finally {
		release.resolve();
		releaseAcknowledgment.resolve();
		releaseDiagnostic.resolve();
		await invocation;
		await worker.drain();
		await draining;
	}
	expect(drained).toHaveBeenCalledOnce();
	expect((await source.outbox.read(reference)).acknowledged).toBe(true);
	expect(await sourceClient.comment.findMany()).toMatchObject([
		{ body: "First" },
	]);
	// A real later host can finish the still-pending task without repeating ingestion.
	const host = await database.bellowsHost(schema);
	expect(
		await runTaskOnce(host.execution, worker.factory, 2, {
			type: "task",
			taskId: row.id,
		}),
	).toEqual({ type: "done" });
	await worker.drain();
});

for (const failure of [
	"gap",
	"missing",
	"malformed",
	"unknown implementation",
	"content conflict",
	"business rejection",
] as const) {
	test(`real worker boundary: ${failure} does not silently lose pending work`, async ({
		database,
	}) => {
		const diagnostics: BellowsDeliveryDiagnostic[] = [];
		const context = await setup(database, {
			onDiagnostic: (value) => {
				diagnostics.push(value);
			},
		});
		const { source, destination, outgoing, schema, sourceClient } = context;
		await source.dispatch(publish, {
			id: "p",
			comments: [
				{
					postId: "p",
					number: 1,
					body: failure === "business rejection" ? "rejected" : "First",
				},
			],
		});
		const row = only(await tasks(database, schema));
		let reference = outgoing.task.codec.decode(row.payload);
		let target = destination.ingest;
		if (failure === "gap") {
			// Deliberately finished manual predecessor in the source only: the
			// destination has no incoming history and must refuse sequence two.
			await source.outbox.acknowledge(
				(await source.outbox.read(reference)).envelope,
			);
			await source.dispatch(publish, {
				id: "p",
				comments: [{ postId: "p", number: 1, body: "Second" }],
			});
			const later = only(
				(await tasks(database, schema)).filter((value) => value.id !== row.id),
			);
			reference = outgoing.task.codec.decode(later.payload);
		} else if (failure === "missing") {
			reference = { ...reference, channelSequence: "99" };
		} else if (failure === "unknown implementation") {
			target = createPostgresRuntime({
				client: database.client("unknown"),
				registry: definitions.registry({
					streams: [post, comment],
					events: [add],
					handlers: [],
				}),
				schema: database.otherJournalSchema,
			}).ingest;
		} else if (failure === "content conflict") {
			const envelope = (await source.outbox.read(reference)).envelope;
			expect(
				await destination.ingest({
					...envelope,
					encodedInput: JSON.stringify({
						postId: "p",
						number: 1,
						body: "Other",
					}),
				}),
			).toEqual({ status: "processed" });
		}
		const worker = bind(database, outgoing, {
			source: source.outbox,
			deliver: target,
		});
		// Missing/malformed payloads are administrative corruption, never produced
		// by the strict publisher. Real Bellows decoding must also fail closed.
		if (failure === "missing" || failure === "malformed") {
			await database.admin.query(
				`UPDATE "${schema}".bellows_tasks SET payload_json = $1 WHERE task_id = $2`,
				[
					failure === "malformed"
						? '{"channelSequence":0}'
						: outgoing.task.codec.encode(reference),
					row.id,
				],
			);
		}
		const selected =
			failure === "gap"
				? only(
						(await tasks(database, schema)).filter(
							(value) => value.id !== row.id,
						),
					)
				: row;
		const host = await database.bellowsHost(schema);
		const result = await runTaskOnce(host.execution, worker.factory, 1, {
			type: "task",
			taskId: selected.id,
		});
		if (failure === "business rejection") {
			expect(result).toEqual({ type: "done" });
			expect((await source.outbox.read(reference)).acknowledged).toBe(true);
			expect(await tasks(database, schema)).toEqual([]);
		} else {
			expect(result).toEqual(
				failure === "malformed"
					? { type: "retry" }
					: { type: "retryAt", availableFromMs: 5_000 },
			);
			expect(
				(await tasks(database, schema)).some(
					(value) => value.id === selected.id,
				),
			).toBe(true);
			const retainedReference =
				failure === "missing"
					? outgoing.task.codec.decode(row.payload)
					: reference;
			expect((await source.outbox.read(retainedReference)).acknowledged).toBe(
				false,
			);
			if (failure !== "malformed") {
				expect(diagnostics).toEqual([
					{
						taskId: selected.id,
						taskName: outgoing.task.name,
						stage:
							failure === "gap"
								? "processing"
								: failure === "missing"
									? "read"
									: "delivery",
						status: failure === "gap" ? "gap" : "failed",
					},
				]);
			}
		}
		expect(await sourceClient.comment.count()).toBe(
			failure === "content conflict" ? 1 : 0,
		);
	});
}
