import {
	runTaskOnce,
	type PublishTaskDefinition,
	type TaskExecutionBackend,
	type WorkerFactory,
} from "@xjonathanlei/bellows";
import { createPostgresProcessorTask } from "@xjonathanlei/bellows/cloudflare/postgres";
import { z } from "zod";
import type {
	Prisma,
	PrismaClient,
} from "../../tests/fixtures/generated/client.js";
import type { DeliverySource } from "../delivery/index.js";
import { createEffectRunner } from "../effects/index.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import {
	createPostgresRuntime,
	createPostgresPublisher,
	createPostgresEffectStore,
	type EffectReference,
	type DeliveryReference,
	type OutgoingRead,
	type PendingOccurrenceRead,
} from "../postgres/index.js";
import { createBellowsDelivery, createBellowsEffects } from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const user = definitions.stream({
	name: "user",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});

const read = defineEvent({
	name: "user.read",
	version: 1,
	stream: user,
	schema: z.strictObject({ id: z.string() }),
	route: (input) => input,
});
const registry = definitions.registry({
	streams: [user],
	events: [read],
	handlers: [
		defineHandler({
			event: read,
			handle: ({ db, scope }) =>
				db.user.findUnique({
					where: scope.user.unique,
					select: { name: true },
				}),
		}),
	],
});
declare const client: PrismaClient;
declare const backend: TaskExecutionBackend;
const delivery = createBellowsDelivery({
	schema: "tasks",
	onTasksCommitted(receipts) {
		for (const receipt of receipts) {
			const id: number = receipt.taskId;
			void id;
			// @ts-expect-error Notification receipts never contain event bodies.
			void receipt.encodedInput;
		}
	},
});
const runtime = createPostgresRuntime({
	client,
	registry,
	schema: "events",
	publication: delivery.publication,
});
const source: DeliverySource = runtime.outbox;
const worker = delivery.createWorkerFactory({
	source,
	deliver: runtime.ingest,
});
const task: PublishTaskDefinition<DeliveryReference, undefined> = delivery.task;
const factory: WorkerFactory<typeof task> = worker.factory;
void runTaskOnce(backend, factory, 1, { type: "task", taskId: 1 });
void createPostgresProcessorTask(factory);
void worker.drain();
const publisher = createPostgresPublisher({
	registry: definitions.registry({
		streams: [user],
		events: [read],
		handlers: [],
	}),
	schema: "events",
	publication: delivery.publication,
});
const prepared = publisher.prepare(read, { id: "u" });
async function handoff() {
	const staged = await client.$transaction((transaction) =>
		publisher.stage(transaction, [prepared]),
	);
	await staged.notifyCommitted();
}
void handoff;
const pending = { occurrenceId: "00000000-0000-4000-8000-000000000001" };
task.codec.encode(pending);
const pendingRead: Promise<PendingOccurrenceRead> = source.read(pending);
const channelRead: Promise<OutgoingRead> = source.read({
	source: { streamName: "user", streamKey: '{"id":"u"}' },
	target: { streamName: "user", streamKey: '{"id":"u"}' },
	channelSequence: "1",
});
void pendingRead;
void channelRead;
const result = runtime.dispatch(read, { id: "u" });
type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;
export type Result = Expect<
	Equal<typeof result, Promise<{ name: string } | null>>
>;
task.codec.encode({
	source: { streamName: "user", streamKey: '{"id":"u"}' },
	target: { streamName: "user", streamKey: '{"id":"u"}' },
	// @ts-expect-error Channel positions are lossless decimal strings, never numbers.
	channelSequence: 1,
});

const effects = createBellowsEffects({ schema: "tasks" });
const effectStore = createPostgresEffectStore({
	client,
	registry,
	schema: "events",
	publication: delivery.publication,
});
const effectRunner = createEffectRunner({ registry, store: effectStore });
const effectWorker = effects.createWorkerFactory({
	runner: effectRunner,
	async onFailure(failure) {
		if (failure.proposal) await effectStore.complete(failure.proposal);
		// @ts-expect-error No arbitrary provider error in policy context.
		void failure.cause;
		return { action: "manual" };
	},
});
const effectTask: PublishTaskDefinition<EffectReference, undefined> =
	effects.task;
void effectTask;
void runTaskOnce(backend, effectWorker.factory, 1, { type: "task", taskId: 1 });
void createPostgresProcessorTask(effectWorker.factory);
void effectWorker.drain();
createPostgresRuntime({
	client,
	registry,
	schema: "events",
	publication: delivery.publication,
	effectPublication: effects.publication,
});
// @ts-expect-error An explicit failure policy is required.
effects.createWorkerFactory({ runner: effectRunner });
createPostgresEffectStore({
	client,
	registry,
	schema: "events",
	// @ts-expect-error Receipt events need delivery publication, never effect task publication.
	publication: effects.publication,
});
