import { createPostgresProcessorTask } from "@xjonathanlei/bellows/cloudflare/postgres";
import {
	createBellowsDelivery,
	createBellowsEffects,
	type EffectFailurePolicy,
} from "../src/bellows/index.js";
import type { DeliveryOptions } from "../src/delivery/index.js";
import { createPostgresRuntime } from "../src/postgres/index.js";
import type { PrismaClient } from "../tests/fixtures/generated/client.js";
import {
	createIndexingExecutor,
	producerDefinitions,
	type PostIndexer,
} from "./effects.js";

/** Host the factories alongside unrelated tasks; this creates no deployment or clients. */
export function createExampleEffectsHost(
	client: PrismaClient,
	indexer: PostIndexer,
	deliver: DeliveryOptions["deliver"],
	onFailure: EffectFailurePolicy,
) {
	const delivery = createBellowsDelivery({
		schema: "tasks",
		taskName: "index_delivery",
	});
	const effects = createBellowsEffects({
		schema: "tasks",
		taskName: "index_effect",
	});
	const source = createPostgresRuntime({
		client,
		registry: producerDefinitions,
		schema: "posts_events",
		publication: delivery.publication,
		effectPublication: effects.publication,
	});
	// A command registry may use the same publication options without local effect implementations.
	const { store, runner } = createIndexingExecutor(
		client,
		"posts_events",
		indexer,
		delivery.publication,
	);
	const effectWorker = effects.createWorkerFactory({ runner, onFailure });
	const deliveryWorker = delivery.createWorkerFactory({
		source: source.outbox,
		deliver,
	});
	return {
		source,
		store,
		// Public definition for deliberate rescheduling after inspection/reconciliation.
		effectTask: effects.task,
		factories: [effectWorker.factory, deliveryWorker.factory],
		tasks: [
			createPostgresProcessorTask(effectWorker.factory),
			createPostgresProcessorTask(deliveryWorker.factory),
		],
		async drain() {
			// Stop admission first; also settle unrelated tasks and host-owned notifications.
			await Promise.all([effectWorker.drain(), deliveryWorker.drain()]);
		},
	};
}

// Conservative example, not a universal safety policy. Manual handling retires
// the task, leaving the effect outstanding. A real host must observe diagnostics
// and retain/reconcile proposals when needed. Retry is a deliberate host choice:
// return { action: "retryAt", availableFromMs: Date.now() + 60_000 } only when
// repeated external execution is acceptable under the provider's contract.
export const manualReconciliation: EffectFailurePolicy = () => ({
	action: "manual",
});
