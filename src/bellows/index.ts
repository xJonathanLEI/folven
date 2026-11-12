import {
	definePublishTask,
	TaskFailure,
	TaskSuccess,
	type TaskResult,
	type WorkerFactory,
} from "@xjonathanlei/bellows";
import { createDeliveryRunner, DeliveryError } from "../delivery/index.js";
import type {
	DeliveryReference,
	PostgresPublication,
} from "../postgres/types.js";
import { publicationPreparer } from "./publication.js";
import type {
	BellowsDelivery,
	BellowsDeliveryDiagnostic,
	BellowsDeliveryOptions,
	DeliveryTask,
} from "./types.js";
import { parseConfiguration, referenceCodec, retryDeadline } from "./utils.js";

export { createBellowsEffects } from "./effects.js";
export type {
	BellowsEffects,
	BellowsEffectsOptions,
	BellowsEffectWorker,
	BellowsEffectWorkerOptions,
	BellowsEffectDiagnostic,
	EffectFailureContext,
	EffectFailureDecision,
	EffectFailurePolicy,
	EffectTask,
	EffectTaskReceipt,
} from "./effect-types.js";
export type {
	BellowsDelivery,
	BellowsDeliveryDiagnostic,
	BellowsDeliveryOptions,
	BellowsDeliveryWorker,
	DeliveryTask,
	DeliveryTaskReceipt,
} from "./types.js";

/** Caller-owned infrastructure only. Construction performs no I/O. */
export function createBellowsDelivery(
	options: BellowsDeliveryOptions,
): BellowsDelivery {
	const { schema, taskName, retryDelayMs } = parseConfiguration({
		schema: options.schema,
		taskName: options.taskName ?? "folven_delivery",
		retryDelayMs: options.retryDelayMs ?? 5_000,
	});
	const { now = Date.now, onTasksCommitted, onDiagnostic } = options;
	const task = Object.freeze(
		definePublishTask<DeliveryReference>(taskName, referenceCodec),
	);

	async function diagnose(
		diagnostic: BellowsDeliveryDiagnostic,
	): Promise<void> {
		try {
			await onDiagnostic?.(Object.freeze(diagnostic));
		} catch {
			// Observers never change transaction or acknowledgment outcomes.
		}
	}

	const publication: PostgresPublication = Object.freeze({
		prepare: publicationPreparer(task, schema, async (committed) => {
			try {
				await onTasksCommitted?.(committed);
			} catch {
				for (const receipt of committed) {
					await diagnose({
						...receipt,
						stage: "notification",
						status: "failed",
					});
				}
			}
		}),
	} satisfies PostgresPublication);

	return Object.freeze({
		task,
		publication,
		createWorkerFactory(options) {
			const runner = createDeliveryRunner(options);
			const active = new Set<Promise<TaskResult<undefined>>>();

			async function process(
				taskId: number,
				reference: DeliveryReference,
			): Promise<TaskResult<undefined>> {
				if (!Number.isSafeInteger(taskId) || taskId <= 0) {
					throw new Error("Invalid Bellows delivery task identity.");
				}
				try {
					const result = await runner.attempt(reference);
					if (result.status === "completed") return TaskSuccess.done(undefined);
					await diagnose({
						taskId,
						taskName,
						stage: "processing",
						status: result.reason,
					});
				} catch (error) {
					await diagnose({
						taskId,
						taskName,
						stage: error instanceof DeliveryError ? error.stage : "processing",
						status: "failed",
					});
				}
				return TaskFailure.retryAt(retryDeadline(now, retryDelayMs));
			}

			const factory: WorkerFactory<DeliveryTask> = Object.freeze({
				task,
				build() {
					return {
						process(taskId, reference) {
							// Track before starting work, including asynchronous diagnostics.
							// Lease loss can stop Bellows waiting, not this business promise.
							const operation = Promise.resolve().then(() =>
								process(taskId, reference),
							);
							active.add(operation);
							void operation.then(
								() => {
									active.delete(operation);
								},
								() => {
									active.delete(operation);
								},
							);
							return operation;
						},
					};
				},
			} satisfies WorkerFactory<DeliveryTask>);
			return Object.freeze({
				factory,
				async drain() {
					while (active.size > 0) await Promise.allSettled(active);
				},
			});
		},
	} satisfies BellowsDelivery);
}
