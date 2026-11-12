import {
	definePublishTask,
	TaskFailure,
	TaskSuccess,
	type TaskResult,
	type WorkerFactory,
} from "@xjonathanlei/bellows";
import { EffectExecutionError } from "../effects/index.js";
import type {
	EffectReference,
	PostgresEffectPublication,
} from "../postgres/types.js";
import type {
	BellowsEffects,
	BellowsEffectsOptions,
	BellowsEffectDiagnostic,
	EffectFailureContext,
	EffectTask,
} from "./effect-types.js";
import { publicationPreparer } from "./publication.js";
import {
	effectReferenceCodec,
	parseEffectConfiguration,
	parseFailureDecision,
} from "./utils.js";

/** Optional claimed host. No client ownership, external-action fencing or implicit retry policy. */
export function createBellowsEffects(
	options: BellowsEffectsOptions,
): BellowsEffects {
	const { schema, taskName } = parseEffectConfiguration({
		schema: options.schema,
		taskName: options.taskName ?? "folven_effect",
	});
	const { now = Date.now, onTasksCommitted, onDiagnostic } = options;
	const task = Object.freeze(
		definePublishTask<EffectReference>(taskName, effectReferenceCodec),
	);

	async function diagnose(diagnostic: BellowsEffectDiagnostic): Promise<void> {
		try {
			await onDiagnostic?.(Object.freeze(diagnostic));
		} catch {
			// Observers cannot turn manual handling into Bellows's automatic retry.
		}
	}

	const publication: PostgresEffectPublication = Object.freeze({
		kind: "effect",
		prepare: publicationPreparer(task, schema, async (receipts) => {
			try {
				await onTasksCommitted?.(receipts);
			} catch {
				for (const receipt of receipts) {
					await diagnose({
						...receipt,
						stage: "notification",
						status: "failed",
					});
				}
			}
		}),
	});

	return Object.freeze({
		task,
		publication,
		createWorkerFactory({ runner, onFailure }) {
			if (typeof onFailure !== "function") {
				throw new TypeError("A Bellows effect failure policy is required.");
			}
			const active = new Set<Promise<TaskResult<undefined>>>();

			async function process(
				taskId: number,
				reference: EffectReference,
			): Promise<TaskResult<undefined>> {
				let failure: EffectFailureContext;
				try {
					const result = await runner.attempt(reference);
					if (
						result.status !== "completed" &&
						result.status !== "alreadyCompleted"
					)
						throw new Error();
					return TaskSuccess.done(undefined);
				} catch (error) {
					failure = Object.freeze({
						taskId,
						taskName,
						reference,
						stage:
							error instanceof EffectExecutionError
								? error.stage
								: "processing",
						externalWork:
							error instanceof EffectExecutionError
								? error.externalWork
								: "mayHaveRun",
						...(error instanceof EffectExecutionError &&
						error.proposal !== undefined
							? { proposal: error.proposal }
							: {}),
					});
				}

				try {
					// Includes async policy, strict result parsing and clock validation.
					// Never let ordinary caught failures escape into Bellows's immediate retry.
					const decision = parseFailureDecision(await onFailure(failure), now);
					await diagnose({
						taskId,
						taskName,
						stage: failure.stage,
						status: decision.action === "manual" ? "manual" : "retry",
					});
					return decision.action === "manual"
						? TaskSuccess.done(undefined)
						: TaskFailure.retryAt(decision.availableFromMs);
				} catch {
					await diagnose({
						taskId,
						taskName,
						stage: "policy",
						status: "manual",
					});
					return TaskSuccess.done(undefined);
				}
			}

			const factory: WorkerFactory<EffectTask> = Object.freeze({
				task,
				build() {
					return {
						process(taskId, reference) {
							// Retain the business promise even if Bellows stops awaiting after lease loss.
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
			} satisfies WorkerFactory<EffectTask>);
			return Object.freeze({
				factory,
				async drain() {
					while (active.size > 0) await Promise.allSettled(active);
				},
			});
		},
	} satisfies BellowsEffects);
}
