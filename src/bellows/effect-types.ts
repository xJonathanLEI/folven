import type {
	PublishTaskDefinition,
	WorkerFactory,
} from "@xjonathanlei/bellows";
import type {
	createEffectRunner,
	EffectExecutionStage,
	PreparedEffectCompletion,
} from "../effects/index.js";
import type {
	EffectReference,
	PostgresEffectPublication,
} from "../postgres/types.js";

export interface EffectTaskReceipt {
	readonly taskId: number;
	readonly taskName: string;
}

/** Bounded diagnostics: no reference keys, proposals, payloads or arbitrary causes. */
export interface BellowsEffectDiagnostic extends EffectTaskReceipt {
	readonly stage:
		| EffectExecutionStage
		| "processing"
		| "policy"
		| "notification";
	readonly status: "retry" | "manual" | "failed";
}

/** Host-only context, not a diagnostic. Retain proposals for completion-only retry. */
export interface EffectFailureContext extends EffectTaskReceipt {
	readonly reference: EffectReference;
	readonly stage: EffectExecutionStage | "processing";
	readonly externalWork: "notInvoked" | "mayHaveRun";
	readonly proposal?: PreparedEffectCompletion;
}

export type EffectFailureDecision =
	| { readonly action: "retryAt"; readonly availableFromMs: number }
	| { readonly action: "manual" };

export type EffectFailurePolicy = (
	failure: EffectFailureContext,
) => EffectFailureDecision | Promise<EffectFailureDecision>;

export interface BellowsEffectsOptions {
	/** Explicit, separately installed Bellows schema in the source database. */
	readonly schema: string;
	/** Default: folven_effect. Must differ from delivery and unrelated task names. */
	readonly taskName?: string;
	/** Unix milliseconds used to validate policy deadlines. Defaults to Date.now. */
	readonly now?: () => number;
	/** Confirmed-commit notification only; failure never replays publication. */
	readonly onTasksCommitted?: (
		receipts: readonly EffectTaskReceipt[],
	) => void | Promise<void>;
	/** Awaited and isolated, including during drain. */
	readonly onDiagnostic?: (
		diagnostic: BellowsEffectDiagnostic,
	) => void | Promise<void>;
}

export interface BellowsEffectWorkerOptions {
	readonly runner: ReturnType<typeof createEffectRunner>;
	/** Required. Retry may repeat external work; manual retires only this task. */
	readonly onFailure: EffectFailurePolicy;
}

export type EffectTask = PublishTaskDefinition<EffectReference, undefined>;

export interface BellowsEffectWorker {
	readonly factory: WorkerFactory<EffectTask>;
	/** Stop host admission first. Does not cancel or fence overlapping workers. */
	drain(): Promise<void>;
}

export interface BellowsEffects {
	readonly task: EffectTask;
	readonly publication: PostgresEffectPublication;
	createWorkerFactory(options: BellowsEffectWorkerOptions): BellowsEffectWorker;
}
