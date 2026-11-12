import type { EffectReceipt } from "../effect-handler.js";
import type { PreparedEffectCompletion } from "../postgres/effect-store.js";
import type {
	EffectRead,
	EffectReference,
	OutstandingEffect,
} from "../postgres/types.js";

/** No scheduling, transaction ownership or client lifetime in the neutral runner. */
export interface EffectStore {
	read(reference: EffectReference): Promise<EffectRead>;
	prepareCompletion(
		effect: OutstandingEffect,
		receipt: EffectReceipt,
	): PreparedEffectCompletion;
	complete(proposal: PreparedEffectCompletion): Promise<EffectCompletionResult>;
}

export type EffectCompletionResult = {
	readonly status: "completed" | "alreadyCompleted";
};

/** Bounded facts for host policy, never arbitrary provider errors or payloads. */
export type EffectExecutionStage =
	| "read"
	| "definition"
	| "input"
	| "handler"
	| "receipt"
	| "completionAborted"
	| "completionUnknown";
