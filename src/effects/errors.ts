import type { PreparedEffectCompletion } from "../postgres/effect-store.js";
import type { EffectExecutionStage } from "./types.js";

const messages = {
	read: "Could not read an established effect matching the reference.",
	definition: "The outstanding effect requires a registered implementation.",
	input: "The stored effect input could not be decoded.",
	handler: "The external handler failed; external actions may have occurred.",
	receipt: "The handler returned an invalid completion receipt.",
	completionAborted:
		"Effect completion exhausted three known-aborted transactions.",
	completionUnknown:
		"Effect completion failed or its commit outcome is unknown.",
} satisfies Record<EffectExecutionStage, string>;

/** Safe default diagnostics. No arbitrary cause or recoverable business result. */
export class EffectExecutionError extends Error {
	override readonly name = "EffectExecutionError";
	readonly externalWork: "notInvoked" | "mayHaveRun";

	constructor(
		readonly stage: EffectExecutionStage,
		/** Opaque capture for deliberate completion-only retry, never external replay. */
		readonly proposal?: PreparedEffectCompletion,
	) {
		super(messages[stage]);
		this.externalWork = ["read", "definition", "input"].includes(stage)
			? "notInvoked"
			: "mayHaveRun";
	}
}
