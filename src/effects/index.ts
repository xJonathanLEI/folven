import type { EventRegistration } from "../event.js";
import type { HandlerRegistration } from "../handler.js";
import {
	effectId,
	parseEffectReference,
	parseOutstandingEffect,
} from "../postgres/effect-protocol.js";
import { parseWireInput } from "../postgres/encoding.js";
import { createHandlerLookup } from "../postgres/registry.js";
import type { EffectReference, OutstandingEffect } from "../postgres/types.js";
import type { DefinitionRegistry } from "../registry.js";
import type { ScopeStream } from "../scope.js";
import { EffectExecutionError } from "./errors.js";
import type { EffectCompletionResult, EffectStore } from "./types.js";

export { EffectExecutionError } from "./errors.js";
export type {
	EffectStore,
	EffectExecutionStage,
	EffectCompletionResult,
} from "./types.js";
export type { PreparedEffectCompletion } from "../postgres/effect-store.js";

/** One caller-assigned attempt. External work is never inside a retry loop. */
export function createEffectRunner<
	Definitions extends DefinitionRegistry<
		readonly ScopeStream[],
		readonly EventRegistration[],
		readonly HandlerRegistration[]
	>,
>(options: { readonly registry: Definitions; readonly store: EffectStore }) {
	const lookup = createHandlerLookup(options.registry);
	const store = options.store;
	return Object.freeze({
		async attempt(reference: EffectReference): Promise<EffectCompletionResult> {
			let effect: OutstandingEffect;
			try {
				const parsed = parseEffectReference(reference);
				const read = await store.read(parsed);
				if (read.status === "alreadyCompleted")
					return { status: "alreadyCompleted" };
				if (read.status !== "outstanding") throw new Error();
				effect = parseOutstandingEffect(read.effect);
				if (
					effect.reference.source.streamName !== parsed.source.streamName ||
					effect.reference.source.streamKey !== parsed.source.streamKey ||
					effect.reference.effectSequence !== parsed.effectSequence
				)
					throw new Error();
			} catch {
				throw new EffectExecutionError("read");
			}
			let handler;
			try {
				handler = lookup.requireEffectHandler(
					lookup.resolveEffect(effect.effectName, effect.effectVersion),
				);
			} catch {
				throw new EffectExecutionError("definition");
			}
			let payload: unknown;
			try {
				payload = handler.effect.schema.parse(
					parseWireInput(effect.encodedInput),
				);
			} catch {
				throw new EffectExecutionError("input");
			}
			let receipt;
			try {
				receipt = await handler.handle({
					effect: payload,
					effectId: effectId(effect.reference),
				});
			} catch {
				// Even a P2034-shaped external error must not retry this callback.
				throw new EffectExecutionError("handler");
			}
			let proposal;
			try {
				proposal = store.prepareCompletion(effect, receipt);
			} catch {
				throw new EffectExecutionError("receipt");
			}
			try {
				const result = await store.complete(proposal);
				if (
					result.status !== "completed" &&
					result.status !== "alreadyCompleted"
				)
					throw new Error();
				return { status: result.status };
			} catch (error) {
				if (
					error instanceof EffectExecutionError &&
					(error.stage === "completionAborted" ||
						error.stage === "completionUnknown")
				)
					throw new EffectExecutionError(error.stage, proposal);
				throw new EffectExecutionError("completionUnknown", proposal);
			}
		},
	});
}
