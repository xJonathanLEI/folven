import type { EffectRequester } from "../effect.js";
import type { CapturedEffect } from "./effect-registration.js";
import { encodeInput, parseWireInput, payloadFormat } from "./encoding.js";
import {
	EffectRequestClosedError,
	EffectRequestFailedError,
} from "./errors.js";
import type { CanonicalEffectLookup } from "./registry.js";

/** One synchronous, I/O-free collector per source transaction attempt. */
export function createEffectCollector(effects: CanonicalEffectLookup) {
	const captured: CapturedEffect[] = [];
	let closed = false;
	let failed = false;
	const requestEffect: EffectRequester = (effect, input) => {
		if (closed) {
			throw new EffectRequestClosedError(
				"This attempt's effect collector is closed.",
			);
		}
		if (failed) {
			throw new EffectRequestFailedError(
				"This attempt has a failed effect request.",
			);
		}
		try {
			const canonical = effects.getEffect(effect);
			const encodedInput = encodeInput(input);
			// Validate reconstructed schema input, but retain its original bytes,
			// never the potentially transformed or stripped schema output.
			canonical.schema.parse(parseWireInput(encodedInput));
			captured.push(
				Object.freeze({
					effectName: canonical.name,
					effectVersion: canonical.version,
					payloadFormat,
					encodedInput,
				}),
			);
		} catch (error) {
			failed = true;
			throw error;
		}
	};
	return Object.freeze({
		requestEffect,
		close(): void {
			closed = true;
		},
		finish(): readonly CapturedEffect[] {
			closed = true;
			if (failed) {
				throw new EffectRequestFailedError(
					"This attempt has a failed effect request.",
				);
			}
			return Object.freeze([...captured]);
		},
	});
}
