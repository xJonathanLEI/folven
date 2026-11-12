import type { EventEmitter } from "../handler.js";
import { EmissionClosedError, EmissionFailedError } from "./errors.js";
import { prepareEvent } from "./occurrence.js";
import type { CanonicalEventLookup } from "./registry.js";
import type { CapturedOutput } from "./types.js";

/** One synchronous, in-memory collector per protected transaction attempt. */
export function createOutputCollector(events: CanonicalEventLookup) {
	const outputs: CapturedOutput[] = [];
	let closed = false;
	let failed = false;

	const emit: EventEmitter = (event, input) => {
		if (closed) {
			throw new EmissionClosedError("This attempt's emitter is closed.");
		}
		if (failed) {
			throw new EmissionFailedError("This attempt has a failed emission.");
		}
		try {
			const { prepared } = prepareEvent(events.getEvent(event), input);
			outputs.push(
				Object.freeze({
					target: Object.freeze({
						streamName: prepared.streamName,
						streamKey: prepared.streamKey,
					}),
					eventName: prepared.eventName,
					eventVersion: prepared.eventVersion,
					payloadFormat: prepared.payloadFormat,
					encodedInput: prepared.encodedInput,
				}),
			);
		} catch (error) {
			// User code may catch the immediate validation failure, but it must
			// never turn the remainder of this attempt into a committable batch.
			failed = true;
			throw error;
		}
	};

	return Object.freeze({
		emit,
		close(): void {
			closed = true;
		},
		finish(): readonly CapturedOutput[] {
			closed = true;
			if (failed) {
				throw new EmissionFailedError("This attempt has a failed emission.");
			}
			return Object.freeze([...outputs]);
		},
	});
}
