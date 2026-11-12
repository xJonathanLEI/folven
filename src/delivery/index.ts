import { parseDeliveryReference } from "../postgres/protocol.js";
import type {
	DeliveryRead,
	DeliveryReference,
	IngestionResult,
} from "../postgres/types.js";
import { DeliveryError } from "./errors.js";
import type { DeliveryOptions, DeliveryResult } from "./types.js";
import { parseDeliveryRead, parseIngestionResult } from "./utils.js";

export { DeliveryError } from "./errors.js";
export type {
	DeliveryOptions,
	DeliveryResult,
	DeliverySource,
	DeliveryStage,
} from "./types.js";

/** One read–ingest–acknowledge attempt, without retries, locks or scheduling. */
export function createDeliveryRunner({ source, deliver }: DeliveryOptions) {
	return Object.freeze({
		async attempt(reference: DeliveryReference): Promise<DeliveryResult> {
			let read: DeliveryRead;
			try {
				const parsed = parseDeliveryReference(reference);
				read = parseDeliveryRead(await source.read(parsed), parsed);
			} catch {
				throw new DeliveryError("read");
			}
			if (read.acknowledged) return { status: "completed" };
			if ("hasPredecessor" in read && read.hasPredecessor) {
				return { status: "waiting", reason: "predecessor" };
			}

			let response: unknown;
			try {
				response = await deliver(read.envelope);
			} catch {
				throw new DeliveryError("delivery");
			}
			let result: IngestionResult;
			try {
				result = parseIngestionResult(response);
			} catch {
				throw new DeliveryError("response");
			}
			if (result.status === "gap") {
				if ("occurrenceId" in read.envelope) {
					throw new DeliveryError("response");
				}
				return {
					status: "waiting",
					reason: "gap",
					expectedSequence: result.expectedSequence,
				};
			}

			try {
				await source.acknowledge(read.envelope);
			} catch {
				// Even if it committed, retry only this retained identity. A later
				// read can finish without redelivering an acknowledged output.
				throw new DeliveryError("acknowledgment");
			}
			return { status: "completed" };
		},
	});
}
