import type { DeliveryStage } from "./types.js";

const messages = {
	read: "Could not read a valid retained output matching the reference.",
	delivery: "Destination ingestion failed or its outcome is unknown.",
	response: "Destination returned an invalid ingestion acknowledgment.",
	acknowledgment: "Source acknowledgment failed or its outcome is unknown.",
} satisfies Record<DeliveryStage, string>;

/** Deliberately omits arbitrary causes, payloads, identities and provider details. */
export class DeliveryError extends Error {
	override readonly name = "DeliveryError";

	constructor(readonly stage: DeliveryStage) {
		super(messages[stage]);
	}
}
