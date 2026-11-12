import { z } from "zod";
import { validateName } from "../ownership.js";
import {
	parseOutgoingEnvelope,
	parseOutgoingReference,
	parseStreamIdentity,
} from "./channel.js";
import { parseWireInput } from "./encoding.js";
import { PendingOccurrenceEnvelopeError } from "./errors.js";
import { validateIdentityText } from "./identity.js";
import type {
	DeliveryEnvelope,
	DeliveryReference,
	PendingOccurrenceEnvelope,
	PendingOccurrenceReference,
} from "./types.js";

const referenceSchema = z.strictObject({
	occurrenceId: z.uuid().regex(/^[0-9a-f-]+$/),
});
const envelopeSchema = referenceSchema.extend({
	target: z.unknown(),
	eventName: z.string(),
	eventVersion: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER),
	payloadFormat: z.literal("json-v1"),
	encodedInput: z.string(),
});

export function parsePendingOccurrenceReference(
	value: unknown,
): PendingOccurrenceReference {
	const parsed = referenceSchema.safeParse(value);
	if (!parsed.success) {
		throw new PendingOccurrenceEnvelopeError(
			"Invalid pending occurrence reference structure.",
		);
	}
	return Object.freeze(parsed.data);
}

/** Wire validation only; the registered event owns schema parsing and routing. */
export function parsePendingOccurrenceEnvelope(
	value: unknown,
): PendingOccurrenceEnvelope {
	const parsed = envelopeSchema.safeParse(value);
	if (!parsed.success) {
		throw new PendingOccurrenceEnvelopeError(
			"Invalid pending occurrence envelope structure.",
		);
	}
	const envelope = parsed.data;
	const target = parseStreamIdentity(envelope.target);
	validateName(envelope.eventName, "Event name");
	validateIdentityText(envelope.eventName);
	parseWireInput(envelope.encodedInput);
	return Object.freeze({ ...envelope, target });
}

function isIndependent(value: unknown): boolean {
	return typeof value === "object" && value !== null && "occurrenceId" in value;
}

export function parseDeliveryReference(value: unknown): DeliveryReference {
	return isIndependent(value)
		? parsePendingOccurrenceReference(value)
		: parseOutgoingReference(value);
}

export function parseDeliveryEnvelope(value: unknown): DeliveryEnvelope {
	return isIndependent(value)
		? parsePendingOccurrenceEnvelope(value)
		: parseOutgoingEnvelope(value);
}
