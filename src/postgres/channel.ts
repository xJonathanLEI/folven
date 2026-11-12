import { createHash } from "node:crypto";
import { z } from "zod";
import { validateName } from "../ownership.js";
import { parseWireInput } from "./encoding.js";
import { ChannelEnvelopeError } from "./errors.js";
import { canonicalStreamKey, validateIdentityText } from "./identity.js";
import type {
	OutgoingEnvelope,
	OutgoingReference,
	StreamIdentity,
} from "./types.js";

const maximumChannelSequence = 9_223_372_036_854_775_807n;
const identitySchema = z.strictObject({
	streamName: z.string(),
	streamKey: z.string(),
});
const referenceSchema = z.strictObject({
	source: identitySchema,
	target: identitySchema,
	channelSequence: z.string(),
});
const envelopeSchema = referenceSchema.extend({
	eventName: z.string(),
	eventVersion: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER),
	payloadFormat: z.literal("json-v1"),
	encodedInput: z.string(),
});

export function parseOutgoingReference(value: unknown): OutgoingReference {
	const parsed = referenceSchema.safeParse(value);
	if (!parsed.success) {
		throw new ChannelEnvelopeError("Invalid outgoing reference structure.");
	}
	const reference = parsed.data;
	const source = parseStreamIdentity(reference.source);
	const target = parseStreamIdentity(reference.target);
	parseChannelSequence(reference.channelSequence);
	return Object.freeze({ ...reference, source, target });
}

export function parseChannelSequence(value: unknown): bigint {
	if (
		typeof value !== "string" ||
		value.trim() !== value ||
		!/^[1-9][0-9]{0,18}$/.test(value) ||
		BigInt(value) > maximumChannelSequence
	) {
		throw new ChannelEnvelopeError(
			"Channel sequence must be a canonical positive PostgreSQL bigint decimal string.",
		);
	}
	return BigInt(value);
}

/** Validate identities without requiring the source's definitions locally. */
export function parseStreamIdentity(value: unknown): StreamIdentity {
	const parsed = identitySchema.safeParse(value);
	if (!parsed.success) {
		throw new ChannelEnvelopeError(
			"A channel requires exact stream identities.",
		);
	}
	const { streamName, streamKey } = parsed.data;
	validateName(streamName, "Stream name");
	let key: unknown;
	try {
		key = JSON.parse(streamKey);
	} catch {
		throw new ChannelEnvelopeError("Stream key must be canonical JSON.");
	}
	if (typeof key !== "object" || key === null || Array.isArray(key)) {
		throw new ChannelEnvelopeError(
			"Stream key must be a canonical JSON object.",
		);
	}
	const components: Record<string, unknown> = Object.fromEntries(
		Object.entries(key),
	);
	for (const name of Object.keys(components)) {
		validateName(name, "Stream key component");
	}
	if (canonicalStreamKey(streamName, components) !== streamKey) {
		throw new ChannelEnvelopeError(
			"Stream key must use its canonical encoding.",
		);
	}
	return Object.freeze({ streamName, streamKey });
}

/** Structural/wire boundary only; event lookup, schema parsing and routing follow. */
export function parseOutgoingEnvelope(value: unknown): OutgoingEnvelope {
	const parsed = envelopeSchema.safeParse(value);
	if (!parsed.success) {
		throw new ChannelEnvelopeError("Invalid outgoing envelope structure.");
	}
	const envelope = parsed.data;
	const source = parseStreamIdentity(envelope.source);
	const target = parseStreamIdentity(envelope.target);
	parseChannelSequence(envelope.channelSequence);
	validateName(envelope.eventName, "Event name");
	validateIdentityText(envelope.eventName);
	parseWireInput(envelope.encodedInput);
	return Object.freeze({ ...envelope, source, target });
}

/**
 * Frozen v1 identity: SHA-256(UTF8("folven/channel/v1\0") || UTF8(JSON.stringify(
 * [source name, source canonical key, target name, target canonical key, sequence]
 * ))). First 16 bytes, with UUIDv8 version and RFC 9562 variant bits.
 * Event identity, payload and source occurrence are deliberately absent.
 */
export function channelOccurrenceId(channel: OutgoingReference): string {
	const source = parseStreamIdentity(channel.source);
	const target = parseStreamIdentity(channel.target);
	parseChannelSequence(channel.channelSequence);
	const bytes = createHash("sha256")
		.update("folven/channel/v1\0", "utf8")
		.update(
			JSON.stringify([
				source.streamName,
				source.streamKey,
				target.streamName,
				target.streamKey,
				channel.channelSequence,
			]),
			"utf8",
		)
		.digest()
		.subarray(0, 16);
	bytes.writeUInt8((bytes.readUInt8(6) & 0x0f) | 0x80, 6);
	bytes.writeUInt8((bytes.readUInt8(8) & 0x3f) | 0x80, 8);
	const hex = bytes.toString("hex");
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
