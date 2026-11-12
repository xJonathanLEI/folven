import { createHash } from "node:crypto";
import { z } from "zod";
import { validateName } from "../ownership.js";
import { parseStreamIdentity } from "./channel.js";
import { parseWireInput } from "./encoding.js";
import { EffectProtocolError } from "./errors.js";
import { validateIdentityText } from "./identity.js";
import type { EffectReference, OutstandingEffect } from "./types.js";

const referenceSchema = z.strictObject({
	source: z.unknown(),
	effectSequence: z.string(),
});
const envelopeSchema = z.strictObject({
	reference: z.unknown(),
	effectName: z.string(),
	effectVersion: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER),
	payloadFormat: z.literal("json-v1"),
	encodedInput: z.string(),
});

export function parseEffectSequence(value: unknown): bigint {
	if (
		typeof value !== "string" ||
		value.trim() !== value ||
		!/^[1-9][0-9]{0,18}$/.test(value) ||
		BigInt(value) > 9_223_372_036_854_775_807n
	) {
		throw new EffectProtocolError(
			"Effect sequence must be a canonical positive PostgreSQL bigint decimal string.",
		);
	}
	return BigInt(value);
}

export function parseEffectReference(value: unknown): EffectReference {
	const parsed = referenceSchema.safeParse(value);
	if (!parsed.success) {
		throw new EffectProtocolError("Invalid effect reference structure.");
	}
	const source = parseStreamIdentity(parsed.data.source);
	parseEffectSequence(parsed.data.effectSequence);
	return Object.freeze({ source, effectSequence: parsed.data.effectSequence });
}

/** Wire validation only, independent of locally installed implementations. */
export function parseOutstandingEffect(value: unknown): OutstandingEffect {
	const parsed = envelopeSchema.safeParse(value);
	if (!parsed.success) {
		throw new EffectProtocolError("Invalid outstanding effect structure.");
	}
	const reference = parseEffectReference(parsed.data.reference);
	validateName(parsed.data.effectName, "Effect name");
	validateIdentityText(parsed.data.effectName);
	parseWireInput(parsed.data.encodedInput);
	return Object.freeze({ ...parsed.data, reference });
}

/**
 * Frozen v1 identity: SHA-256(UTF8("folven/effect/v1\0") || UTF8(JSON.stringify(
 * [source name, canonical source key, decimal effect sequence]))), first 16
 * bytes with UUIDv8 version and RFC 9562 variant bits. Definition identity and
 * payload are deliberately absent; database comparisons use full coordinates.
 */
export function effectId(reference: EffectReference): string {
	const { source, effectSequence } = parseEffectReference(reference);
	const bytes = createHash("sha256")
		.update("folven/effect/v1\0", "utf8")
		.update(
			JSON.stringify([source.streamName, source.streamKey, effectSequence]),
			"utf8",
		)
		.digest()
		.subarray(0, 16);
	bytes.writeUInt8((bytes.readUInt8(6) & 0x0f) | 0x80, 6);
	bytes.writeUInt8((bytes.readUInt8(8) & 0x3f) | 0x80, 8);
	const hex = bytes.toString("hex");
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
