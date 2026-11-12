import type { TaskCodec } from "@xjonathanlei/bellows";
import { z } from "zod";
import { parseEffectReference } from "../postgres/effect-protocol.js";
import { parseDeliveryReference } from "../postgres/protocol.js";
import type { DeliveryReference, EffectReference } from "../postgres/types.js";
import type { EffectFailureDecision } from "./effect-types.js";

const configuration = z.strictObject({
	schema: z
		.string()
		.regex(/^(?!.*[\r\n])[a-z_][a-z0-9_]*$/)
		.max(63),
	taskName: z.string().regex(/^(?!.*[\r\n])[a-zA-Z][a-zA-Z0-9_.:-]{0,127}$/),
	retryDelayMs: z.number().int().min(1).max(86_400_000),
});

export function parseConfiguration(value: unknown) {
	const parsed = configuration.safeParse(value);
	if (!parsed.success)
		throw new TypeError("Invalid Bellows delivery configuration.");
	return parsed.data;
}

export function parseEffectConfiguration(value: unknown) {
	const parsed = configuration.omit({ retryDelayMs: true }).safeParse(value);
	if (!parsed.success)
		throw new TypeError("Invalid Bellows effect configuration.");
	return parsed.data;
}

const failureDecision = z.discriminatedUnion("action", [
	z.strictObject({ action: z.literal("manual") }),
	z.strictObject({
		action: z.literal("retryAt"),
		availableFromMs: z.number().int().min(0).max(8_640_000_000_000_000),
	}),
]);

export function parseFailureDecision(
	value: unknown,
	now: () => number,
): EffectFailureDecision {
	const decision = failureDecision.parse(value);
	if (decision.action === "retryAt") {
		const instant = now();
		if (
			!Number.isSafeInteger(instant) ||
			instant < 0 ||
			decision.availableFromMs <= instant
		)
			throw new TypeError("Invalid Bellows effect retry deadline.");
	}
	return decision;
}

export const effectReferenceCodec: TaskCodec<EffectReference> = Object.freeze({
	encode(value: EffectReference): string {
		try {
			return JSON.stringify(parseEffectReference(value));
		} catch {
			throw new Error("Invalid Folven effect task reference.");
		}
	},
	decode(value: string): EffectReference {
		try {
			const parsed: unknown = JSON.parse(value);
			return parseEffectReference(parsed);
		} catch {
			throw new Error("Invalid Folven effect task reference.");
		}
	},
});

/** Both directions validate; tasks contain references only, never event bodies. */
export const referenceCodec: TaskCodec<DeliveryReference> = Object.freeze({
	encode(value: DeliveryReference): string {
		try {
			return JSON.stringify(parseDeliveryReference(value));
		} catch {
			throw new Error("Invalid Folven delivery task reference.");
		}
	},
	decode(value: string): DeliveryReference {
		try {
			const parsed: unknown = JSON.parse(value);
			return parseDeliveryReference(parsed);
		} catch {
			throw new Error("Invalid Folven delivery task reference.");
		}
	},
});

export function retryDeadline(now: () => number, delay: number): number {
	let instant: number;
	try {
		instant = now();
	} catch {
		throw new Error("Invalid Bellows delivery clock.");
	}
	if (
		!Number.isSafeInteger(instant) ||
		instant < 0 ||
		instant > 8_640_000_000_000_000 - delay
	) {
		throw new Error("Invalid Bellows delivery clock.");
	}
	return instant + delay;
}
