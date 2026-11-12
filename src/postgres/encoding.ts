import { isProxy } from "node:util/types";
import type { EventPayload, EventRegistration } from "../event.js";
import { parseHandlerEvent } from "../trusted-binding.js";
import { PayloadEncodingError } from "./errors.js";

export const payloadFormat = "json-v1";

type JsonValue =
	| null
	| boolean
	| string
	| number
	| JsonValue[]
	| { [key: string]: JsonValue };

function unsupported(): never {
	throw new PayloadEncodingError(
		"json-v1 requires plain JSON values without lossy properties, accessors or cycles.",
	);
}

/**
 * Inspect descriptors, not property values, and serialize a detached copy.
 * JSON.stringify must never run caller getters or custom serialization.
 */
function copyJson(value: unknown, ancestors: Set<object>): JsonValue {
	if (
		value === null ||
		typeof value === "string" ||
		typeof value === "boolean"
	) {
		return value;
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value) || Object.is(value, -0)) unsupported();
		return value;
	}
	if (typeof value !== "object") return unsupported();
	if (isProxy(value)) return unsupported();
	if (ancestors.has(value)) return unsupported();

	const array = Array.isArray(value);
	const prototype: unknown = Object.getPrototypeOf(value);
	if (
		array
			? prototype !== Array.prototype
			: prototype !== Object.prototype && prototype !== null
	) {
		return unsupported();
	}
	ancestors.add(value);
	try {
		const descriptors = Object.getOwnPropertyDescriptors(value);
		const names = Reflect.ownKeys(descriptors);
		const entries: [string, JsonValue][] = [];
		for (const name of names) {
			if (typeof name !== "string") return unsupported();
			if (array && name === "length") continue;
			const descriptor = descriptors[name];
			if (
				descriptor === undefined ||
				!Object.hasOwn(descriptor, "value") ||
				(!array && !descriptor.enumerable)
			) {
				return unsupported();
			}
			const child: unknown = descriptor.value;
			entries.push([name, copyJson(child, ancestors)]);
		}
		if (array) {
			// Only dense canonical index properties, no extra array properties.
			if (
				entries.length !== value.length ||
				entries.some(([name], index) => name !== String(index))
			) {
				return unsupported();
			}
			return entries.map(([, child]) => child);
		}
		// A null prototype also makes "__proto__" an ordinary data property.
		const result = Object.create(null) as { [key: string]: JsonValue };
		for (const [name, child] of entries) result[name] = child;
		return result;
	} finally {
		ancestors.delete(value);
	}
}

export function encodeInput(input: unknown): string {
	return JSON.stringify(copyJson(input, new Set()));
}

/** Accept only exact bytes the json-v1 encoder could have produced. */
export function parseWireInput(encodedInput: string): unknown {
	let input: unknown;
	try {
		input = JSON.parse(encodedInput);
	} catch {
		throw new PayloadEncodingError("json-v1 input must be valid encoded JSON.");
	}
	if (encodeInput(input) !== encodedInput) {
		throw new PayloadEncodingError(
			"json-v1 input must be an exact lossless encoder representation.",
		);
	}
	return input;
}

/** Always parse schema input, never a previous attempt's transformed output. */
export function decodeInput<Event extends EventRegistration>(
	event: Event,
	encodedInput: string,
): EventPayload<Event> {
	const input = parseWireInput(encodedInput);
	return parseHandlerEvent(event, input);
}
