import { Buffer } from "node:buffer";
import { PostgresIdentityError } from "./errors.js";

export const streamIdentityByteLimit = 1_024;

/** PostgreSQL text cannot contain NUL; refuse implicit Unicode replacement. */
export function validateIdentityText(value: string): void {
	if (value.includes("\0") || !value.isWellFormed()) {
		throw new PostgresIdentityError(
			"PostgreSQL identity text must be well-formed Unicode without NUL.",
		);
	}
}

export function canonicalStreamKey(
	streamName: string,
	key: Readonly<Record<string, unknown>>,
): string {
	validateIdentityText(streamName);
	const components = Object.keys(key)
		.sort()
		.map((name) => {
			validateIdentityText(name);
			const value = key[name];
			if (typeof value === "string" && value.length > 0) {
				validateIdentityText(value);
			} else if (
				typeof value !== "number" ||
				!Number.isSafeInteger(value) ||
				Object.is(value, -0)
			) {
				throw new PostgresIdentityError(
					"PostgreSQL stream keys require nonempty strings or safe integers.",
				);
			}
			return `${JSON.stringify(name)}:${JSON.stringify(value)}`;
		});
	// Construct in sorted order explicitly, including integer-like field names.
	const encoded = `{${components.join(",")}}`;
	if (
		Buffer.byteLength(streamName, "utf8") + Buffer.byteLength(encoded, "utf8") >
		streamIdentityByteLimit
	) {
		throw new PostgresIdentityError(
			"PostgreSQL stream name and canonical key exceed the 1,024-byte identity limit.",
		);
	}
	return encoded;
}
