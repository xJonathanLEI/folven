import { expect, it } from "vitest";
import { PostgresIdentityError } from "./errors.js";
import { canonicalStreamKey, validateIdentityText } from "./identity.js";

it("canonicalizes component order without losing types or special property names", () => {
	expect(canonicalStreamKey("comment", { post: "p1", number: 1 })).toBe(
		canonicalStreamKey("comment", { number: 1, post: "p1" }),
	);
	expect(canonicalStreamKey("author", { id: 1 })).not.toBe(
		canonicalStreamKey("author", { id: "1" }),
	);
	expect(canonicalStreamKey("singleton", {})).toBe("{}");
	expect(
		canonicalStreamKey("ordered", {
			"2": "two",
			"10": "ten",
			ä: "umlaut",
			z: "last",
			["__proto__"]: "data",
		}),
	).toBe('{"10":"ten","2":"two","__proto__":"data","z":"last","ä":"umlaut"}');
});

it("enforces the combined UTF-8 bound, including escaped key bytes", () => {
	const key = { id: "😀".repeat(250) };
	expect(canonicalStreamKey("s".repeat(15), key)).toHaveLength(509);
	expect(() => canonicalStreamKey("s".repeat(16), key)).toThrow(
		PostgresIdentityError,
	);
	expect(() => canonicalStreamKey("s".repeat(1_023), {})).toThrow(
		PostgresIdentityError,
	);
	expect(() => canonicalStreamKey("s", { id: "\n".repeat(508) })).toThrow(
		PostgresIdentityError,
	);
});

it.each(["nul\0text", "\ud800", "\udfff", "x\ud800y"])(
	"rejects PostgreSQL-incompatible identity text in every identity component (%#)",
	(text) => {
		expect(() => validateIdentityText(text)).toThrow(PostgresIdentityError);
		expect(() => canonicalStreamKey(text, {})).toThrow(PostgresIdentityError);
		expect(() => canonicalStreamKey("s", { [text]: "ok" })).toThrow(
			PostgresIdentityError,
		);
		expect(() => canonicalStreamKey("s", { id: text })).toThrow(
			PostgresIdentityError,
		);
	},
);
