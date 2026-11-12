import { describe, expect, it, vi } from "vitest";
import { encodeInput, parseWireInput } from "./encoding.js";
import { PayloadEncodingError } from "./errors.js";

describe("json-v1", () => {
	it("retains plain JSON data, aliases and null-prototype records without caller mutation", () => {
		const shared = Object.freeze({ text: "😀\0", number: 1.25 });
		const record: unknown = Object.assign(Object.create(null), {
			shared,
		});
		const input = Object.freeze({
			list: Object.freeze([null, true, false, "", 0, shared]),
			shared,
			record,
			["__proto__"]: "ordinary data",
		});
		const encoded = encodeInput(input);
		expect(parseWireInput(encoded)).toEqual(input);
		expect(input.shared).toBe(shared);
	});

	it("accepts only exact encoder bytes and keeps malformed wire contents out of errors", () => {
		for (const encoded of [
			'{"private":"one","private":"two"}',
			'{"private":1e999}',
			'{"private":9007199254740993}',
			'{"private":-0}',
			'{"private":1.0}',
			'{"private":"\\u0061"}',
			'{"private":true} ',
			'{"private":undefined}',
			"private-not-json",
		]) {
			expect(() => parseWireInput(encoded)).toThrow(PayloadEncodingError);
			try {
				parseWireInput(encoded);
			} catch (error) {
				expect(String(error)).not.toContain("private");
			}
		}
		for (const input of [
			null,
			true,
			1.5,
			["text", 1],
			{ z: "é", a: "\ud800" },
		]) {
			const encoded = encodeInput(input);
			expect(encodeInput(parseWireInput(encoded))).toBe(encoded);
		}
	});

	it.each([
		undefined,
		() => undefined,
		Symbol("unsupported"),
		1n,
		NaN,
		Infinity,
		-Infinity,
		-0,
		new Date(0),
		new Map(),
		new Set(),
		new Number(1),
		new (class Value {
			value = 1;
		})(),
		Array<unknown>(3),
		Object.assign([1], { extra: true }),
		Object.defineProperty({}, "hidden", { value: 1 }),
		{ [Symbol("hidden")]: 1 },
	])(
		"rejects unsupported values at the root and within containers (%#)",
		(value) => {
			for (const input of [value, { nested: value }, [value]]) {
				expect(() => encodeInput(input)).toThrow(PayloadEncodingError);
			}
		},
	);

	it("rejects cycles but never calls accessors or toJSON", () => {
		const cyclic: { self?: unknown } = {};
		cyclic.self = cyclic;
		const getter = vi.fn(() => "secret");
		const toJSON = vi.fn(() => ({}));
		const inputs = [
			cyclic,
			Object.defineProperty({}, "value", { get: getter, enumerable: true }),
			Object.defineProperty({}, "toJSON", { get: getter }),
			{ toJSON },
			Object.defineProperty([0], "0", { get: getter, enumerable: true }),
		];
		for (const input of inputs) {
			expect(() => encodeInput(input)).toThrow(PayloadEncodingError);
		}
		expect(getter).not.toHaveBeenCalled();
		expect(toJSON).not.toHaveBeenCalled();
	});

	it("rejects proxies without executing reflection or property traps", () => {
		const trap = vi.fn(() => {
			throw new Error("Caller code must not run");
		});
		const value = new Proxy(
			{},
			{
				get: trap,
				getPrototypeOf: trap,
				getOwnPropertyDescriptor: trap,
				ownKeys: trap,
			},
		);
		expect(() => encodeInput(value)).toThrow(PayloadEncodingError);
		expect(trap).not.toHaveBeenCalled();
	});
});
