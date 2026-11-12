import { describe, expect, it } from "vitest";
import { channelOccurrenceId } from "./channel.js";
import {
	effectId,
	parseEffectReference,
	parseEffectSequence,
	parseOutstandingEffect,
} from "./effect-protocol.js";
import { canonicalStreamKey } from "./identity.js";

const source = { streamName: "author", streamKey: '{"id":"a"}' };
const reference = { source, effectSequence: "1" };
const effect = {
	reference,
	effectName: "post.index",
	effectVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"title":"Draft","views":100}',
} as const;

describe("compact effect wire identity", () => {
	it("freezes compatibility vectors and excludes definition and content from the ID", () => {
		expect(effectId(reference)).toBe("1b827f65-f73a-8be3-8b55-16d43cb77b0b");
		expect(effectId({ source, effectSequence: "9007199254740993" })).toBe(
			"efa123e9-1ac2-888a-8828-744ab3eec002",
		);
		expect(
			effectId({
				source: { streamName: "作者", streamKey: '{"id":"é"}' },
				effectSequence: "9223372036854775807",
			}),
		).toBe("a6eec0b3-8d70-8c88-b61f-93bafcafac45");
		for (const variation of [
			effect,
			{
				...effect,
				effectName: "post.message",
				effectVersion: 99,
				encodedInput: "null",
			},
		])
			expect(effectId(parseOutstandingEffect(variation).reference)).toBe(
				effectId(reference),
			);
		expect(effectId(reference)).not.toBe(
			channelOccurrenceId({
				source,
				target: source,
				channelSequence: "1",
			}),
		);
		expect(effectId({ ...reference, effectSequence: "2" })).not.toBe(
			effectId(reference),
		);
		expect(
			effectId({
				...reference,
				source: { ...source, streamKey: '{"id":"b"}' },
			}),
		).not.toBe(effectId(reference));
	});

	it("retains exact input text and detaches and freezes protocol structures", () => {
		const parsed = parseOutstandingEffect(effect);
		expect(parsed.encodedInput).toBe(effect.encodedInput);
		expect(parsed.reference).not.toBe(reference);
		expect(parsed.reference.source).not.toBe(source);
		expect(
			[parsed, parsed.reference, parsed.reference.source].every(
				Object.isFrozen,
			),
		).toBe(true);
		expect(parseEffectSequence("9007199254740993")).toBe(9007199254740993n);
		expect(parseEffectSequence("9223372036854775807")).toBe(
			9223372036854775807n,
		);
	});

	it("rejects noncanonical coordinates and foreign delivery metadata at every boundary", () => {
		for (const value of [
			"0",
			"-1",
			"01",
			"+1",
			"1.0",
			"1e2",
			" 1",
			"1\n",
			"9223372036854775808",
			1,
			1n,
		]) {
			expect(() =>
				parseEffectReference({ source, effectSequence: value }),
			).toThrow();
		}
		for (const extra of [
			{ channelSequence: "1" },
			{ target: source },
			{ occurrenceId: "x" },
			{ effectId: "x" },
		]) {
			expect(() => parseEffectReference({ ...reference, ...extra })).toThrow();
			expect(() => parseOutstandingEffect({ ...effect, ...extra })).toThrow();
		}
		for (const invalidSource of [
			{ ...source, extra: true },
			{ ...source, streamKey: '{"z":1,"a":2}' },
			{ ...source, streamKey: '{"id":-0}' },
			{ ...source, streamKey: '{"id":""}' },
			{ ...source, streamName: "a\0b" },
			{ ...source, streamKey: '{"id":"\\ud800"}' },
		])
			expect(() =>
				parseEffectReference({ ...reference, source: invalidSource }),
			).toThrow();
		for (const patch of [
			{ effectVersion: 0 },
			{ effectName: "x\0y" },
			{ payloadFormat: "json-v2" },
			{ encodedInput: "{" },
			{ encodedInput: '{"x":9007199254740993}' },
		])
			expect(() => parseOutstandingEffect({ ...effect, ...patch })).toThrow();
	});

	it("shares the canonical UTF-8 identity limit without Unicode normalization", () => {
		const key = { id: "é".repeat(504) };
		const bounded = {
			streamName: "author",
			streamKey: canonicalStreamKey("author", key),
		};
		expect(
			parseEffectReference({ source: bounded, effectSequence: "1" }).source,
		).toEqual(bounded);
		expect(() => canonicalStreamKey("author", { id: `${key.id}é` })).toThrow();
		const canonical = canonicalStreamKey("author", { z: 2, a: "e\u0301" });
		expect(canonical).toBe('{"a":"é","z":2}');
		expect(
			effectId({
				source: { ...source, streamKey: canonical },
				effectSequence: "1",
			}),
		).not.toBe(
			effectId({
				source: { ...source, streamKey: '{"a":"é","z":2}' },
				effectSequence: "1",
			}),
		);
	});
});
