import { expect, test, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { DefinitionError } from "../errors.js";
import { createDefinitions, defineEffect } from "../index.js";
import { createEffectCollector } from "./effect-collector.js";
import {
	EffectRequestClosedError,
	EffectRequestFailedError,
	PayloadEncodingError,
} from "./errors.js";
import { createHandlerLookup } from "./registry.js";

const parse = vi.fn((value: string) => new Date(value));
const effect = defineEffect({
	name: "post.index",
	version: 1,
	schema: z.object({ at: z.string().transform((value) => parse(value)) }),
});
const lookup = createHandlerLookup(
	createDefinitions<Prisma.TransactionClient>().registry({
		streams: [],
		events: [],
		effects: [effect],
	}),
);

test("captures detached exact schema input bytes, not transformed output, and closes its capability", () => {
	parse.mockClear();
	const collector = createEffectCollector(lookup);
	const input = { at: "2026-01-01", extra: "retained" };
	collector.requestEffect(effect, input);
	input.at = "mutated";
	const captured = collector.finish();
	expect(captured).toEqual([
		{
			effectName: "post.index",
			effectVersion: 1,
			payloadFormat: "json-v1",
			encodedInput: '{"at":"2026-01-01","extra":"retained"}',
		},
	]);
	expect(parse).toHaveBeenCalledExactlyOnceWith("2026-01-01");
	expect(Object.isFrozen(captured)).toBe(true);
	expect(Object.isFrozen(captured[0])).toBe(true);
	expect(() => collector.requestEffect(effect, input)).toThrow(
		EffectRequestClosedError,
	);
});

test("canonical membership, encoding and schema failures poison even a previously valid batch", () => {
	for (const { fail, error } of [
		{
			error: DefinitionError,
			fail: (collector: ReturnType<typeof createEffectCollector>) =>
				collector.requestEffect(
					defineEffect({
						name: effect.name,
						version: 1,
						schema: effect.schema,
					}),
					{ at: "valid" },
				),
		},
		{
			error: PayloadEncodingError,
			fail: (collector: ReturnType<typeof createEffectCollector>) => {
				const input = {
					at: "valid",
					toJSON() {
						return {};
					},
				};
				collector.requestEffect(effect, input);
			},
		},
		{
			error: z.ZodError,
			fail: (collector: ReturnType<typeof createEffectCollector>) =>
				// @ts-expect-error Exercise an untyped request with invalid schema input.
				collector.requestEffect(effect, { at: 10 }),
		},
	]) {
		const collector = createEffectCollector(lookup);
		collector.requestEffect(effect, { at: "2026-01-01" });
		expect(() => fail(collector)).toThrow(error);
		expect(() => collector.requestEffect(effect, { at: "2026-01-01" })).toThrow(
			EffectRequestFailedError,
		);
		expect(() => collector.finish()).toThrow(EffectRequestFailedError);
		expect(() => collector.requestEffect(effect, { at: "2026-01-01" })).toThrow(
			EffectRequestClosedError,
		);
	}
});
