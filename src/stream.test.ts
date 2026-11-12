import { expect, it } from "vitest";
import { z } from "zod";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { createDefinitions, parseStreamKey } from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

it("uses parsed key values and rejects unsupported parsed components without leaking values", () => {
	const stream = definitions.stream({
		name: "comment",
		key: z.strictObject({
			postId: z.string().trim(),
			number: z.string().transform(Number),
		}),
		owns: {
			comment: {
				kind: "unique",
				selector: "postId_number",
				fields: { postId: "postId", number: "number" },
			},
		},
	});
	expect(parseStreamKey(stream, { postId: " post-1 ", number: "2" })).toEqual({
		postId: "post-1",
		number: 2,
	});
	for (const number of [
		"1.5",
		"9007199254740992",
		"-0",
		"Infinity",
		"not-a-number",
	]) {
		expect(() => parseStreamKey(stream, { postId: "post-1", number })).toThrow(
			'Stream "comment" keys must contain only nonempty strings or safe integers',
		);
	}
	expect(() => parseStreamKey(stream, { postId: " ", number: "1" })).toThrow(
		"nonempty strings",
	);
});

it("rejects unsupported values even when a caller bypasses the declaration's static key check", () => {
	const stream = { name: "invalid", key: z.object({ id: z.unknown() }) };
	for (const id of [
		null,
		undefined,
		[],
		{},
		true,
		"",
		1.5,
		-0,
		Infinity,
		NaN,
	]) {
		expect(() => parseStreamKey(stream, { id })).toThrow(
			"nonempty strings or safe integers",
		);
	}
});

it("rejects open-ended object schemas whose key dimensions cannot be enumerated", () => {
	expect(() =>
		definitions.stream({
			name: "open",
			key: z.looseObject({ id: z.string() }),
			owns: { post: { kind: "partition", fields: { id: "id" } } },
		}),
	).toThrow('Stream "open" requires a fixed object key schema');
});
