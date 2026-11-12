import { expect, it } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { DefinitionError } from "../errors.js";
import { createDefinitions, defineEvent } from "../index.js";
import { createOutputCollector } from "./collector.js";
import { EmissionClosedError, EmissionFailedError } from "./errors.js";
import { createHandlerLookup } from "./registry.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "comments",
	key: z.strictObject({ postId: z.string() }),
	owns: { comment: { kind: "partition", fields: { postId: "postId" } } },
});
const event = defineEvent({
	name: "comment.add",
	version: 1,
	stream,
	schema: z.strictObject({
		postId: z.string(),
		body: z.string().transform((value) => ({ text: value.trim() })),
	}),
	route: (payload) => ({ postId: payload.postId }),
});
const events = createHandlerLookup(
	definitions.registry({ streams: [stream], events: [event], handlers: [] }),
);

it("retains only frozen wire metadata, with no caller-owned or transformed payload references", () => {
	const collector = createOutputCollector(events);
	const input = { postId: "p", body: " Original " };
	collector.emit(event, input);
	input.postId = "changed";
	input.body = "Changed";
	const outputs = collector.finish();
	expect(outputs).toEqual([
		{
			target: { streamName: "comments", streamKey: '{"postId":"p"}' },
			eventName: "comment.add",
			eventVersion: 1,
			payloadFormat: "json-v1",
			encodedInput: '{"postId":"p","body":" Original "}',
		},
	]);
	expect(Object.isFrozen(outputs)).toBe(true);
	expect(Object.isFrozen(outputs[0])).toBe(true);
	expect(Object.isFrozen(outputs[0]?.target)).toBe(true);
	expect(() => collector.emit(event, input)).toThrow(EmissionClosedError);
});

it("poisons a failed attempt and retires its emitter without affecting a new collector", () => {
	const input = { postId: "p", body: "Comment" };
	const collector = createOutputCollector(events);
	collector.emit(event, input);
	expect(() => collector.emit({ ...event }, input)).toThrow(DefinitionError);
	expect(() => collector.emit(event, input)).toThrow(EmissionFailedError);
	expect(() => collector.finish()).toThrow(EmissionFailedError);
	const next = createOutputCollector(events);
	expect(() => collector.emit(event, input)).toThrow(EmissionClosedError);
	next.emit(event, input);
	expect(next.finish()).toHaveLength(1);
	const rejected = createOutputCollector(events);
	rejected.close();
	expect(() => rejected.emit(event, input)).toThrow(EmissionClosedError);
});
