import { expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import { PayloadEncodingError, PostgresIdentityError } from "./errors.js";
import {
	bindOccurrenceAttempt,
	prepareOccurrence,
	reconstructPayload,
} from "./occurrence.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

it("prepares fixed input bytes once and binds fresh attempts without rerouting or double transformation", () => {
	const parseKey = vi.fn((id: string) => id.trim());
	const stream = definitions.stream({
		name: "author",
		key: z.strictObject({ authorId: z.string().transform(parseKey) }),
		owns: { post: { kind: "partition", fields: { authorId: "authorId" } } },
	});
	const parseTimestamp = vi.fn((timestamp: string) => new Date(timestamp));
	const schema = z.object({
		authorId: z.string(),
		timestamp: z.iso.datetime().transform(parseTimestamp),
		tags: z.array(z.string()),
	});
	const route = vi.fn((event: z.output<typeof schema>) => ({
		authorId: event.authorId,
	}));
	const event = defineEvent({
		name: "post.timestamp",
		version: 1,
		stream,
		schema,
		route,
	});
	const handle = vi.fn((payload: z.output<typeof schema>) => payload.timestamp);
	const handler = defineHandler({
		event,
		handle: ({ event: payload }) => handle(payload),
	});
	const input = {
		authorId: " u1 ",
		timestamp: "2026-01-02T03:04:05.000Z",
		tags: ["original"],
		discardedBySchema: "still stored",
	};
	const { occurrence, firstPayload } = prepareOccurrence(handler, input);
	const fixedBytes = occurrence.encodedInput;
	expect(JSON.parse(fixedBytes)).toEqual(input);
	expect(firstPayload).not.toHaveProperty("discardedBySchema");
	expect(firstPayload.timestamp).toEqual(new Date(input.timestamp));
	expect(firstPayload).not.toBe(input);
	expect(firstPayload.tags).not.toBe(input.tags);
	expect(parseTimestamp).toHaveBeenCalledTimes(1);
	expect(occurrence.key).toEqual({ authorId: "u1" });
	expect(occurrence.streamKey).toBe('{"authorId":"u1"}');
	expect(occurrence.payloadFormat).toBe("json-v1");
	expect(occurrence.eventName).toBe(event.name);
	expect(occurrence.eventVersion).toBe(event.version);
	expect(occurrence.streamName).toBe(stream.name);
	expect(Object.isFrozen(occurrence)).toBe(true);
	expect(Object.isFrozen(occurrence.key)).toBe(true);
	expect(handle).not.toHaveBeenCalled();

	// Sentinels only: no database operations are invoked by these preparation
	// helpers. Compiler contracts use the real generated delegate signatures.
	const firstClient = {} as Prisma.TransactionClient;
	const retryClient = {} as Prisma.TransactionClient;
	bindOccurrenceAttempt(occurrence, firstClient, firstPayload, (context) => {
		expect(context.db).toBe(firstClient);
		expect(context.event).toBe(firstPayload);
		expect(context.scope.post.where).toEqual({ authorId: "u1" });
		context.event.timestamp.setTime(0);
		context.event.tags.push("failed attempt");
	});
	expect(input.tags).toEqual(["original"]);
	input.tags.push("caller mutation");
	input.authorId = "another author";
	const retryPayload = reconstructPayload(occurrence);
	bindOccurrenceAttempt(occurrence, retryClient, retryPayload, (context) => {
		expect(context.db).toBe(retryClient);
		expect(context.event.timestamp.toISOString()).toBe(
			"2026-01-02T03:04:05.000Z",
		);
		expect(context.event.tags).toEqual(["original"]);
		expect(context.scope.post.where).toEqual({ authorId: "u1" });
	});
	expect(occurrence.encodedInput).toBe(fixedBytes);
	expect(parseTimestamp).toHaveBeenCalledTimes(2);
	expect(route).toHaveBeenCalledTimes(1);
	expect(parseKey).toHaveBeenCalledTimes(1);
	expect(
		prepareOccurrence(handler, JSON.parse(fixedBytes)).occurrence.occurrenceId,
	).not.toBe(occurrence.occurrenceId);
	expect(occurrence.occurrenceId).toMatch(
		/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
	);
});

it("rejects invalid wire data before parsing/routing and invalid identities before binding", () => {
	const parse = vi.fn((value: string) => value);
	const route = vi.fn((payload: { id: string }) => ({ id: payload.id }));
	const stream = definitions.stream({
		name: "author",
		key: z.strictObject({ id: z.string() }),
		owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
	});
	const event = defineEvent({
		name: "author.read",
		version: 1,
		stream,
		schema: z.strictObject({ id: z.string().transform(parse) }),
		route,
	});
	const handler = defineHandler({ event, handle: vi.fn(() => null) });
	expect(() => prepareOccurrence(handler, { id: undefined })).toThrow(
		PayloadEncodingError,
	);
	expect(parse).not.toHaveBeenCalled();
	expect(route).not.toHaveBeenCalled();
	expect(() => prepareOccurrence(handler, { id: "x".repeat(1_024) })).toThrow(
		PostgresIdentityError,
	);
	expect(() => prepareOccurrence(handler, { id: "\ud800" })).toThrow(
		PostgresIdentityError,
	);
	expect(handler.handle).not.toHaveBeenCalled();
});
