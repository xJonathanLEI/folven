import { expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	createDefinitions,
	defineEvent,
	defineHandler,
	DefinitionError,
} from "../index.js";
import { PostgresIdentityError } from "./errors.js";
import { prepareEncodedEvent, prepareEvent } from "./occurrence.js";
import { createHandlerLookup } from "./registry.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});

it("prepares registered outgoing metadata without requiring a target implementation", () => {
	const parse = vi.fn((value: string) => value.length);
	const route = vi.fn((payload: { id: string; body: number }) => ({
		id: payload.id,
	}));
	const event = defineEvent({
		name: "author.notice",
		version: 1,
		stream,
		schema: z.object({ id: z.string(), body: z.string().transform(parse) }),
		route,
	});
	const nextVersion = defineEvent({ ...event, version: 2 });
	const lookup = createHandlerLookup(
		definitions.registry({
			streams: [stream],
			events: [event, nextVersion],
		}),
	);
	expect(lookup.getEvent(event)).toBe(event);
	expect(lookup.resolveEvent(event.name, 1)).toBe(event);
	expect(lookup.resolveEvent(event.name, 2)).toBe(nextVersion);
	expect(() => lookup.getEvent({ ...event })).toThrow(DefinitionError);
	expect(() => lookup.resolveEvent(event.name, 3)).toThrow(DefinitionError);
	expect(() => lookup.requireHandler(event)).toThrow(DefinitionError);
	expect(parse).not.toHaveBeenCalled();
	expect(route).not.toHaveBeenCalled();
	const input = { id: "u1", body: "text", extra: "retained" };
	const { prepared, firstPayload } = prepareEvent(
		lookup.getEvent(event),
		input,
	);
	expect(firstPayload).toEqual({ id: "u1", body: 4 });
	expect(prepared.encodedInput).toBe(JSON.stringify(input));
	expect(prepared.streamKey).toBe('{"id":"u1"}');
	expect(prepared).not.toHaveProperty("handler");
	expect(prepared).not.toHaveProperty("occurrenceId");
	expect(Object.isFrozen(prepared)).toBe(true);
	input.id = "changed";
	input.body = "mutated";
	const reconstructed = prepareEncodedEvent(event, prepared.encodedInput);
	expect(reconstructed.prepared).toEqual(prepared);
	expect(reconstructed.firstPayload).toEqual(firstPayload);
	expect(reconstructed.firstPayload).not.toBe(firstPayload);
});

it("resolves only the exact authoritative descriptor without parsing, routing or invocation", () => {
	const parse = vi.fn((id: string) => id);
	const route = vi.fn((payload: { id: string }) => payload);
	const event = defineEvent({
		name: "author.read",
		version: 1,
		stream,
		schema: z.strictObject({ id: z.string().transform(parse) }),
		route,
	});
	const handler = defineHandler({ event, handle: vi.fn(() => null) });
	const registry = definitions.registry({
		streams: [stream],
		events: [event],
		handlers: [handler],
	});
	const lookup = createHandlerLookup(registry);
	expect(lookup.get(event)).toBe(handler);
	expect(lookup.resolveEvent(event.name, event.version)).toBe(event);
	expect(
		lookup.requireHandler(lookup.resolveEvent(event.name, event.version)),
	).toBe(handler);
	expect(Object.isFrozen(lookup)).toBe(true);
	expect(() => lookup.get({ ...event })).toThrow(DefinitionError);
	expect(parse).not.toHaveBeenCalled();
	expect(route).not.toHaveBeenCalled();
	expect(handler.handle).not.toHaveBeenCalled();

	const malformed = definitions.registry({
		streams: [stream],
		events: [event],
		handlers: [{ ...handler, handle: false }],
	});
	expect(() => createHandlerLookup(malformed)).toThrow(
		/callable implementation/,
	);
});

it("rejects PostgreSQL-incompatible definition metadata without parsing a payload", () => {
	for (const text of ["bad\0name", "bad\ud800name"]) {
		const event = defineEvent({
			name: text,
			version: 1,
			stream,
			schema: z.strictObject({ id: z.string() }),
			route: (payload) => payload,
		});
		const registry = definitions.registry({
			streams: [stream],
			events: [event],
			handlers: [defineHandler({ event, handle: () => null })],
		});
		expect(() => createHandlerLookup(registry)).toThrow(PostgresIdentityError);
	}
});
