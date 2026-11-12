import { expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { prepareHandler } from "./adapter.js";
import {
	createDefinitions,
	defineEvent,
	defineHandler,
	DefinitionError,
	EmissionUnavailableError,
} from "./index.js";
import type { EventEmitter } from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

function authorStream() {
	return definitions.stream({
		name: "author",
		key: z.strictObject({ authorId: z.string().trim().toLowerCase() }),
		owns: {
			user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
		},
	});
}

function namedEvent(
	stream: ReturnType<typeof authorStream>,
	route = (event: { authorId: string }) => ({ authorId: event.authorId }),
) {
	return defineEvent({
		name: "author.name",
		version: 1,
		stream,
		route,
		schema: z.strictObject({
			authorId: z.string(),
			name: z.string().transform((name) => name.length),
		}),
	});
}

// A client identity sentinel only: these callbacks execute no database methods.
const client = Object.freeze({}) as Prisma.TransactionClient;

it("fails closed without an emitter and lends only the explicitly supplied capability", () => {
	const target = namedEvent(authorStream());
	const source = defineHandler({
		event: target,
		handle({ emit, db }) {
			expect(db).toBe(client);
			emit(target, { authorId: "u1", name: "Original input" });
			return "ordinary result";
		},
	});
	const input = { authorId: "u1", name: "Source" };
	expect(() =>
		prepareHandler(source, client, input, ({ context }) =>
			source.handle(context),
		),
	).toThrow(EmissionUnavailableError);
	const emit = vi.fn<EventEmitter>();
	expect(
		prepareHandler(
			source,
			client,
			input,
			({ context }) => source.handle(context),
			{ emit },
		),
	).toBe("ordinary result");
	expect(emit).toHaveBeenCalledExactlyOnceWith(target, {
		authorId: "u1",
		name: "Original input",
	});
});

it("parses the event and routed key once, lending the original client without invoking the handler", () => {
	const parseNumber = vi.fn((value: string) => Number(value));
	const stream = definitions.stream({
		name: "comment",
		key: z.strictObject({
			postId: z.string().trim().toLowerCase(),
			number: z.string().transform(parseNumber),
		}),
		owns: {
			comment: {
				kind: "unique",
				selector: "postId_number",
				fields: { postId: "postId", number: "number" },
			},
		},
	});
	const parse = vi.fn((name: string) => name.length);
	const schema = z.strictObject({
		authorId: z.string(),
		name: z.string().transform(parse),
	});
	const route = vi.fn((parsed: z.output<typeof schema>) => ({
		postId: parsed.authorId,
		number: String(parsed.name),
	}));
	const event = defineEvent({
		name: "comment.name",
		version: 1,
		stream,
		schema,
		route,
	});
	const called = vi.fn();
	const handler = defineHandler({
		event,
		handle({ event: parsed, db, scope }) {
			called();
			expect(parsed).toEqual({ authorId: " POST-1 ", name: 4 });
			expect(db).toBe(client);
			expect(scope.comment.unique).toEqual({
				postId_number: { postId: "post-1", number: 4 },
			});
			return parsed.name;
		},
	});
	const payload = { authorId: " POST-1 ", name: "Name" };
	const output = prepareHandler(handler, client, payload, (prepared) => {
		expect(called).not.toHaveBeenCalled();
		expect(parse).toHaveBeenCalledOnce();
		expect(parseNumber).toHaveBeenCalledOnce();
		expect(route).toHaveBeenCalledExactlyOnceWith({
			authorId: " POST-1 ",
			name: 4,
		});
		expect(prepared.key).toEqual({ postId: "post-1", number: 4 });
		expect(prepared.context.scope.comment.fields).toEqual(prepared.key);
		expect(Object.isFrozen(prepared)).toBe(true);
		expect(Object.isFrozen(prepared.key)).toBe(true);
		expect(Object.isFrozen(prepared.context)).toBe(true);
		return handler.handle(prepared.context);
	});
	expect(output).toBe(4);
	expect(called).toHaveBeenCalledOnce();
	expect(payload).toEqual({ authorId: " POST-1 ", name: "Name" });
	expect(Object.isFrozen(handler)).toBe(true);
});

it("rejects malformed payloads and invalid routed keys before lending a context", () => {
	const route = vi.fn((event: { authorId: string }) => ({
		authorId: event.authorId,
	}));
	const called = vi.fn();
	const handler = defineHandler({
		event: namedEvent(authorStream(), route),
		handle() {
			called();
		},
	});
	const use = vi.fn<() => void>();
	expect(() =>
		prepareHandler(
			handler,
			client,
			{ authorId: "private-value", name: 1 },
			use,
		),
	).toThrow(z.ZodError);
	expect(route).not.toHaveBeenCalled();
	expect(() =>
		prepareHandler(handler, client, { authorId: " ", name: "Name" }, use),
	).toThrow(DefinitionError);
	expect(route).toHaveBeenCalledOnce();
	expect(use).not.toHaveBeenCalled();
	expect(called).not.toHaveBeenCalled();
});

it("registers one authoritative handler per event and checks descriptor identity, not just names", () => {
	const stream = authorStream();
	const event = namedEvent(stream);
	const otherEvent = defineEvent({
		name: event.name,
		version: 2,
		schema: event.schema,
		stream,
		route: event.route,
	});
	const input = {
		event,
		handle: vi.fn(() => "result"),
	};
	const handler = defineHandler(input);
	const other = defineHandler({ ...input, event: otherEvent });
	const posts = definitions.stream({
		name: "posts",
		key: stream.key,
		owns: { post: { kind: "partition", fields: { authorId: "authorId" } } },
	});
	const postEvent = defineEvent({
		name: "post.read",
		version: 1,
		schema: event.schema,
		stream: posts,
		route: event.route,
	});
	const postHandler = defineHandler({ ...input, event: postEvent });
	const handlers = [handler, other, postHandler];
	const registry = definitions.registry({
		streams: [stream, posts],
		events: [event, otherEvent, postEvent],
		handlers,
	});
	expect(registry.handlers).toEqual(handlers);
	expect(registry.handlers).not.toBe(handlers);
	expect(Object.isFrozen(registry.handlers)).toBe(true);
	expect(Object.isFrozen(handlers)).toBe(false);
	expect(Object.isFrozen(input)).toBe(false);
	expect(input.handle).not.toHaveBeenCalled();
	expect(
		definitions.registry({ streams: [stream], events: [event] }).handlers,
	).toEqual([]);
	for (const duplicate of [handler, defineHandler(input)]) {
		expect(() =>
			definitions.registry({
				streams: [stream],
				events: [event],
				handlers: [handler, duplicate],
			}),
		).toThrow(
			'Handler for stream "author", event "author.name" version 1 is registered more than once',
		);
	}
	expect(() =>
		definitions.registry({
			streams: [stream],
			events: [namedEvent(stream)],
			handlers: [handler],
		}),
	).toThrow('unregistered event "author.name" version 1');
});
