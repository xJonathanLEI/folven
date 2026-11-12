import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import {
	createDefinitions,
	defineEvent,
	DefinitionError,
	parseStreamKey,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

function authorStream(name = "author") {
	return definitions.stream({
		name,
		key: z.strictObject({ authorId: z.string() }),
		owns: {
			user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
			post: { kind: "partition", fields: { authorId: "authorId" } },
		},
	});
}

describe("definition registry", () => {
	it("reuses a canonical stream across events, retains schema identity and separates versions", () => {
		const stream = authorStream();
		const schema = z.strictObject({ title: z.string() });
		const route = vi.fn(() => ({ authorId: "u1" }));
		const renamed = defineEvent({
			name: "post.rename",
			version: 1,
			schema,
			stream,
			route,
		});
		const renamedV2 = defineEvent({
			name: "post.rename",
			version: 2,
			schema,
			stream,
			route,
		});
		const removed = defineEvent({
			name: "post.remove",
			version: 1,
			schema: z.string(),
			stream,
			route,
		});
		const registry = definitions.registry({
			streams: [stream, stream],
			events: [renamed, renamedV2, removed],
		});
		expect(registry.streams[0]).toBe(stream);
		expect(registry.events).toEqual([renamed, renamedV2, removed]);
		expect(registry.events[0].schema).toBe(schema);
		expect(route).not.toHaveBeenCalled();
	});

	it("requires canonical event streams even with no handlers and routes without a handler or client", () => {
		const stream = authorStream();
		const parse = vi.fn((authorId: string) => authorId.trim());
		const route = vi.fn((authorId: string) => ({ authorId }));
		const event = defineEvent({
			name: "author.read",
			version: 1,
			stream,
			schema: z.string().transform(parse),
			route,
		});
		const absent: (typeof stream)[] = [];
		for (const streams of [absent, [authorStream()]]) {
			expect(() => definitions.registry({ streams, events: [event] })).toThrow(
				'Event "author.read" version 1 refers to unregistered stream "author"',
			);
		}
		definitions.registry({ streams: [stream], events: [event] });
		expect(parse).not.toHaveBeenCalled();
		expect(route).not.toHaveBeenCalled();
		expect(
			parseStreamKey(event.stream, event.route(event.schema.parse(" u1 "))),
		).toEqual({ authorId: "u1" });
		expect(parse).toHaveBeenCalledOnce();
		expect(route).toHaveBeenCalledExactlyOnceWith("u1");
	});

	it("rejects competing families and independently declared names, including alternate unique owners", () => {
		const stream = authorStream();
		const byEmail = definitions.stream({
			name: "email",
			key: z.object({ email: z.string() }),
			owns: {
				user: { kind: "unique", selector: "email", fields: { email: "email" } },
			},
		});
		expect(() =>
			definitions.registry({ streams: [stream, byEmail], events: [] }),
		).toThrow(
			'Model "user" is owned by both stream "author" and stream "email"',
		);
		expect(() =>
			definitions.registry({ streams: [stream, authorStream()], events: [] }),
		).toThrow('Stream "author" has multiple definitions');
		// Registries are local; neither construction creates a process-global owner.
		expect(() =>
			definitions.registry({ streams: [byEmail], events: [] }),
		).not.toThrow();
	});

	it("rejects incomplete mappings for each table and whole-table ownership with varying keys", () => {
		const incomplete = definitions.stream({
			name: "post",
			key: z.object({ authorId: z.string(), slug: z.string() }),
			owns: {
				post: {
					kind: "unique",
					selector: "authorId_slug",
					fields: { authorId: "authorId", slug: "slug" },
				},
				user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
			},
		});
		expect(() =>
			definitions.registry({ streams: [incomplete], events: [] }),
		).toThrow(
			'Stream "post", model "user", selector "id" omits stream-key component "slug"',
		);
		const whole = definitions.stream({
			name: "varying",
			key: z.object({ id: z.string() }),
			owns: { user: { kind: "table" } },
		});
		expect(() =>
			definitions.registry({ streams: [whole], events: [] }),
		).toThrow("whole-table ownership with a varying stream key");
	});

	it("accepts singleton, composite partition and compound primary-key declarations", () => {
		const users = definitions.stream({
			name: "users",
			key: z.strictObject({}),
			owns: { user: { kind: "table" } },
		});
		const posts = definitions.stream({
			name: "posts",
			key: z.object({ author: z.string(), slug: z.string() }),
			owns: {
				post: {
					kind: "partition",
					fields: { authorId: "author", slug: "slug" },
				},
			},
		});
		const comments = definitions.stream({
			name: "comments",
			key: z.object({ post: z.string(), number: z.number() }),
			owns: {
				comment: {
					kind: "unique",
					selector: "postId_number",
					fields: { postId: "post", number: "number" },
				},
			},
		});
		expect(
			definitions.registry({ streams: [users, posts, comments], events: [] })
				.streams,
		).toHaveLength(3);
		expect(parseStreamKey(users, {})).toEqual({});
		expect(parseStreamKey(comments, { post: "post-1", number: 1 })).toEqual({
			post: "post-1",
			number: 1,
		});
	});

	it("rejects duplicate event identities even when descriptors or schemas are shared", () => {
		const stream = authorStream();
		const route = () => ({ authorId: "u1" });
		const event = defineEvent({
			name: "post.rename",
			version: 1,
			schema: z.string(),
			stream,
			route,
		});
		const comments = definitions.stream({
			name: "comments",
			key: z.strictObject({}),
			owns: { comment: { kind: "table" } },
		});
		for (const other of [
			event,
			defineEvent({
				name: "post.rename",
				version: 1,
				schema: z.number(),
				stream,
				route,
			}),
			defineEvent({
				name: "post.rename",
				version: 1,
				schema: event.schema,
				stream,
				route: () => ({ authorId: "u2" }),
			}),
			defineEvent({
				name: "post.rename",
				version: 1,
				schema: event.schema,
				stream: comments,
				route: () => ({}),
			}),
		]) {
			expect(() =>
				definitions.registry({
					streams: [stream, comments],
					events: [event, other],
				}),
			).toThrow('Event "post.rename" version 1 is registered more than once');
		}
	});

	it("detaches and freezes declaration metadata and registry lists, not schema internals", () => {
		const fields = { authorId: "authorId" } as const;
		const owns = { post: { kind: "partition", fields } } as const;
		const stream = definitions.stream({
			name: "author",
			key: z.object({ authorId: z.string() }),
			owns,
		});
		const eventInput = {
			name: "post.rename",
			version: 1,
			schema: z.string(),
			stream,
			route: (authorId: string) => ({ authorId }),
		} as const;
		const event = defineEvent(eventInput);
		const streams = [stream];
		const events = [event];
		const registry = definitions.registry({ streams, events });
		streams.length = 0;
		events.length = 0;
		expect(registry.streams).toEqual([stream]);
		expect(registry.events).toEqual([event]);
		expect(stream.owns).not.toBe(owns);
		expect(stream.owns.post.fields).not.toBe(fields);
		for (const value of [
			registry,
			registry.streams,
			registry.events,
			stream,
			stream.owns,
			stream.owns.post,
			stream.owns.post.fields,
			event,
		]) {
			expect(Object.isFrozen(value)).toBe(true);
		}
		expect(event.stream).toBe(stream);
		expect(event.route).toBe(eventInput.route);
		for (const value of [
			owns,
			fields,
			eventInput,
			event.schema,
			event.route,
			stream.key,
		]) {
			expect(Object.isFrozen(value)).toBe(false);
		}
	});

	it("reports malformed declaration metadata without inspecting payloads", () => {
		const stream = authorStream();
		const route = () => ({ authorId: "u1" });
		expect(() => authorStream(" ")).toThrow(DefinitionError);
		expect(() =>
			defineEvent({
				name: "bad.route",
				version: 1,
				stream,
				schema: z.string(),
				// @ts-expect-error JavaScript callers must provide callable routing too.
				route: undefined,
			}),
		).toThrow("must declare a callable route");
		expect(() =>
			defineEvent({
				name: "bad.stream",
				version: 1,
				stream: { ...stream, name: " " },
				schema: z.string(),
				route,
			}),
		).toThrow(DefinitionError);
		expect(() =>
			defineEvent({ name: "", version: 1, schema: z.string(), stream, route }),
		).toThrow(DefinitionError);
		expect(() =>
			defineEvent({
				name: "post.rename",
				version: 9007199254740992,
				schema: z.string(),
				stream,
				route,
			}),
		).toThrow("positive safe-integer version");
		expect(() =>
			definitions.stream({ name: "empty", key: z.object({}), owns: {} }),
		).toThrow("at least one owned model");
		expect(() =>
			definitions.stream({
				name: "empty-mapping",
				key: z.object({}),
				owns: { post: { kind: "partition", fields: {} } },
			}),
		).toThrow("must map at least one scalar field");
	});
});
