import { z } from "zod";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { createDefinitions, defineEvent, parseStreamKey } from "./index.js";
import type {
	EventInput,
	EventPayload,
	Ownership,
	StreamKey,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

export const author = definitions.stream({
	name: "author",
	key: z.strictObject({ authorId: z.string().min(1) }),
	owns: {
		user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
		post: { kind: "partition", fields: { authorId: "authorId" } },
	},
});

const transformedStream = definitions.stream({
	name: "transformed-comment",
	key: z.strictObject({
		postId: z.string(),
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
export const transformedEvent = defineEvent({
	name: "comment.transformed",
	version: 1,
	stream: transformedStream,
	schema: z.strictObject({
		postId: z.string(),
		number: z.string().transform(Number),
	}),
	route: (payload) => ({
		postId: payload.postId,
		number: String(payload.number),
	}),
});
export type TransformedRoute = Expect<
	Equal<
		ReturnType<typeof transformedEvent.route>,
		{ postId: string; number: string }
	>
>;
export type TransformedKey = Expect<
	Equal<
		StreamKey<typeof transformedEvent.stream>,
		{ postId: string; number: number }
	>
>;
defineEvent({
	name: "bad",
	version: 1,
	stream: transformedStream,
	schema: transformedEvent.schema,
	// @ts-expect-error Routing returns key input, not its transformed numeric output.
	route: (payload) => ({ postId: payload.postId, number: payload.number }),
});

export const post = definitions.stream({
	name: "post",
	key: z.strictObject({ authorId: z.string(), slug: z.string() }),
	owns: {
		post: {
			kind: "unique",
			selector: "authorId_slug",
			fields: { authorId: "authorId", slug: "slug" },
		},
	},
});

export const comment = definitions.stream({
	name: "comment",
	key: z.strictObject({ post: z.string(), number: z.number().int() }),
	owns: {
		comment: {
			kind: "unique",
			selector: "postId_number",
			fields: { postId: "post", number: "number" },
		},
	},
});

export const singleton = definitions.stream({
	name: "all-users",
	key: z.strictObject({}),
	owns: { user: { kind: "table" } },
});

const eventSchema = z.strictObject({
	title: z.string(),
	length: z.string().transform((value) => value.length),
});
const event = defineEvent({
	name: "post.rename",
	version: 1,
	stream: author,
	route: (payload) => ({ authorId: payload.title }),
	schema: eventSchema,
});
export const registry = definitions.registry({
	streams: [author],
	events: [event],
});

type Equal<Left, Right> =
	(<Value>() => Value extends Left ? 1 : 2) extends <
		Value,
	>() => Value extends Right ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;

export type EventName = Expect<Equal<typeof event.name, "post.rename">>;
export type EventVersion = Expect<Equal<typeof event.version, 1>>;
export type EventStream = Expect<Equal<typeof event.stream, typeof author>>;
export type EventSchema = Expect<
	Equal<typeof event.schema, typeof eventSchema>
>;
export type Input = Expect<
	Equal<EventInput<typeof event>, { title: string; length: string }>
>;
export type Output = Expect<
	Equal<EventPayload<typeof event>, { title: string; length: number }>
>;
export type Key = Expect<
	Equal<StreamKey<typeof comment>, { post: string; number: number }>
>;
export type Owner = Expect<
	Equal<typeof author.owns.user.fields.id, "authorId">
>;
export type RegisteredEvent = Expect<
	Equal<(typeof registry.events)[0], typeof event>
>;
export type RegisteredStream = Expect<
	Equal<(typeof registry.streams)[0], typeof author>
>;
export const parsed: { post: string; number: number } = parseStreamKey(
	comment,
	{},
);

definitions.stream({
	name: "bad-model",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Unknown models are not Prisma delegates.
	owns: { missing: { kind: "partition", fields: { id: "id" } } },
});
definitions.stream({
	name: "method",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Client methods are not models.
	owns: { $executeRaw: { kind: "table" } },
});
definitions.stream({
	name: "nonunique",
	key: z.object({ title: z.string() }),
	owns: {
		// @ts-expect-error title is a scalar, but is not a unique selector.
		post: { kind: "unique", selector: "title", fields: { title: "title" } },
	},
});
definitions.stream({
	name: "incomplete-selector",
	key: z.object({ id: z.string() }),
	owns: {
		// @ts-expect-error The compound selector requires all its component fields.
		post: {
			kind: "unique",
			selector: "authorId_slug",
			fields: { authorId: "id" },
		},
	},
});
definitions.stream({
	name: "extra-selector-component",
	key: z.object({ id: z.string() }),
	owns: {
		// @ts-expect-error A scalar unique selector maps exactly its single scalar field.
		post: { kind: "unique", selector: "id", fields: { id: "id", slug: "id" } },
	},
});
definitions.stream({
	name: "unknown-field",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Unknown scalar field.
	owns: { post: { kind: "partition", fields: { missing: "id" } } },
});
definitions.stream({
	name: "relation",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Relations cannot participate in ownership mappings.
	owns: { post: { kind: "partition", fields: { author: "id" } } },
});
definitions.stream({
	name: "nullable",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Nullable columns cannot establish ownership.
	owns: { user: { kind: "partition", fields: { nickname: "id" } } },
});
definitions.stream({
	name: "mismatched",
	key: z.object({ number: z.string() }),
	// @ts-expect-error A string key cannot bind an integer column.
	owns: { comment: { kind: "partition", fields: { number: "number" } } },
});
definitions.stream({
	name: "list",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Scalar lists are not scalar key columns.
	owns: { post: { kind: "partition", fields: { tags: "id" } } },
});
definitions.stream({
	name: "wrong-key",
	key: z.object({ id: z.string() }),
	// @ts-expect-error Mappings reference parsed key components, not arbitrary values.
	owns: { post: { kind: "partition", fields: { id: "other" } } },
});
definitions.stream({
	name: "optional",
	// @ts-expect-error Optional key components can be omitted by Prisma.
	key: z.object({ id: z.string().optional() }),
	owns: { post: { kind: "table" } },
});
definitions.stream({
	name: "unsupported",
	// @ts-expect-error Object-valued key components are not supported.
	key: z.object({ id: z.object({ nested: z.string() }) }),
	owns: { post: { kind: "table" } },
});
definitions.stream({
	name: "ambiguous",
	// @ts-expect-error A key component cannot alternate between string and number.
	key: z.object({ id: z.union([z.string(), z.number()]) }),
	owns: { post: { kind: "table" } },
});
const alias = { kind: "table", fields: { id: "id" } } as const;
definitions.stream({
	name: "alias",
	key: z.object({}),
	// @ts-expect-error Unsupported properties are rejected even in predeclared ownership objects.
	owns: { post: alias },
});

// @ts-expect-error Non-Prisma delegates must not hit public utility any fallbacks.
createDefinitions<{ post: { findUnique(): Promise<unknown> } }>();
// @ts-expect-error Unknown client types fail closed.
createDefinitions<unknown>();
// @ts-expect-error An erased client is not recognized.
createDefinitions<ReturnType<typeof JSON.parse>>();
const partiallyErased = createDefinitions<{
	post: Prisma.TransactionClient["post"];
	user: ReturnType<typeof JSON.parse>;
}>();
partiallyErased.stream({
	name: "erased",
	key: z.object({}),
	// @ts-expect-error An any-erased property is never a model.
	owns: { user: { kind: "table" } },
});

defineEvent({
	name: "bad",
	// @ts-expect-error An event version is a positive integer literal.
	version: 0,
	schema: z.string(),
	stream: singleton,
	route: () => ({}),
});
defineEvent({
	name: "bad",
	// @ts-expect-error Fractional versions are invalid.
	version: 1.5,
	schema: z.string(),
	stream: singleton,
	route: () => ({}),
});
defineEvent({
	name: "bad",
	// @ts-expect-error Negative versions are invalid.
	version: -1,
	schema: z.string(),
	stream: singleton,
	route: () => ({}),
});
declare const version: number;
defineEvent({
	name: "bad",
	// @ts-expect-error A widened number cannot identify one event version.
	version,
	schema: z.string(),
	stream: singleton,
	route: () => ({}),
});
declare const unionVersion: 1 | 2;
defineEvent({
	name: "bad",
	// @ts-expect-error An event descriptor identifies exactly one version.
	version: unionVersion,
	schema: z.string(),
	stream: singleton,
	route: () => ({}),
});
const wrongPayload: EventPayload<(typeof registry.events)[0]> = {
	title: "x",
	// @ts-expect-error The registry preserves each event's schema, not a widened union.
	length: "1",
};
void wrongPayload;

const mutableOwner: {
	post: { kind: "partition"; fields: { authorId: "authorId" } };
} = {
	post: { kind: "partition", fields: { authorId: "authorId" } },
};
const immutableStream = definitions.stream({
	name: "immutable",
	key: z.object({ authorId: z.string() }),
	owns: mutableOwner,
});
// @ts-expect-error Copied metadata is readonly even if its original input was mutable.
immutableStream.owns.post.fields.authorId = "authorId";
const streams = [immutableStream];
const events = [
	defineEvent({
		name: "immutable",
		version: 1,
		stream: immutableStream,
		schema: z.string(),
		route: (authorId) => ({ authorId }),
	}),
];
const immutableRegistry = definitions.registry({ streams, events });
// @ts-expect-error Copied registry lists are always readonly.
immutableRegistry.streams[0] = immutableStream;
createDefinitions<{ post: Prisma.TransactionClient["post"] }>().registry({
	// @ts-expect-error Registry clients remain invariant, not structurally widened across schemas.
	streams: [author],
	events: [],
});

declare const erasedOwner: Ownership;
definitions.stream({
	name: "erased-owner",
	key: z.object({ authorId: z.string() }),
	// @ts-expect-error A union-erased declaration cannot bypass its field and selector checks.
	owns: { post: erasedOwner },
});
declare const ambiguousSelector: "id" | "email";
definitions.stream({
	name: "ambiguous-selector",
	key: z.object({ id: z.string() }),
	owns: {
		// @ts-expect-error A unique declaration must identify one selector, not several alternatives.
		user: { kind: "unique", selector: ambiguousSelector, fields: { id: "id" } },
	},
});

const singletonEvent = defineEvent({
	name: "users.read",
	version: 1,
	stream: singleton,
	schema: z.strictObject({}),
	route: () => ({}),
});
definitions.registry({ streams: [singleton], events: [singletonEvent] });
// @ts-expect-error Singleton events still require explicit routing.
defineEvent({ name: "bad", version: 1, stream: singleton, schema: z.string() });
// @ts-expect-error Unique-row streams require routing.
defineEvent({ name: "bad", version: 1, stream: comment, schema: z.string() });
// @ts-expect-error Partition streams require routing.
defineEvent({ name: "bad", version: 1, stream: author, schema: z.string() });
// @ts-expect-error Every event requires a stream.
defineEvent({ name: "bad", version: 1, schema: z.string(), route: () => ({}) });
defineEvent({
	name: "bad",
	version: 1,
	stream: singleton,
	schema: z.string(),
	// @ts-expect-error Even empty singleton keys must not accept Promise-returning routes.
	route: async () => ({}),
});
defineEvent({
	name: "bad",
	version: 1,
	stream: author,
	schema: z.string(),
	// @ts-expect-error A route annotation must not widen the supplied schema.
	route: (value: number) => ({ authorId: String(value) }),
});
defineEvent({
	name: "bad",
	version: 1,
	stream: author,
	schema: z.string(),
	route: (authorId) => ({ authorId }),
	// @ts-expect-error Ownership is declared only on streams.
	owns: { user: { kind: "table" } },
});
definitions.registry({
	streams: [],
	// @ts-expect-error Membership is required without handlers too.
	events: [event],
});
definitions.registry({
	streams: [comment],
	// @ts-expect-error An unrelated stream cannot supply event ownership.
	events: [event],
});
const partialDefinitions = createDefinitions<{
	post: Prisma.TransactionClient["post"];
}>();
const partialAuthor = partialDefinitions.stream({
	name: "author",
	key: author.key,
	owns: { post: { kind: "partition", fields: { authorId: "authorId" } } },
});
const fullAuthor = definitions.stream({
	name: partialAuthor.name,
	key: partialAuthor.key,
	owns: partialAuthor.owns,
});
const fullClientEvent = defineEvent({
	name: "post.full-client",
	version: 1,
	stream: fullAuthor,
	schema: z.string(),
	route: (authorId) => ({ authorId }),
});
partialDefinitions.registry({
	streams: [partialAuthor],
	// @ts-expect-error Event streams retain the invariant Prisma-client association without handlers.
	events: [fullClientEvent],
});
