import { z } from "zod";
import type { addCommentHandler } from "../examples/comments.handler.js";
import {
	authorStream,
	addComment,
	commentStream,
	definitions,
	renamePost,
	readAuthor,
} from "../examples/definitions.js";
import type { readAuthorHandler } from "../examples/posts.handler.js";
import { renamePostHandler } from "../examples/posts.handler.js";
import type { registry } from "../examples/registry.js";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { prepareHandler } from "./adapter.js";
import { defineEvent, defineHandler } from "./index.js";
import type { BoundScope, EventEmitter, HandlerContext } from "./index.js";
import { transformedEvent } from "./registry.test-d.js";

type Equal<Left, Right> =
	(<Value>() => Value extends Left ? 1 : 2) extends <
		Value,
	>() => Value extends Right ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;

export type HandlerRow = Expect<
	Equal<
		Awaited<ReturnType<typeof renamePostHandler.handle>>,
		{ id: string; title: string }
	>
>;
export type NullableRow = Expect<
	Equal<
		Awaited<ReturnType<typeof readAuthorHandler.handle>>,
		{ id: string; name: string } | null
	>
>;
export type CompoundRow = Expect<
	Equal<
		Awaited<ReturnType<typeof addCommentHandler.handle>>,
		{ postId: string; number: number; body: string }
	>
>;
export type RegisteredHandler = Expect<
	Equal<(typeof registry.handlers)[0], typeof renamePostHandler>
>;

const parsedEvent = defineEvent({
	name: "post.parsed",
	version: 1,
	stream: authorStream,
	schema: z.strictObject({
		id: z.string().transform((value) => value.length),
		authorId: z.string(),
	}),
	route: (event) => {
		const length: number = event.id;
		// @ts-expect-error routing receives parsed output, not schema input
		const text: string = event.id;
		void [length, text];
		return { authorId: event.authorId };
	},
});

defineHandler({
	event: renamePost,
	handle({ event, emit }) {
		const result = emit(addComment, {
			postId: event.postId,
			authorId: event.authorId,
			number: 1,
			body: "Renamed",
		});
		const synchronous: void = result;
		emit(parsedEvent, { authorId: event.authorId, id: "schema input" });
		// @ts-expect-error transformed schema output is not the input
		emit(parsedEvent, { authorId: event.authorId, id: 12 });
		// @ts-expect-error a target's required input cannot be widened away
		emit(addComment, { postId: event.postId });
		// @ts-expect-error payloads from another event are not interchangeable
		emit(addComment, event);
		emit(
			parsedEvent,
			{ authorId: event.authorId, id: "input" },
			// @ts-expect-error callers cannot supply a routing/identity argument
			{ source: "x", sequence: "1" },
		);
		emit(
			// @ts-expect-error emission takes a descriptor, not a routing envelope
			{ event: parsedEvent, target: "x" },
			{ authorId: event.authorId, id: "input" },
		);
		emit(parsedEvent, {
			authorId: event.authorId,
			id: "input",
			// @ts-expect-error identity metadata is not part of this schema input
			occurrenceId: "x",
		});
		return { title: event.title, synchronous };
	},
});
const parsedHandler = defineHandler({
	event: parsedEvent,
	handle({ event, db, scope }) {
		const length: number = event.id;
		void length;
		// @ts-expect-error event context cannot widen the declared event
		void event.title;
		// @ts-expect-error handlers have no unrestricted client
		void db.$transaction;
		// @ts-expect-error undeclared models do not become available
		void db.comment;
		// @ts-expect-error raw event values are not ownership capabilities
		void db.post.findMany({ where: { authorId: event.authorId } });
		return db.post.findMany({
			where: scope.post.where,
			select: { title: true },
		});
	},
});
export type ParsedRows = Expect<
	Equal<Awaited<ReturnType<typeof parsedHandler.handle>>, { title: string }[]>
>;

defineEvent({
	name: "bad",
	version: 1,
	schema: renamePost.schema,
	stream: authorStream,
	// @ts-expect-error route cannot change the key's value type
	route: () => ({ authorId: 3 }),
});
defineEvent({
	name: "bad",
	version: 1,
	schema: renamePost.schema,
	stream: commentStream,
	// @ts-expect-error every stream-key component is required
	route: (event) => ({ postId: event.postId }),
});
defineEvent({
	name: "bad",
	version: 1,
	schema: renamePost.schema,
	stream: authorStream,
	// @ts-expect-error routing must be synchronous
	route: async (event) => ({ authorId: event.authorId }),
});
defineHandler({
	event: renamePost,
	// @ts-expect-error the handler cannot substitute another event's payload
	handle<Invocation>({
		event,
	}: HandlerContext<typeof parsedEvent, Invocation>) {
		return event.id;
	},
});
defineHandler({
	event: renamePost,
	handle() {},
	// @ts-expect-error no per-event ownership declaration
	owns: { comment: { kind: "table" } },
});

declare const previousScope: BoundScope<typeof authorStream, unknown>;
declare const savedScope: BoundScope<typeof authorStream, "previous">;
declare const commentScope: BoundScope<typeof commentStream, unknown>;
defineHandler({
	event: renamePost,
	handle({ db, scope }) {
		// @ts-expect-error an erased scope cannot match this fresh invocation
		void db.post.findMany({ where: previousScope.post.where });
		// @ts-expect-error a previous invocation cannot match this invocation
		void db.post.findMany({ where: savedScope.post.where });
		// @ts-expect-error an invocation cannot be stored under an erased token
		const captured: BoundScope<typeof authorStream, unknown> = scope;
		void captured;
		// @ts-expect-error another model's component is not interchangeable
		void db.post.findMany({ where: { authorId: scope.user.fields.id } });
		void db.post.findMany({
			// @ts-expect-error another stream's component is not interchangeable
			where: { authorId: commentScope.comment.fields.postId },
		});
	},
});

definitions.registry({
	streams: [authorStream],
	events: [readAuthor],
	// @ts-expect-error the handler's declared event is not in the registry
	handlers: [renamePostHandler],
});
definitions.registry({
	streams: [commentStream],
	// @ts-expect-error the event's stream must be registered, independently of handlers
	events: [renamePost],
	handlers: [renamePostHandler],
});

declare const client: Prisma.TransactionClient;
declare const emitter: EventEmitter;
const emittingAdapterResult = prepareHandler(
	parsedHandler,
	client,
	{ authorId: "u1", id: "input" },
	({ context }) => parsedHandler.handle(context),
	{ emit: emitter },
);
export type EmittingAdapterResult = Expect<
	Equal<typeof emittingAdapterResult, Promise<{ title: string }[]>>
>;
const preparedResult = prepareHandler(
	renamePostHandler,
	client,
	{},
	({ context, key }) => {
		const authorId: string = key.authorId;
		void authorId;
		// @ts-expect-error identity metadata is readonly and is not a capability
		key.authorId = "different";
		// @ts-expect-error identity metadata does not mint bound scalar keys
		void context.db.post.findMany({ where: { authorId: key.authorId } });
		return renamePostHandler.handle(context);
	},
);
export type PreparedResult = Expect<
	Equal<typeof preparedResult, Promise<{ id: string; title: string }>>
>;
void prepareHandler(renamePostHandler, client, {}, ({ context: outer }) =>
	prepareHandler(renamePostHandler, client, {}, ({ context: inner }) => {
		// @ts-expect-error two prepared contexts have independent invariant tokens
		void inner.db.post.findMany({ where: outer.scope.post.where });
		return renamePostHandler.handle(inner);
	}),
);
// @ts-expect-error the adapter must receive the stream's original client type
prepareHandler(renamePostHandler, {}, {}, () => {});

defineHandler({
	event: renamePost,
	handle() {},
	// @ts-expect-error A handler cannot choose a different stream.
	stream: commentStream,
});
defineHandler({
	event: renamePost,
	handle() {},
	// @ts-expect-error A handler cannot supply routing, even the event's own route.
	route: renamePost.route,
});
const streamOverride = { event: renamePost, handle() {}, stream: authorStream };
// @ts-expect-error Predeclared configurations cannot retain legacy stream fields.
defineHandler(streamOverride);
const routeOverride = {
	event: renamePost,
	handle() {},
	route: renamePost.route,
};
// @ts-expect-error Predeclared configurations cannot retain legacy route fields.
defineHandler(routeOverride);
// @ts-expect-error Forbidden optional fields cannot be supplied as undefined.
defineHandler({ event: renamePost, handle() {}, route: undefined });
// @ts-expect-error The handler descriptor does not duplicate stream metadata.
void renamePostHandler.stream;
// @ts-expect-error The handler descriptor does not duplicate routing metadata.
void renamePostHandler.route;

const transformedHandler = defineHandler({
	event: transformedEvent,
	handle({ db, scope }) {
		return db.comment.findUnique({
			where: scope.comment.unique,
			select: { body: true },
		});
	},
});
const transformedResult = prepareHandler(
	transformedHandler,
	client,
	{},
	({ key, context }) => {
		type PreparedKey = Expect<
			Equal<typeof key, Readonly<{ postId: string; number: number }>>
		>;
		const exact: PreparedKey = true;
		void exact;
		return transformedHandler.handle(context);
	},
);
export type TransformedResult = Expect<
	Equal<typeof transformedResult, Promise<{ body: string } | null>>
>;
