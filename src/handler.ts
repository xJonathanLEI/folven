import type { EffectRequester } from "./effect.js";
import type { EventRegistration, EventPayload, EventInput } from "./event.js";
import { validateEvent } from "./event.js";
import type { FencedPrisma } from "./prisma-fence.js";
import type { BoundScope, ScopeStream } from "./scope.js";
import type { StreamDefinition } from "./stream.js";

const handlerDefinition = Symbol("handlerDefinition");

export type StreamClient<Stream extends ScopeStream> =
	Stream extends StreamDefinition<infer Client> ? Client : never;

export type HandlerDatabase<Stream extends ScopeStream, Invocation> =
	Stream extends StreamDefinition<infer Client>
		? FencedPrisma<Client, Stream, Invocation>
		: never;

/** Capture schema input for a transition; never execute the target handler. */
export interface EventEmitter {
	<TargetEvent extends EventRegistration>(
		event: TargetEvent,
		input: EventInput<NoInfer<TargetEvent>>,
	): void;
}

export interface HandlerContext<Event extends EventRegistration, Invocation> {
	readonly event: EventPayload<Event>;
	readonly db: HandlerDatabase<Event["stream"], Invocation>;
	readonly scope: BoundScope<Event["stream"], Invocation>;
	readonly emit: EventEmitter;
	readonly requestEffect: EffectRequester;
}

/** Metadata-only registration view; never erases or invokes a callback. */
export interface HandlerRegistration {
	readonly event: EventRegistration;
	readonly [handlerDefinition]: true;
}

export interface HandlerDefinition<
	Event extends EventRegistration,
	Result,
> extends HandlerRegistration {
	readonly event: Event;
	readonly handle: <Invocation>(
		context: HandlerContext<Event, Invocation>,
	) => Result;
}

export function defineHandler<Event extends EventRegistration, Result>(input: {
	readonly stream?: never;
	readonly route?: never;
	readonly event: Event;
	readonly handle: <Invocation>(
		context: HandlerContext<NoInfer<Event>, Invocation>,
	) => Result;
}): HandlerDefinition<Event, Result> {
	validateEvent(input.event);
	return Object.freeze({
		event: input.event,
		handle: input.handle,
		[handlerDefinition]: true as const,
	});
}
