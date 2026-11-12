import type { EffectRequester } from "./effect.js";
import type { EventPayload, EventRegistration } from "./event.js";
import type {
	EventEmitter,
	HandlerContext,
	HandlerDefinition,
	StreamClient,
} from "./handler.js";
import { parseStreamKey } from "./stream.js";
import type { StreamKey, StreamKeyInput } from "./stream.js";
import { bindHandlerContext, parseHandlerEvent } from "./trusted-binding.js";

export interface PreparedHandler<Event extends EventRegistration, Invocation> {
	readonly key: Readonly<StreamKey<Event["stream"]>>;
	readonly context: HandlerContext<Event, Invocation>;
}

/**
 * Low-level adapter boundary, not a dispatcher or executor. Parse and route
 * once, then lend the context to a higher-rank callback. The adapter decides
 * whether to call the handler; no queries or transactions are opened here.
 */
export function prepareHandler<
	Event extends EventRegistration & {
		readonly route: (
			event: EventPayload<Event>,
		) => StreamKeyInput<Event["stream"]>;
	},
	Result,
	Output,
>(
	handler: HandlerDefinition<Event, Result>,
	client: NoInfer<StreamClient<Event["stream"]>>,
	payload: unknown,
	use: <Invocation>(prepared: PreparedHandler<Event, Invocation>) => Output,
	options?: {
		readonly emit?: EventEmitter;
		readonly requestEffect?: EffectRequester;
	},
): Output {
	const event = parseHandlerEvent(handler.event, payload);
	const key: StreamKey<Event["stream"]> = parseStreamKey<Event["stream"]>(
		handler.event.stream,
		handler.event.route(event),
	);
	const context = bindHandlerContext(
		handler,
		client,
		event,
		key,
		options?.emit,
		options?.requestEffect,
	);
	return use(Object.freeze({ key: Object.freeze(key), context }));
}
