import type { EffectRequester } from "./effect.js";
import {
	EffectRequestUnavailableError,
	EmissionUnavailableError,
} from "./errors.js";
import type { EventRegistration, EventPayload } from "./event.js";
import type {
	EventEmitter,
	HandlerContext,
	HandlerDatabase,
	HandlerDefinition,
	StreamClient,
} from "./handler.js";
import type { BoundScope, ScopeStream } from "./scope.js";
import type { StreamKey } from "./stream.js";

function emissionUnavailable(): never {
	throw new EmissionUnavailableError(
		"Emission requires an explicitly installed composition-owned emitter.",
	);
}

function effectRequestUnavailable(): never {
	throw new EffectRequestUnavailableError(
		"Effect requests require an explicitly installed composition-owned collector.",
	);
}

export function parseHandlerEvent<Event extends EventRegistration>(
	event: Event,
	payload: unknown,
): EventPayload<Event> {
	// The broad descriptor constraint erases the concrete schema output here.
	return event.schema.parse(payload) as EventPayload<Event>;
}

export function bindHandlerContext<
	Event extends EventRegistration,
	Result,
	Invocation,
>(
	handler: HandlerDefinition<Event, Result>,
	client: StreamClient<Event["stream"]>,
	event: EventPayload<Event>,
	key: Readonly<StreamKey<Event["stream"]>>,
	emit: EventEmitter = emissionUnavailable,
	requestEffect: EffectRequester = effectRequestUnavailable,
): HandlerContext<Event, Invocation> {
	return Object.freeze({
		event,
		db: narrowPrisma<Event["stream"], Invocation>(client),
		scope: bindScope<Event["stream"], Invocation>(handler.event.stream, key),
		emit,
		requestEffect,
	});
}

/**
 * Private trusted bridge. Prisma's generic delegates are not assignable to the
 * stricter generic signatures. The object itself MUST remain untouched.
 */
export function narrowPrisma<Stream extends ScopeStream, Invocation>(
	client: StreamClient<Stream>,
): HandlerDatabase<Stream, Invocation> {
	return client as HandlerDatabase<Stream, Invocation>;
}

/**
 * Private capability constructor. The caller must supply the already parsed,
 * validated key and a validated definition. No parsing, routing or execution.
 */
export function bindScope<Stream extends ScopeStream, Invocation>(
	stream: Stream,
	key: Readonly<StreamKey<Stream>>,
): BoundScope<Stream, Invocation> {
	const scope = Object.fromEntries(
		Object.entries(stream.owns).map(([model, owner]) => {
			const fields = Object.freeze(
				Object.fromEntries(
					owner.kind === "table"
						? []
						: Object.entries(owner.fields).map(([field, component]) => [
								field,
								key[component],
							]),
				),
			);
			const unique =
				owner.kind === "unique"
					? {
							unique: Object.freeze({
								[owner.selector]: Object.hasOwn(fields, owner.selector)
									? fields[owner.selector]
									: fields,
							}),
						}
					: {};
			return [model, Object.freeze({ where: fields, fields, ...unique })];
		}),
	);
	// Values are unchanged primitives. The assertion alone gives them their roles.
	return Object.freeze(scope) as BoundScope<Stream, Invocation>;
}
