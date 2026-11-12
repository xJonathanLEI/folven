import { randomUUID } from "node:crypto";
import type { EffectRequester } from "../effect.js";
import { DefinitionError } from "../errors.js";
import type { EventPayload, EventRegistration } from "../event.js";
import { validateEvent } from "../event.js";
import type {
	EventEmitter,
	HandlerContext,
	HandlerDefinition,
	StreamClient,
} from "../handler.js";
import { parseStreamKey } from "../stream.js";
import type { StreamKey } from "../stream.js";
import { bindHandlerContext } from "../trusted-binding.js";
import { channelOccurrenceId, parseChannelSequence } from "./channel.js";
import { decodeInput, encodeInput, payloadFormat } from "./encoding.js";
import {
	ChannelEnvelopeError,
	PendingOccurrenceEnvelopeError,
} from "./errors.js";
import { canonicalStreamKey, validateIdentityText } from "./identity.js";
import type { DeliveryEnvelope, IncomingOrigin } from "./types.js";

/** Prepared event facts do not require a local target implementation. */
export interface PreparedEvent<Event extends EventRegistration> {
	readonly eventName: string;
	readonly eventVersion: number;
	readonly streamName: string;
	readonly streamKey: string;
	readonly key: Readonly<StreamKey<Event["stream"]>>;
	readonly payloadFormat: typeof payloadFormat;
	readonly encodedInput: string;
}

/** The public boundary has validated the envelope and resolved its handler. */
export function prepareIncomingOccurrence<
	Event extends EventRegistration,
	Result,
>(
	handler: HandlerDefinition<Event, Result>,
	envelope: DeliveryEnvelope,
): {
	readonly occurrence: PreparedOccurrence<Event, Result>;
	readonly firstPayload: EventPayload<Event>;
} {
	const { prepared, firstPayload } = prepareEncodedEvent(
		handler.event,
		envelope.encodedInput,
	);
	if (
		prepared.streamName !== envelope.target.streamName ||
		prepared.streamKey !== envelope.target.streamKey
	) {
		const EnvelopeError =
			"occurrenceId" in envelope
				? PendingOccurrenceEnvelopeError
				: ChannelEnvelopeError;
		throw new EnvelopeError(
			"The event route does not match the envelope target.",
		);
	}
	return Object.freeze({
		occurrence: Object.freeze({
			...prepared,
			handler,
			...("occurrenceId" in envelope
				? { occurrenceId: envelope.occurrenceId }
				: {
						occurrenceId: channelOccurrenceId(envelope),
						incoming: Object.freeze({
							source: envelope.source,
							channelSequence: parseChannelSequence(envelope.channelSequence),
						}),
					}),
		}),
		firstPayload,
	});
}

/** Private durable facts; no caller payload or mutable attempt state is kept. */
export interface PreparedOccurrence<
	Event extends EventRegistration,
	Result,
> extends PreparedEvent<Event> {
	readonly occurrenceId: string;
	readonly handler: HandlerDefinition<Event, Result>;
	readonly incoming?: IncomingOrigin;
}

export function prepareEvent<Event extends EventRegistration>(
	event: Event,
	input: unknown,
): {
	readonly prepared: PreparedEvent<Event>;
	readonly firstPayload: EventPayload<Event>;
} {
	return prepareEncodedEvent(event, encodeInput(input));
}

/** Preserve original input bytes, including fields removed by schema parsing. */
export function prepareEncodedEvent<Event extends EventRegistration>(
	event: Event,
	encodedInput: string,
): {
	readonly prepared: PreparedEvent<Event>;
	readonly firstPayload: EventPayload<Event>;
} {
	validateEvent(event);
	validateIdentityText(event.name);
	validateIdentityText(event.stream.name);
	const firstPayload = decodeInput(event, encodedInput);
	const route = event.route;
	if (typeof route !== "function") {
		throw new DefinitionError("An occurrence requires a callable event route.");
	}
	// Metadata erases the route signature. The event schema parsed this exact
	// descriptor's input above; parseStreamKey checks its unknown route output.
	const routed: unknown = (route as (payload: EventPayload<Event>) => unknown)(
		firstPayload,
	);
	const key = Object.freeze(
		parseStreamKey<Event["stream"]>(event.stream, routed),
	);
	const streamKey = canonicalStreamKey(event.stream.name, key);
	return Object.freeze({
		prepared: Object.freeze({
			eventName: event.name,
			eventVersion: event.version,
			streamName: event.stream.name,
			streamKey,
			key,
			payloadFormat,
			encodedInput,
		}),
		firstPayload,
	});
}

export function prepareOccurrence<Event extends EventRegistration, Result>(
	handler: HandlerDefinition<Event, Result>,
	input: unknown,
): {
	readonly occurrence: PreparedOccurrence<Event, Result>;
	readonly firstPayload: EventPayload<Event>;
} {
	const { prepared, firstPayload } = prepareEvent(handler.event, input);
	return Object.freeze({
		occurrence: Object.freeze({
			...prepared,
			occurrenceId: randomUUID(),
			handler,
		}),
		firstPayload,
	});
}

export function reconstructPayload<Event extends EventRegistration, Result>(
	occurrence: PreparedOccurrence<Event, Result>,
): EventPayload<Event> {
	return decodeInput(occurrence.handler.event, occurrence.encodedInput);
}

/** Lend a fresh context with the already resolved key and the actual client. */
export function bindOccurrenceAttempt<
	Event extends EventRegistration,
	Result,
	Output,
>(
	occurrence: PreparedOccurrence<Event, Result>,
	client: NoInfer<StreamClient<Event["stream"]>>,
	payload: EventPayload<Event>,
	use: <Invocation>(context: HandlerContext<Event, Invocation>) => Output,
	emit?: EventEmitter,
	requestEffect?: EffectRequester,
): Output {
	return use(
		bindHandlerContext(
			occurrence.handler,
			client,
			payload,
			occurrence.key,
			emit,
			requestEffect,
		),
	);
}
