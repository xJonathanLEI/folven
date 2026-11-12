import { z } from "zod";
import { validateEffect } from "./effect.js";
import type { EffectPayload, EffectRegistration } from "./effect.js";
import { DefinitionError } from "./errors.js";
import type { EventInput, EventRegistration } from "./event.js";

const eventDescription = Symbol("eventDescription");
const effectHandlerDefinition = Symbol("effectHandlerDefinition");

/** A description, not a prepared occurrence. Construct through eventInput. */
export interface EventInputDescription<
	Event extends EventRegistration = EventRegistration,
> {
	readonly event: Event;
	readonly input: EventInput<Event>;
	readonly [eventDescription]: true;
}

/** No parsing, routing, dispatch, storage, or task publication occurs here. */
export function eventInput<Event extends EventRegistration>(
	event: Event,
	input: EventInput<NoInfer<Event>>,
): EventInputDescription<Event> {
	return Object.freeze({
		event,
		input,
		[eventDescription]: true as const,
	});
}

export interface EffectReceipt {
	readonly events: readonly EventInputDescription[];
}

/** Private execution boundary; canonical membership and payload parsing follow. */
export function parseEffectReceipt(receipt: unknown): EffectReceipt {
	return z
		.strictObject({
			events: z.array(
				z.custom<EventInputDescription>(
					(value: unknown) =>
						typeof value === "object" &&
						value !== null &&
						Reflect.ownKeys(value).length === 3 &&
						"event" in value &&
						"input" in value &&
						eventDescription in value &&
						value[eventDescription] === true,
				),
			),
		})
		.parse(receipt);
}

export interface EffectHandlerContext<Effect extends EffectRegistration> {
	readonly effect: EffectPayload<Effect>;
	/** Stable registration identity, supplied by the execution host. */
	readonly effectId: string;
}

/** Metadata only; application-owned closures supply external dependencies. */
export interface EffectHandlerRegistration {
	readonly effect: EffectRegistration;
	readonly [effectHandlerDefinition]: true;
}

export interface EffectHandlerDefinition<
	Effect extends EffectRegistration,
	Result extends EffectReceipt | Promise<EffectReceipt> =
		| EffectReceipt
		| Promise<EffectReceipt>,
> extends EffectHandlerRegistration {
	readonly effect: Effect;
	readonly handle: (context: EffectHandlerContext<Effect>) => Result;
}

export function defineEffectHandler<
	Effect extends EffectRegistration,
	Result extends EffectReceipt | Promise<EffectReceipt>,
>(input: {
	readonly effect: Effect;
	readonly handle: (context: EffectHandlerContext<NoInfer<Effect>>) => Result;
}): EffectHandlerDefinition<Effect, Result> {
	validateEffect(input.effect);
	if (typeof input.handle !== "function") {
		throw new DefinitionError(
			`Effect "${input.effect.name}" version ${input.effect.version} must declare a callable handler.`,
		);
	}
	return Object.freeze({
		effect: input.effect,
		handle: input.handle,
		[effectHandlerDefinition]: true as const,
	});
}
