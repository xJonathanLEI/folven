import type { z } from "zod";
import type { EffectHandlerRegistration } from "./effect-handler.js";
import { validateEffect } from "./effect.js";
import type { EffectRegistration } from "./effect.js";
import { DefinitionError } from "./errors.js";
import { validateEvent } from "./event.js";
import type { EventRegistration } from "./event.js";
import type { HandlerRegistration } from "./handler.js";
import type { OwnershipMap } from "./ownership.js";
import type { ModelNames } from "./prisma-types.js";
import type { ScopeStream } from "./scope.js";
import { createStream, validateStream } from "./stream.js";
import type { StreamDeclaration, StreamDefinition } from "./stream.js";

export interface DefinitionRegistry<
	Streams extends readonly unknown[],
	Events extends readonly EventRegistration[],
	Handlers extends readonly HandlerRegistration[] = readonly [],
	Effects extends readonly EffectRegistration[] = readonly EffectRegistration[],
	EffectHandlers extends readonly EffectHandlerRegistration[] =
		readonly EffectHandlerRegistration[],
> {
	readonly streams: readonly [...Streams];
	readonly events: readonly [...Events];
	readonly handlers: readonly [...Handlers];
	readonly effects: readonly [...Effects];
	readonly effectHandlers: readonly [...EffectHandlers];
}

/** Shared metadata-only validation; never parse, route or invoke an event. */
export function validateRegistrations(
	streams: readonly ScopeStream[],
	events: readonly EventRegistration[],
	handlers: readonly HandlerRegistration[],
	effects: readonly EffectRegistration[] = [],
	effectHandlers: readonly EffectHandlerRegistration[] = [],
): void {
	const identities = new Map<string, Set<number>>();
	for (const event of events) {
		validateEvent(event);
		if (!streams.some((stream) => stream === event.stream)) {
			throw new DefinitionError(
				`Event "${event.name}" version ${event.version} refers to unregistered stream "${event.stream.name}".`,
			);
		}
		const versions = identities.get(event.name) ?? new Set<number>();
		if (versions.has(event.version)) {
			throw new DefinitionError(
				`Event "${event.name}" version ${event.version} is registered more than once.`,
			);
		}
		versions.add(event.version);
		identities.set(event.name, versions);
	}
	const associations = new Set<EventRegistration>();
	for (const handler of handlers) {
		if (!events.some((event) => event === handler.event)) {
			throw new DefinitionError(
				`Handler for stream "${handler.event.stream.name}" refers to unregistered event "${handler.event.name}" version ${handler.event.version}.`,
			);
		}
		if (associations.has(handler.event)) {
			throw new DefinitionError(
				`Handler for stream "${handler.event.stream.name}", event "${handler.event.name}" version ${handler.event.version} is registered more than once.`,
			);
		}
		associations.add(handler.event);
	}
	const effectIdentities = new Map<string, Set<number>>();
	for (const effect of effects) {
		validateEffect(effect);
		const versions = effectIdentities.get(effect.name) ?? new Set<number>();
		if (versions.has(effect.version)) {
			throw new DefinitionError(
				`Effect "${effect.name}" version ${effect.version} is registered more than once.`,
			);
		}
		versions.add(effect.version);
		effectIdentities.set(effect.name, versions);
	}
	const effectAssociations = new Set<EffectRegistration>();
	for (const handler of effectHandlers) {
		if (!effects.some((effect) => effect === handler.effect)) {
			throw new DefinitionError(
				`Handler refers to unregistered effect "${handler.effect.name}" version ${handler.effect.version}.`,
			);
		}
		if (effectAssociations.has(handler.effect)) {
			throw new DefinitionError(
				`Handler for effect "${handler.effect.name}" version ${handler.effect.version} is registered more than once.`,
			);
		}
		effectAssociations.add(handler.effect);
	}
}

type RegisteredEvents<
	Streams extends readonly unknown[],
	Events extends readonly EventRegistration[],
> = {
	[Index in keyof Events]: Events[Index]["stream"] extends Streams[number]
		? unknown
		: never;
};

type RegisteredEffectHandlers<
	Effects extends readonly EffectRegistration[],
	Handlers extends readonly EffectHandlerRegistration[],
> = {
	[Index in keyof Handlers]: Handlers[Index]["effect"] extends Effects[number]
		? unknown
		: never;
};

type RegisteredHandlers<
	Events extends readonly EventRegistration[],
	Handlers extends readonly HandlerRegistration[],
> = {
	[Index in keyof Handlers]: Handlers[Index]["event"] extends Events[number]
		? unknown
		: never;
};

function immutableList<const Items extends readonly unknown[]>(
	items: Items,
): readonly [...Items] {
	// Preserve tuple identities while making a detached, frozen list.
	return Object.freeze([...items] as [...Items]);
}

/** One complete registry is required for all managed models in a client/schema. */
export function createDefinitions<Client>(
	..._validClient: [ModelNames<Client>] extends [never]
		? [unsupportedClient: never]
		: []
) {
	return Object.freeze({
		stream<
			const Name extends string,
			Key extends z.ZodObject,
			const Owns extends OwnershipMap,
		>(
			input: StreamDeclaration<Client, Name, Key, Owns>,
		): StreamDefinition<Client, Name, Key, Owns> {
			return createStream<Client, Name, Key, Owns>(input);
		},
		registry<
			const Streams extends readonly StreamDefinition<Client>[],
			const Events extends readonly EventRegistration[],
			const Handlers extends readonly HandlerRegistration[] = readonly [],
			const Effects extends readonly EffectRegistration[] = readonly [],
			const EffectHandlers extends readonly EffectHandlerRegistration[] =
				readonly [],
		>(
			input: {
				readonly streams: Streams;
				readonly events: Events &
					RegisteredEvents<NoInfer<Streams>, NoInfer<Events>>;
			} & (Handlers extends readonly []
				? { readonly handlers?: Handlers }
				: {
						readonly handlers: Handlers &
							RegisteredHandlers<NoInfer<Events>, NoInfer<Handlers>>;
					}) &
				(Effects extends readonly []
					? { readonly effects?: Effects }
					: { readonly effects: Effects }) &
				(EffectHandlers extends readonly []
					? { readonly effectHandlers?: EffectHandlers }
					: {
							readonly effectHandlers: EffectHandlers &
								RegisteredEffectHandlers<
									NoInfer<Effects>,
									NoInfer<EffectHandlers>
								>;
						}),
		): DefinitionRegistry<Streams, Events, Handlers, Effects, EffectHandlers> {
			const names = new Map<string, StreamDefinition<Client>>();
			const owners = new Map<string, StreamDefinition<Client>>();
			for (const stream of input.streams) {
				validateStream(stream);
				const previous = names.get(stream.name);
				if (previous !== undefined && previous !== stream) {
					throw new DefinitionError(
						`Stream "${stream.name}" has multiple definitions.`,
					);
				}
				names.set(stream.name, stream);
				for (const model of Object.keys(stream.owns)) {
					const owner = owners.get(model);
					if (owner !== undefined && owner !== stream) {
						throw new DefinitionError(
							`Model "${model}" is owned by both stream "${owner.name}" and stream "${stream.name}".`,
						);
					}
					owners.set(model, stream);
				}
			}
			const handlers = input.handlers ?? [];
			const effects = input.effects ?? [];
			const effectHandlers = input.effectHandlers ?? [];
			validateRegistrations(
				input.streams,
				input.events,
				handlers,
				effects,
				effectHandlers,
			);
			return Object.freeze({
				streams: immutableList(input.streams),
				events: immutableList<Events>(input.events),
				// Omission is permitted only for the empty tuple; callbacks are
				// never erased or invoked while validating heterogeneous metadata.
				handlers: immutableList(handlers) as unknown as readonly [...Handlers],
				effects: immutableList(effects) as unknown as readonly [...Effects],
				effectHandlers: immutableList(effectHandlers) as unknown as readonly [
					...EffectHandlers,
				],
			});
		},
	});
}
