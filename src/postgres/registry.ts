import type {
	EffectHandlerDefinition,
	EffectHandlerRegistration,
} from "../effect-handler.js";
import type { EffectRegistration } from "../effect.js";
import { DefinitionError } from "../errors.js";
import type { EventRegistration } from "../event.js";
import type {
	HandlerDefinition,
	HandlerRegistration,
	StreamClient,
} from "../handler.js";
import type { DefinitionRegistry } from "../registry.js";
import { validateRegistrations } from "../registry.js";
import type { ScopeStream } from "../scope.js";
import { validateIdentityText } from "./identity.js";

type Registry = DefinitionRegistry<
	readonly ScopeStream[],
	readonly EventRegistration[],
	readonly HandlerRegistration[]
>;

/** Metadata-only membership capability, retaining the descriptor's exact type. */
export interface CanonicalEventLookup {
	getEvent<Event extends EventRegistration>(event: Event): Event;
}

export interface CanonicalEffectLookup {
	getEffect<Effect extends EffectRegistration>(effect: Effect): Effect;
}

export type RegistryClient<Definitions extends Registry> = StreamClient<
	Definitions["streams"][number]
>;

export type DispatchableEvent<Definitions extends Registry> =
	Definitions["handlers"][number]["event"];

export type RegisteredHandler<
	Definitions extends Registry,
	Event extends DispatchableEvent<Definitions>,
> = Extract<Definitions["handlers"][number], { readonly event: Event }>;

type HandlerReturn<
	Definitions extends Registry,
	Event extends DispatchableEvent<Definitions>,
> =
	RegisteredHandler<Definitions, Event> extends HandlerDefinition<
		infer _Event,
		infer Result
	>
		? Result
		: never;

export type DispatchResult<
	Definitions extends Registry,
	Event extends DispatchableEvent<Definitions>,
> = Awaited<HandlerReturn<Definitions, Event>>;

/** Private immutable lookup; dispatch/emission retain exact descriptor identity. */
export function createHandlerLookup<Definitions extends Registry>(
	registry: Definitions,
) {
	validateRegistrations(
		registry.streams,
		registry.events,
		registry.handlers,
		registry.effects,
		registry.effectHandlers,
	);
	for (const stream of registry.streams) {
		validateIdentityText(stream.name);
		for (const component of Object.keys(stream.key.shape)) {
			validateIdentityText(component);
		}
	}
	for (const event of registry.events) validateIdentityText(event.name);
	for (const effect of registry.effects) validateIdentityText(effect.name);

	const events = new Set<EventRegistration>(registry.events);
	const effects = new Set<EffectRegistration>(registry.effects);
	const identities = new Map<
		string,
		Map<number, Definitions["events"][number]>
	>();
	for (const event of registry.events) {
		const versions =
			identities.get(event.name) ??
			new Map<number, Definitions["events"][number]>();
		versions.set(event.version, event);
		identities.set(event.name, versions);
	}
	const handlers = new Map<EventRegistration, HandlerRegistration>();
	const effectHandlers = new Map<
		EffectRegistration,
		EffectHandlerRegistration
	>();
	for (const handler of registry.effectHandlers) {
		if (!("handle" in handler) || typeof handler.handle !== "function") {
			throw new DefinitionError(
				"An effect handler must have a callable implementation.",
			);
		}
		effectHandlers.set(handler.effect, handler);
	}
	for (const handler of registry.handlers) {
		if (!("handle" in handler) || typeof handler.handle !== "function") {
			throw new DefinitionError(
				"A registered authoritative handler must have a callable implementation.",
			);
		}
		handlers.set(handler.event, handler);
	}
	function requireHandler<Event extends EventRegistration>(
		event: Event,
	): HandlerDefinition<Event, unknown> {
		const handler = handlers.get(event);
		if (handler === undefined) {
			throw new DefinitionError(
				"Execution requires an exact registered event with an authoritative handler.",
			);
		}
		// Identity and callability were validated above; ingress has no result contract.
		return handler as HandlerDefinition<Event, unknown>;
	}
	return Object.freeze({
		resolveEffect(name: string, version: number): EffectRegistration {
			const effect = registry.effects.find(
				(candidate) => candidate.name === name && candidate.version === version,
			);
			if (!effect)
				throw new DefinitionError(
					"Execution requires a registered effect identity.",
				);
			return effect;
		},
		requireEffectHandler<Effect extends EffectRegistration>(
			effect: Effect,
		): EffectHandlerDefinition<Effect> {
			const handler = effectHandlers.get(effect);
			if (!handler)
				throw new DefinitionError(
					"Execution requires a registered effect implementation.",
				);
			// Canonical membership and callability above preserve this relationship
			// across the heterogeneous map, just as for event implementations.
			return handler as EffectHandlerDefinition<Effect>;
		},
		getEffect<Effect extends EffectRegistration>(effect: Effect): Effect {
			if (!effects.has(effect)) {
				throw new DefinitionError(
					"Effect requests require an exact registered effect descriptor.",
				);
			}
			return effect;
		},
		getEvent<Event extends EventRegistration>(event: Event): Event {
			if (!events.has(event)) {
				throw new DefinitionError(
					"Emission requires an exact registered event descriptor.",
				);
			}
			return event;
		},
		resolveEvent(name: string, version: number): Definitions["events"][number] {
			const event = identities.get(name)?.get(version);
			if (event === undefined) {
				throw new DefinitionError(
					"Ingress requires a registered event identity.",
				);
			}
			return event;
		},
		requireHandler,
		get<Event extends DispatchableEvent<Definitions>>(
			event: Event,
		): HandlerDefinition<Event, HandlerReturn<Definitions, Event>> {
			const handler = requireHandler(event);
			// Tuple erasure is confined here. Validation above proves uniqueness,
			// canonical descriptor identity and callability; the private map can
			// only contain the original concrete handlers from that tuple.
			return handler as HandlerDefinition<
				Event,
				HandlerReturn<Definitions, Event>
			>;
		},
	});
}
