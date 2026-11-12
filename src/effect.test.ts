import { expect, it, vi } from "vitest";
import { z } from "zod";
import {
	authorStream,
	definitions,
	renamePost,
} from "../examples/definitions.js";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { prepareHandler } from "./adapter.js";
import {
	defineEffect,
	defineEffectHandler,
	defineHandler,
	DefinitionError,
	EffectRequestUnavailableError,
	EmissionUnavailableError,
	eventInput,
} from "./index.js";
import type { EffectRequester } from "./index.js";

function indexingEffect() {
	return defineEffect({
		name: "post.index",
		version: 1,
		schema: z.strictObject({ postId: z.string() }),
	});
}

it("registers detached, frozen metadata without parsing inputs or executing callbacks", () => {
	const parse = vi.fn((value: string) => value.length);
	const declaration = {
		name: renamePost.name,
		version: 1,
		schema: z.string().transform(parse),
	} as const;
	const effect = defineEffect(declaration);
	const handle = vi.fn(() => ({ events: [] }));
	const implementation = { effect, handle };
	const handler = defineEffectHandler(implementation);
	const effects = [effect];
	const effectHandlers = [handler];
	const producer = definitions.registry({
		streams: [authorStream],
		events: [renamePost],
		effects,
	});
	const executor = definitions.registry({
		streams: [authorStream],
		events: [renamePost],
		effects,
		effectHandlers,
	});
	effects.length = 0;
	effectHandlers.length = 0;
	expect(producer.effects).toEqual([effect]);
	expect(producer.effectHandlers).toEqual([]);
	expect(executor.effects).toEqual([effect]);
	expect(executor.effectHandlers).toEqual([handler]);
	expect(effect).not.toBe(declaration);
	expect(effect.schema).toBe(declaration.schema);
	expect(handler).not.toBe(implementation);
	expect(handler.effect).toBe(effect);
	expect(handler.handle).toBe(handle);
	for (const value of [
		effect,
		handler,
		producer.effects,
		producer.effectHandlers,
		executor.effects,
		executor.effectHandlers,
	]) {
		expect(Object.isFrozen(value)).toBe(true);
	}
	expect(Object.isFrozen(effect.schema)).toBe(false);
	expect(parse).not.toHaveBeenCalled();
	expect(handle).not.toHaveBeenCalled();
});

it("requires canonical effect descriptors and rejects duplicate identities and implementations", () => {
	const effect = indexingEffect();
	const handler = defineEffectHandler({
		effect,
		handle: () => ({ events: [] }),
	});
	const base = { streams: [], events: [], effects: [effect] };
	for (const other of [effect, indexingEffect()]) {
		expect(() =>
			definitions.registry({ ...base, effects: [effect, other] }),
		).toThrow('Effect "post.index" version 1 is registered more than once');
	}
	for (const other of [
		handler,
		defineEffectHandler({ effect, handle: async () => ({ events: [] }) }),
	]) {
		expect(() =>
			definitions.registry({ ...base, effectHandlers: [handler, other] }),
		).toThrow(
			'Handler for effect "post.index" version 1 is registered more than once',
		);
	}
	expect(() =>
		definitions.registry({
			...base,
			effectHandlers: [
				defineEffectHandler({
					effect: indexingEffect(),
					handle: () => ({ events: [] }),
				}),
			],
		}),
	).toThrow('Handler refers to unregistered effect "post.index" version 1');
	const absent: (typeof effect)[] = [];
	expect(() =>
		definitions.registry({
			...base,
			effects: absent,
			effectHandlers: [handler],
		}),
	).toThrow("unregistered effect");
	expect(
		definitions.registry({
			...base,
			effects: [
				effect,
				defineEffect({ name: effect.name, version: 2, schema: z.number() }),
			],
		}).effects,
	).toHaveLength(2);
});

it("validates effect metadata without examining payloads", () => {
	expect(() =>
		defineEffect({ name: " ", version: 1, schema: z.string() }),
	).toThrow(DefinitionError);
	expect(() =>
		defineEffect({
			name: "post.index",
			version: 9007199254740992,
			schema: z.string(),
		}),
	).toThrow("positive safe-integer version");
	expect(() =>
		defineEffect({
			name: "post.index",
			// @ts-expect-error JavaScript input is also validated.
			version: 0,
			schema: z.string(),
		}),
	).toThrow("positive safe-integer version");
	expect(() =>
		defineEffect({
			name: "post.index",
			version: 1,
			// @ts-expect-error JavaScript callers must supply a Zod schema.
			schema: {},
		}),
	).toThrow("must declare a Zod schema");
	for (const field of ["stream", "route", "owns"]) {
		expect(() =>
			defineEffect({
				name: "post.index",
				version: 1,
				schema: z.string(),
				[field]: {},
			}),
		).toThrow(`cannot declare "${field}"`);
	}
	expect(() =>
		defineEffectHandler({
			effect: indexingEffect(),
			// @ts-expect-error JavaScript callers must supply a function.
			handle: undefined,
		}),
	).toThrow("must declare a callable handler");
});

it("eventInput describes original schema input without parsing, routing, or dispatching", () => {
	const parse = vi.fn((value: string) => value.length);
	const route = vi.fn(() => ({ authorId: "u1" }));
	const event = { ...renamePost, schema: z.string().transform(parse), route };
	const description = eventInput(event, "original input");
	expect(description.event).toBe(event);
	expect(description.input).toBe("original input");
	expect(Object.isFrozen(description)).toBe(true);
	expect(parse).not.toHaveBeenCalled();
	expect(route).not.toHaveBeenCalled();
});

it("fails closed without an effect capability and preserves the explicit client and scope bindings", () => {
	const effect = indexingEffect();
	// Identity sentinel only: no database methods are called.
	const client = Object.freeze({}) as Prisma.TransactionClient;
	const source = defineHandler({
		event: renamePost,
		handle({ event, db, scope, requestEffect, emit }) {
			expect(db).toBe(client);
			expect(scope.post.where).toEqual({ authorId: "u1" });
			requestEffect(effect, { postId: event.postId });
			expect(() => emit(renamePost, event)).toThrow(EmissionUnavailableError);
			return event.title;
		},
	});
	const input = { authorId: " u1 ", postId: "p1", title: "New title" };
	expect(() =>
		prepareHandler(source, client, input, ({ context }) =>
			source.handle(context),
		),
	).toThrow(EffectRequestUnavailableError);
	const requestEffect = vi.fn<EffectRequester>();
	expect(
		prepareHandler(
			source,
			client,
			input,
			({ context }) => source.handle(context),
			{ requestEffect },
		),
	).toBe("New title");
	expect(requestEffect).toHaveBeenCalledExactlyOnceWith(effect, {
		postId: "p1",
	});
});
