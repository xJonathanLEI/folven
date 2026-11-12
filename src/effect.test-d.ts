import { z } from "zod";
import {
	addComment,
	authorStream,
	definitions,
	renamePost,
} from "../examples/definitions.js";
import {
	defineEffect,
	defineEffectHandler,
	defineEvent,
	defineHandler,
	eventInput,
} from "./index.js";
import type {
	EffectInput,
	EffectPayload,
	EffectReceipt,
	EventInputDescription,
} from "./index.js";

type Equal<Left, Right> =
	(<Value>() => Value extends Left ? 1 : 2) extends <
		Value,
	>() => Value extends Right ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;

const effect = defineEffect({
	name: "post.index",
	version: 1,
	schema: z.strictObject({
		postId: z.string(),
		revision: z.string().transform((value) => Number(value)),
	}),
});

export type ExactInput = Expect<
	Equal<EffectInput<typeof effect>, { postId: string; revision: string }>
>;
export type ExactOutput = Expect<
	Equal<EffectPayload<typeof effect>, { postId: string; revision: number }>
>;
export type ExactIdentity = Expect<Equal<typeof effect.name, "post.index">>;
export type ExactVersion = Expect<Equal<typeof effect.version, 1>>;

const parsedEvent = defineEvent({
	name: "post.indexed",
	version: 1,
	stream: authorStream,
	schema: z.string().transform((value) => value.length),
	route: () => ({ authorId: "u1" }),
});

const handler = defineEffectHandler({
	effect,
	handle(context) {
		const input: { postId: string; revision: number } = context.effect;
		const id: string = context.effectId;
		// @ts-expect-error Effect handlers receive parsed output, not raw input.
		const revision: string = context.effect.revision;
		// @ts-expect-error No injected database.
		void context.db;
		// @ts-expect-error No command scope.
		void context.scope;
		// @ts-expect-error No imperative emitter.
		void context.emit;
		// @ts-expect-error No effect collector.
		void context.requestEffect;
		void [input, id, revision];
		return {
			events: [
				eventInput(parsedEvent, "original input"),
				eventInput(addComment, {
					authorId: "u1",
					postId: "p1",
					number: 1,
					body: "Indexed",
				}),
			],
		};
	},
});

export type HeterogeneousReceipt = Expect<
	Equal<
		ReturnType<typeof handler.handle>["events"][number],
		| EventInputDescription<typeof parsedEvent>
		| EventInputDescription<typeof addComment>
	>
>;

defineEffectHandler({ effect, handle: async () => ({ events: [] }) });
// @ts-expect-error Completion requires a receipt, even when there are no events.
defineEffectHandler({ effect, handle: () => {} });
// @ts-expect-error Async missing receipts are also rejected.
defineEffectHandler({ effect, handle: async () => {} });
// @ts-expect-error Arbitrary command-style return values are not receipts.
defineEffectHandler({ effect, handle: () => 1 });
const untypedReceipt: EffectReceipt = {
	// @ts-expect-error Descriptions must come from the typed constructor.
	events: [{ event: parsedEvent, input: 1 }],
};
void untypedReceipt;
// @ts-expect-error Event descriptions take schema input, not transformed output.
eventInput(parsedEvent, 12);
// @ts-expect-error Heterogeneous receipts cannot swap inputs.
eventInput(addComment, "incorrect");
// @ts-expect-error A description has no stream override.
eventInput(parsedEvent, "input", authorStream);

defineHandler({
	event: renamePost,
	handle({ requestEffect }) {
		const result: void = requestEffect(effect, { postId: "p1", revision: "1" });
		// @ts-expect-error Request collection takes schema input.
		requestEffect(effect, { postId: "p1", revision: 1 });
		// @ts-expect-error Required input cannot be widened away.
		requestEffect(effect, { postId: "p1" });
		// @ts-expect-error Effects and events are separate descriptor namespaces.
		requestEffect(renamePost, { postId: "p1", authorId: "u1", title: "Title" });
		void [result];
	},
});

const registry = definitions.registry({
	streams: [authorStream],
	events: [parsedEvent],
	effects: [effect],
	effectHandlers: [handler],
});
export type ExactRegisteredEffect = Expect<
	Equal<(typeof registry.effects)[0], typeof effect>
>;
export type ExactRegisteredHandler = Expect<
	Equal<(typeof registry.effectHandlers)[0], typeof handler>
>;
const legacy = definitions.registry({ streams: [], events: [] });
export type EmptyEffects = Expect<Equal<typeof legacy.effects, readonly []>>;
export type EmptyHandlers = Expect<
	Equal<typeof legacy.effectHandlers, readonly []>
>;
const producer = definitions.registry({
	streams: [],
	events: [],
	effects: [effect],
});
export type ProducerEffects = Expect<
	Equal<typeof producer.effects, readonly [typeof effect]>
>;
definitions.registry({
	streams: [],
	events: [],
	// @ts-expect-error An effect handler must reference a listed effect.
	effectHandlers: [handler],
});

defineEffect({
	name: "invalid",
	version: 1,
	schema: z.string(),
	// @ts-expect-error Effects own no command stream.
	stream: authorStream,
});
defineEffect({
	name: "invalid",
	version: 1,
	schema: z.string(),
	// @ts-expect-error Effects have no route.
	route: () => ({}),
});
// @ts-expect-error Effects declare no state ownership.
defineEffect({ name: "invalid", version: 1, schema: z.string(), owns: {} });
// @ts-expect-error Effect versions are positive.
defineEffect({ name: "invalid", version: 0, schema: z.string() });
// @ts-expect-error Effect versions are integers.
defineEffect({ name: "invalid", version: 1.5, schema: z.string() });
declare const dynamicVersion: number;
// @ts-expect-error Definition identity requires a literal version.
defineEffect({ name: "invalid", version: dynamicVersion, schema: z.string() });
// @ts-expect-error Events are not effect definitions.
defineEffectHandler({ effect: renamePost, handle: () => ({ events: [] }) });
