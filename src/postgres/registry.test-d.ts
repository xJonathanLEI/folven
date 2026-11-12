import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import type { EventInput } from "../index.js";
import { bindOccurrenceAttempt, prepareOccurrence } from "./occurrence.js";
import { createHandlerLookup } from "./registry.js";
import type {
	DispatchableEvent,
	DispatchResult,
	RegisteredHandler,
	RegistryClient,
} from "./registry.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: {
		user: { kind: "unique", selector: "id", fields: { id: "id" } },
		post: { kind: "partition", fields: { authorId: "id" } },
	},
});
const read = defineEvent({
	name: "author.read",
	version: 1,
	stream,
	schema: z.strictObject({ id: z.string() }),
	route: (event) => event,
});
const timestamp = defineEvent({
	name: "author.timestamp",
	version: 1,
	stream,
	schema: z.strictObject({
		id: z.string(),
		at: z.string().transform((value) => new Date(value)),
	}),
	route: (event) => ({ id: event.id }),
});
const unhandled = defineEvent({
	name: "author.unhandled",
	version: 1,
	stream,
	schema: read.schema,
	route: read.route,
});
const unrelated = defineEvent({
	name: "unrelated",
	version: 1,
	stream,
	schema: read.schema,
	route: read.route,
});
const readHandler = defineHandler({
	event: read,
	handle: ({ db, scope }) =>
		db.user.findUnique({
			where: scope.user.unique,
			select: { id: true, nickname: true },
		}),
});
const timeHandler = defineHandler({
	event: timestamp,
	handle: async ({ event }) => ({ at: event.at, callback: () => 1n }),
});
const registry = definitions.registry({
	streams: [stream],
	events: [read, timestamp, unhandled],
	handlers: [readHandler, timeHandler],
});
const lookup = createHandlerLookup(registry);

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;

export type DispatchEvents = Expect<
	Equal<DispatchableEvent<typeof registry>, typeof read | typeof timestamp>
>;
export type SelectedResult = Expect<
	Equal<
		DispatchResult<typeof registry, typeof read>,
		{ id: string; nickname: string | null } | null
	>
>;
export type NonJsonResult = Expect<
	Equal<
		DispatchResult<typeof registry, typeof timestamp>,
		{ at: Date; callback: () => bigint }
	>
>;
export type ExactHandler = Expect<
	Equal<
		RegisteredHandler<typeof registry, typeof timestamp>,
		typeof timeHandler
	>
>;
export type ClientAssociation = Expect<
	Equal<RegistryClient<typeof registry>, Prisma.TransactionClient>
>;
const handler = lookup.get(timestamp);
export type LookupResult = Expect<Equal<typeof handler, typeof timeHandler>>;
export function prepareRegistered<
	Event extends DispatchableEvent<typeof registry>,
>(event: Event, input: EventInput<NoInfer<Event>>) {
	return prepareOccurrence(lookup.get(event), input);
}
const prepared = prepareRegistered(timestamp, {
	id: "u1",
	at: "2026-01-01T00:00:00Z",
});
export type GenericPreparation = Expect<
	Equal<typeof prepared.occurrence.handler, typeof timeHandler>
>;
// @ts-expect-error A definitions-only event cannot be dispatched.
lookup.get(unhandled);
// @ts-expect-error An unrelated event cannot be dispatched.
lookup.get(unrelated);
const definitionsOnly = definitions.registry({
	streams: [stream],
	events: [read],
});
// @ts-expect-error No handlers means no dispatchable events.
createHandlerLookup(definitionsOnly).get(read);

declare const client: Prisma.TransactionClient;
declare const wrongClient: Pick<Prisma.TransactionClient, "post">;
const { occurrence, firstPayload } = prepareOccurrence(handler, {
	id: "u1",
	at: "2026-01-01T00:00:00Z",
});
// @ts-expect-error Each bound attempt still requires the stream's client.
bindOccurrenceAttempt(occurrence, wrongClient, firstPayload, () => null);
export const result = bindOccurrenceAttempt(
	occurrence,
	client,
	firstPayload,
	(context) => {
		bindOccurrenceAttempt(occurrence, client, firstPayload, (otherAttempt) => {
			// @ts-expect-error Each attempt has its own invariant scope token.
			void otherAttempt.db.post.findMany({ where: context.scope.post.where });
		});
		return handler.handle(context);
	},
);
export type BoundResult = Expect<
	Equal<Awaited<typeof result>, { at: Date; callback: () => bigint }>
>;
