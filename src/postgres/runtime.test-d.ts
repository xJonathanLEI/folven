import { z } from "zod";
import type {
	Prisma,
	PrismaClient,
} from "../../tests/fixtures/generated/client.js";
import {
	createDeliveryRunner,
	type DeliveryResult,
	type DeliverySource,
} from "../delivery/index.js";
import {
	createDefinitions,
	defineEvent,
	defineHandler,
	defineEffect,
} from "../index.js";
import {
	createPostgresRuntime,
	type IngestionResult,
	type OutgoingEnvelope,
	type OutgoingReference,
	type OutgoingRead,
	type PostgresPublication,
	type PostgresEffectPublication,
	type PendingCursor,
	type PendingPage,
	type DeliveryReference,
	type DeliveryEnvelope,
	type DeliveryRead,
	type PendingOccurrenceReference,
	type PendingOccurrenceEnvelope,
	type PendingOccurrenceRead,
} from "./index.js";
import type { PostgresRawClient, PostgresTransactionHost } from "./types.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
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
	name: "unhandled",
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
const indexPost = defineEffect({
	name: "post.index",
	version: 1,
	schema: z.object({ at: z.string().transform((value) => new Date(value)) }),
});
const registry = definitions.registry({
	streams: [stream],
	events: [read, timestamp, unhandled],
	effects: [indexPost],
	handlers: [
		defineHandler({
			event: read,
			handle: ({ db, scope }) =>
				db.user.findUnique({
					where: scope.user.unique,
					select: { id: true, nickname: true },
				}),
		}),
		defineHandler({
			event: timestamp,
			handle: async ({ event, requestEffect }) => {
				requestEffect(indexPost, { at: event.at.toISOString() });
				// @ts-expect-error Requests take schema input, not transformed output.
				requestEffect(indexPost, { at: event.at });
				return { at: event.at, callback: () => 1n };
			},
		}),
	],
});
declare const client: PrismaClient;
const publication = {
	prepare(options) {
		// @ts-expect-error Preparation receives no transaction or SQL executor.
		void options.$queryRawUnsafe;
		// @ts-expect-error Preparation has no business delegates.
		void options.user;
		// @ts-expect-error Encoded source metadata is readonly.
		options.source.relation = "changed";
		return {
			sql: `INSERT INTO publication SELECT "${options.source.payloadColumn}" FROM "${options.source.relation}" RETURNING id`,
			parameters: [],
			validate(rows, count) {
				// @ts-expect-error Returned rows must be parsed before use.
				void rows[0]?.id;
				// @ts-expect-error Result collections are readonly.
				rows.length = count;
				return () => {};
			},
		};
	},
} satisfies PostgresPublication;
const effectPublication = {
	kind: "effect",
	prepare: (options) => publication.prepare(options),
} satisfies PostgresEffectPublication;
// @ts-expect-error Event delivery publication cannot schedule effects.
const invalidEffectPublication: PostgresEffectPublication = publication;
// @ts-expect-error Effect publication cannot publish event deliveries.
const invalidDeliveryPublication: PostgresPublication = effectPublication;
void [invalidEffectPublication, invalidDeliveryPublication];
const runtime = createPostgresRuntime({
	client,
	registry,
	schema: "folven",
	publication,
	effectPublication,
});
const source: DeliverySource = runtime.outbox;
const runner = createDeliveryRunner({ source, deliver: runtime.ingest });
declare const deliveryReference: OutgoingReference;
const delivery: Promise<DeliveryResult> = runner.attempt(deliveryReference);
void delivery.then((result) => {
	// @ts-expect-error Delivery never exposes a handler result.
	void result.result;
	if (result.status === "waiting" && result.reason === "gap") {
		const expected: string = result.expectedSequence;
		void expected;
	}
});
type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;
const selected = runtime.dispatch(read, { id: "u1" });
const transformed = runtime.dispatch(timestamp, {
	id: "u1",
	at: "2026-01-01T00:00:00Z",
});
export type Selected = Expect<
	Equal<
		typeof selected,
		Promise<{ id: string; nickname: string | null } | null>
	>
>;
declare const wire: unknown;
const ingested = runtime.ingest(wire);
export type Ingested = Expect<Equal<typeof ingested, Promise<IngestionResult>>>;
declare const independentEnvelope: PendingOccurrenceEnvelope;
void runtime.ingest(independentEnvelope);
declare const independentReference: PendingOccurrenceReference;
declare const independentRead: PendingOccurrenceRead;
const sharedReference: DeliveryReference = independentReference;
const sharedEnvelope: DeliveryEnvelope = independentEnvelope;
const sharedRead: DeliveryRead = independentRead;
void [sharedReference, sharedEnvelope, sharedRead];
function narrowDelivery(
	reference: DeliveryReference,
	envelope: DeliveryEnvelope,
) {
	if ("occurrenceId" in reference) {
		const id: string = reference.occurrenceId;
		// @ts-expect-error Independent references carry no source.
		void reference.source;
		// @ts-expect-error Durable identities are readonly.
		reference.occurrenceId = id;
	} else {
		const sequence: string = reference.channelSequence;
		void sequence;
	}
	if ("occurrenceId" in envelope) {
		// @ts-expect-error Independent envelopes carry no channel position.
		void envelope.channelSequence;
		// @ts-expect-error Retained wire facts are readonly.
		envelope.encodedInput = "{}";
	} else {
		const source: string = envelope.source.streamName;
		void source;
	}
}
void narrowDelivery;
// @ts-expect-error Independent reads have no predecessor readiness.
void independentRead.hasPredecessor;
declare const cursor: PendingCursor;
declare const envelope: OutgoingEnvelope;
declare const reference: OutgoingReference;
const retained = runtime.outbox.read(reference);
export type Retained = Expect<Equal<typeof retained, Promise<OutgoingRead>>>;
// @ts-expect-error An exact read requires a string channel sequence.
void runtime.outbox.read({ ...reference, channelSequence: 1 });
const pending = runtime.outbox.readPending({ limit: 100, after: cursor });
const acknowledged = runtime.outbox.acknowledge(envelope);
export type Pending = Expect<Equal<typeof pending, Promise<PendingPage>>>;
export type Acknowledged = Expect<Equal<typeof acknowledged, Promise<void>>>;
void runtime.outbox.readPending();
// @ts-expect-error A channel cursor is not an event sequence or global watermark.
void runtime.outbox.readPending({ after: "123" });
// @ts-expect-error Acknowledgment requires the complete envelope, not just identity.
void runtime.outbox.acknowledge(cursor);
void pending.then((page) => {
	// @ts-expect-error Pending envelope snapshots are readonly.
	page.envelopes[0] = envelope;
});
async function onlyAcknowledgment() {
	const result = await ingested;
	if (result.status === "gap") {
		const sequence: string = result.expectedSequence;
		return sequence;
	}
	// @ts-expect-error Ingestion never exposes a recovered handler result.
	void result.result;
}
void onlyAcknowledgment;
export type NonJson = Expect<
	Equal<typeof transformed, Promise<{ at: Date; callback: () => bigint }>>
>;
// @ts-expect-error A definitions-only event is not dispatchable.
void runtime.dispatch(unhandled, { id: "u1" });
// @ts-expect-error An unrelated event is not dispatchable.
void runtime.dispatch(unrelated, { id: "u1" });
// @ts-expect-error Input, not transformed output.
void runtime.dispatch(timestamp, { id: "u1", at: new Date() });
// @ts-expect-error Input cannot widen the selected descriptor.
void runtime.dispatch(read, { id: "u1", at: "extra" });
declare const transaction: Prisma.TransactionClient;
// @ts-expect-error The runtime owns the outer transaction; it cannot accept an existing transaction.
createPostgresRuntime({ client: transaction, registry, schema: "folven" });
declare const incompatible: PostgresTransactionHost<
	Pick<Prisma.TransactionClient, "$queryRawUnsafe" | "post">
> &
	PostgresRawClient;
// @ts-expect-error The client must implement the registry's client association.
createPostgresRuntime({ client: incompatible, registry, schema: "folven" });
