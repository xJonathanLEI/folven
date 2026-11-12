import { z } from "zod";
import type {
	Prisma,
	PrismaClient,
} from "../../tests/fixtures/generated/client.js";
import { createDefinitions, defineEvent } from "../index.js";
import {
	createPostgresPublisher,
	createPostgresRuntime,
	type PendingOccurrenceEnvelope,
	type PendingOccurrenceRead,
	type PendingOccurrencePage,
	type PostgresBorrowedTransaction,
	type PreparedPendingOccurrence,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const rename = defineEvent({
	name: "author.rename",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), name: z.string() }),
	route: ({ id }) => ({ id }),
});
const timestamp = defineEvent({
	name: "author.timestamp",
	version: 1,
	stream: author,
	schema: z.strictObject({
		id: z.string(),
		at: z.string().transform((value) => new Date(value)),
	}),
	route: ({ id }) => ({ id }),
});
const unrelated = defineEvent({ ...rename, name: "unrelated" });
const registry = definitions.registry({
	streams: [author],
	events: [rename, timestamp],
	handlers: [],
});
const publisher = createPostgresPublisher({ registry, schema: "metadata" });
const first = publisher.prepare(rename, { id: "a", name: "A" });
const second = publisher.prepare(timestamp, {
	id: "a",
	at: "2026-01-01T00:00:00Z",
});
// @ts-expect-error Schema input, never transformed output.
publisher.prepare(timestamp, { id: "a", at: new Date() });
// @ts-expect-error Input cannot widen the chosen event.
publisher.prepare(rename, { id: "a", at: "2026-01-01T00:00:00Z" });
// @ts-expect-error Registered definitions only, even without handlers.
publisher.prepare(unrelated, { id: "a", name: "A" });
// @ts-expect-error Preparations have no public mutable metadata.
first.occurrenceId = "caller-selected";
declare const root: PrismaClient;
declare const transaction: Prisma.TransactionClient;
const borrowed: PostgresBorrowedTransaction = transaction;
const result = publisher.stage(borrowed, [first, second] as const);
// @ts-expect-error Root-only lifecycle methods exclude an obvious root client.
void publisher.stage(root, [first]);
// @ts-expect-error Raw objects cannot become prepared occurrences.
void publisher.stage(transaction, [{ id: "a", name: "A" }]);
declare const envelope: PendingOccurrenceEnvelope;
// @ts-expect-error Envelopes with caller IDs are not publication preparations.
void publisher.stage(transaction, [envelope]);
// @ts-expect-error An opaque preparation cannot be reconstructed.
const fake: PreparedPendingOccurrence = {};
void fake;
void result.then((staged) => {
	// @ts-expect-error Staged references and collections are readonly.
	staged.references[0] = { occurrenceId: "id" };
	const reference = staged.references[0];
	if (reference) {
		// @ts-expect-error Staged identities are readonly.
		reference.occurrenceId = "changed";
	}
	// @ts-expect-error Notifications carry no republish input.
	void staged.notifyCommitted([first]);
	const notification: Promise<void> = staged.notifyCommitted();
	void notification;
});
const runtime = createPostgresRuntime({
	client: root,
	registry,
	schema: "metadata",
});
const retained: Promise<PendingOccurrenceRead> = runtime.outbox.read({
	occurrenceId: "id",
});
const pending: Promise<PendingOccurrencePage> =
	runtime.outbox.readPendingOccurrences({ after: "id", limit: 1 });
void [retained, pending];
void runtime.outbox.acknowledge(envelope);
void retained.then((read) => {
	// @ts-expect-error Independent reads have no predecessor readiness.
	void read.hasPredecessor;
});
void runtime.outbox.readPendingOccurrences({
	// @ts-expect-error The independent cursor is not a channel cursor.
	after: { source: envelope.target, target: envelope.target },
});
