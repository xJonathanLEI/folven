import type { EventInput } from "../src/index.js";
import { postgresSchemaStatements } from "../src/postgres/admin.js";
import {
	createPostgresPublisher,
	createPostgresRuntime,
} from "../src/postgres/index.js";
import type { PrismaClient } from "../tests/fixtures/generated/client.js";
import {
	addComment,
	commentStream,
	definitions,
	readAuthor,
	renamePost,
} from "./definitions.js";
import { registry } from "./registry.js";

// Run once in an administrative transaction, after creating this schema in
// the command-state database. Runtime construction never executes this DDL.
export const journalStatements = postgresSchemaStatements({ schema: "folven" });

// A source-only registry can publish without importing the target handler.
export const ingressRegistry = definitions.registry({
	streams: [commentStream],
	events: [addComment],
	handlers: [],
});

export function createExamplePublisher() {
	return createPostgresPublisher({
		registry: ingressRegistry,
		schema: "folven",
	});
}

// Webhook is caller-owned source state, not command state owned by a stream.
// The caller has already bounded, authenticated and parsed its external input.
export async function handOffExampleWebhook(
	client: PrismaClient,
	publisher: ReturnType<typeof createExamplePublisher>,
	webhookId: string,
	input: EventInput<typeof addComment>,
) {
	// No I/O. Keep this same preparation if an outer facade retries known aborts.
	const prepared = publisher.prepare(addComment, input);
	const staged = await client.$transaction(async (transaction) => {
		// A guarded update, not an unlocked read followed by an unconditional write.
		const consumed = await transaction.webhook.updateMany({
			where: { id: webhookId, handedOff: false },
			data: { handedOff: true },
		});
		if (consumed.count === 0) return null;
		// Let every staging error escape so source consumption rolls back too.
		return publisher.stage(transaction, [prepared]);
	});
	// Only the winning callback's result survives a retrying transaction facade.
	// Never notify inside the callback, after rollback, or on an uncertain commit.
	await staged?.notifyCommitted();
	// Responsibility is durable, not yet processed. Batch order is not ingest order.
	return staged?.references ?? [];
}

// The caller constructs and eventually disconnects the root client.
export function createExampleRuntime(client: PrismaClient) {
	return createPostgresRuntime({
		client,
		registry,
		schema: "folven",
		transaction: { maxWait: 2_000, timeout: 5_000 },
	});
}

export function renameExamplePost(
	runtime: ReturnType<typeof createExampleRuntime>,
	input: EventInput<typeof renamePost>,
) {
	// Inferred Promise<{ id: string; title: string }>, returned after commit.
	return runtime.dispatch(renamePost, input);
}

export function readExampleAuthor(
	runtime: ReturnType<typeof createExampleRuntime>,
	authorId: string,
) {
	// Inferred Promise<{ id: string; name: string } | null>.
	// Even this read-only handler commits an occurrence: dispatch is not a
	// query-store API, and a normal null result is not a rollback request.
	return runtime.dispatch(readAuthor, { authorId });
}

// One illustrative, manually requested handoff—not a delivery service or pump.
// The runtimes may own independent command/journal namespaces and clients.
export async function handOffExampleEnvelope(
	source: ReturnType<typeof createExampleRuntime>,
	destination: ReturnType<typeof createExampleRuntime>,
) {
	const page = await source.outbox.readPending({ limit: 1 });
	const envelope = page.envelopes[0];
	if (!envelope) return;
	const result = await destination.ingest(envelope);
	if (result.status === "processed" || result.status === "alreadyProcessed") {
		await source.outbox.acknowledge(envelope);
	}
	// A gap or exception must not acknowledge the source. No retry is installed.
	// A later manual scan starts again, not from a permanent event watermark.
	return result;
}

// Independent occurrences have no predecessor or channel cursor. Restart scans
// each cycle: random UUID ordering is not arrival order or a feed watermark.
export async function handOffExampleOccurrence(
	source: ReturnType<typeof createExampleRuntime>,
	destination: ReturnType<typeof createExampleRuntime>,
) {
	const page = await source.outbox.readPendingOccurrences({ limit: 1 });
	const envelope = page.envelopes[0];
	if (!envelope) return;
	const result = await destination.ingest(envelope);
	if (result.status === "processed" || result.status === "alreadyProcessed") {
		await source.outbox.acknowledge(envelope);
	}
	return result;
}
