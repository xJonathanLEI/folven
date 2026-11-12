import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent } from "../index.js";
import {
	createPostgresPublisher,
	createPostgresRuntime,
	PendingOccurrenceIdentityError,
	PendingOccurrenceResultError,
	type PostgresBorrowedTransaction,
	type PostgresPublication,
	type StagedPendingOccurrences,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const timestamp = defineEvent({
	name: "author.timestamp",
	version: 1,
	stream: author,
	schema: z.object({
		id: z.string().trim(),
		at: z.iso.datetime().transform((value) => new Date(value)),
	}),
	route: ({ id }) => ({ id }),
});
const registry = definitions.registry({
	streams: [author],
	events: [timestamp],
	handlers: [],
});
const input = { id: " a ", at: "2026-01-01T00:00:00Z", extra: "kept" };
const transactionOptions = { timeout: 12_000, maxWait: 5_000 };
async function consume(transaction: Prisma.TransactionClient, id: string) {
	const { count } = await transaction.webhook.updateMany({
		where: { id, handedOff: false },
		data: { handedOff: true },
	});
	return count === 1;
}

/** Narrow SQL-composition fixture, not a scheduler or Bellows substitute. */
function publication(
	schema: string,
	notify: () => Promise<void>,
): PostgresPublication {
	return {
		prepare({ source, parameterOffset }) {
			return {
				sql: `INSERT INTO "${schema}".delivery_tasks (name, payload)
SELECT $${parameterOffset + 1}, "${source.payloadColumn}" FROM "${source.relation}"
RETURNING id::text AS id`,
				parameters: ["deliver"],
				validate(rows, count) {
					const receipts = z
						.array(z.strictObject({ id: z.string().regex(/^[1-9][0-9]*$/) }))
						.parse(rows);
					if (
						receipts.length !== count ||
						new Set(receipts.map(({ id }) => id)).size !== count
					) {
						throw new Error("Invalid test publication receipts.");
					}
					return notify;
				},
			};
		},
	};
}

test("source handoff and pending work are invisible before commit, roll back together, and reuse prepared identity after a known abort", async ({
	database,
}) => {
	const client = database.client("owner");
	const observer = database.client("observer");
	await client.webhook.create({ data: { id: "webhook" } });
	const publisher = createPostgresPublisher({
		registry,
		schema: database.journalSchema,
	});
	const runtime = createPostgresRuntime({
		client: observer,
		registry,
		schema: database.journalSchema,
	});
	const value = { ...input };
	const prepared = publisher.prepare(timestamp, value);
	value.id = "mutated";
	const entered = Promise.withResolvers<StagedPendingOccurrences>();
	const release = Promise.withResolvers<void>();
	const failure = new Error("known rollback");
	const attempt = settle(
		client.$transaction(async (transaction) => {
			expect(await consume(transaction, "webhook")).toBe(true);
			const staged = await publisher.stage(transaction, [prepared]);
			expect(staged.references).toHaveLength(1);
			entered.resolve(staged);
			await bounded(release.promise);
			throw failure;
		}, transactionOptions),
	);
	try {
		await bounded(entered.promise);
		expect(
			await observer.webhook.findUnique({ where: { id: "webhook" } }),
		).toMatchObject({ handedOff: false });
		expect((await runtime.outbox.readPendingOccurrences()).envelopes).toEqual(
			[],
		);
		expect(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([]);
	} finally {
		release.resolve();
		await attempt;
	}
	expect(await attempt).toEqual({ status: "rejected", reason: failure });
	expect((await runtime.outbox.readPendingOccurrences()).envelopes).toEqual([]);
	expect(
		await observer.webhook.findUnique({ where: { id: "webhook" } }),
	).toMatchObject({ handedOff: false });
	const queries: string[] = [];
	client.$on("query", ({ query }) => queries.push(query));
	const staged = await client.$transaction(async (transaction) => {
		expect(await consume(transaction, "webhook")).toBe(true);
		return publisher.stage(transaction, [prepared]);
	});
	await staged.notifyCommitted();
	expect(staged.references).toEqual((await entered.promise).references);
	expect(
		queries.filter((query) => query.includes('"pending_occurrence"')),
	).toHaveLength(1);
	expect(
		await observer.webhook.findUnique({ where: { id: "webhook" } }),
	).toMatchObject({ handedOff: true });
	const reference = staged.references[0];
	expect(reference).toBeDefined();
	if (!reference) throw new Error("Missing staged reference");
	const retained = await runtime.outbox.read(reference);
	expect(retained).toEqual({
		acknowledged: false,
		envelope: {
			...reference,
			target: { streamName: "author", streamKey: '{"id":"a"}' },
			eventName: timestamp.name,
			eventVersion: 1,
			payloadFormat: "json-v1",
			encodedInput: JSON.stringify(input),
		},
	});
	expect(
		(
			await database.admin.query(
				`SELECT * FROM "${database.journalSchema}".event_log`,
			)
		).rows,
	).toEqual([]);
	// Committed restaging must fail, not hide a source-consumption mistake.
	await expect(
		client.$transaction((transaction) =>
			publisher.stage(transaction, [prepared]),
		),
	).rejects.toThrow();
	expect(
		(await runtime.outbox.readPendingOccurrences()).envelopes,
	).toHaveLength(1);
	expect(await client.webhook.count()).toBe(1);
});

test("the caller's transactional guard gives one handoff to two racing consumers; equal payloads are not source deduplication", async ({
	database,
}) => {
	const firstClient = database.client("first");
	const secondClient = database.client("second");
	await firstClient.webhook.createMany({
		data: [{ id: "one" }, { id: "two" }],
	});
	const publisher = createPostgresPublisher({
		registry,
		schema: database.journalSchema,
	});
	const firstPrepared = publisher.prepare(timestamp, input);
	const secondPrepared = publisher.prepare(timestamp, input);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const first = settle(
		firstClient.$transaction(async (transaction) => {
			expect(await consume(transaction, "one")).toBe(true);
			const result = await publisher.stage(transaction, [firstPrepared]);
			entered.resolve();
			await bounded(release.promise);
			return result;
		}, transactionOptions),
	);
	const second = settle(
		(async () => {
			await bounded(entered.promise);
			return secondClient.$transaction(
				async (transaction) =>
					(await consume(transaction, "one"))
						? publisher.stage(transaction, [secondPrepared])
						: null,
				transactionOptions,
			);
		})(),
	);
	try {
		await bounded(entered.promise);
		await database.waitForBlock("first", "second", "UPDATE");
	} finally {
		release.resolve();
		await Promise.all([first, second]);
	}
	expect(await first).toMatchObject({
		status: "fulfilled",
		value: { references: [expect.anything()] },
	});
	expect(await second).toEqual({ status: "fulfilled", value: null });
	const next = await secondClient.$transaction(async (transaction) =>
		(await consume(transaction, "two"))
			? publisher.stage(transaction, [secondPrepared])
			: null,
	);
	expect(next?.references).toHaveLength(1);
	const runtime = createPostgresRuntime({
		client: firstClient,
		registry,
		schema: database.journalSchema,
	});
	const rows = (await runtime.outbox.readPendingOccurrences()).envelopes;
	expect(rows).toHaveLength(2);
	expect(new Set(rows.map(({ occurrenceId }) => occurrenceId)).size).toBe(2);
	expect(rows.map(({ encodedInput }) => encodedInput)).toEqual([
		JSON.stringify(input),
		JSON.stringify(input),
	]);
});

test("malformed returned references or publication receipts escape staging and roll back the caller's source write, pending rows and tasks", async ({
	database,
}) => {
	const client = database.client("owner");
	await client.webhook.create({ data: { id: "webhook" } });
	await database.admin
		.query(`CREATE TABLE "${database.journalSchema}".delivery_tasks (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL, payload text NOT NULL)`);
	const notify = vi.fn(async () => {});
	const publisher = createPostgresPublisher({
		registry,
		schema: database.journalSchema,
		publication: publication(database.journalSchema, notify),
	});
	const prepared = publisher.prepare(timestamp, input);
	for (const field of ["references", "publications"]) {
		await expect(
			client.$transaction(async (transaction) => {
				expect(await consume(transaction, "webhook")).toBe(true);
				// Labeled lost/corrupt result double: real SQL executes in the same
				// transaction, only its returned control-plane result is changed.
				const corrupt: PostgresBorrowedTransaction = {
					async $queryRawUnsafe(sql, ...parameters) {
						const rows = z
							.tuple([
								z.strictObject({
									references: z.array(z.unknown()),
									publications: z.array(z.unknown()),
								}),
							])
							.parse(await transaction.$queryRawUnsafe(sql, ...parameters));
						return [{ ...rows[0], [field]: [] }];
					},
				};
				return publisher.stage(corrupt, [prepared]);
			}),
		).rejects.toThrow(
			field === "references" ? PendingOccurrenceResultError : Error,
		);
		expect(
			await client.webhook.findUnique({ where: { id: "webhook" } }),
		).toMatchObject({ handedOff: false });
		expect(
			(
				await database.admin.query(`SELECT
(SELECT count(*)::int FROM "${database.journalSchema}".pending_occurrence) AS pending,
(SELECT count(*)::int FROM "${database.journalSchema}".delivery_tasks) AS tasks`)
			).rows,
		).toEqual([{ pending: 0, tasks: 0 }]);
		expect(notify).not.toHaveBeenCalled();
	}
	const committed = await client.$transaction(async (transaction) => {
		expect(await consume(transaction, "webhook")).toBe(true);
		return publisher.stage(transaction, [prepared]);
	});
	await committed.notifyCommitted();
	expect(notify).toHaveBeenCalledTimes(1);
	expect(await client.webhook.count()).toBe(1);
});

test("exact independent lookup and acknowledgment need no current definitions; UUID pages are isolated, retained and restartable", async ({
	database,
}) => {
	const client = database.client("source");
	const publisher = createPostgresPublisher({
		registry,
		schema: database.journalSchema,
	});
	const batch = Array.from({ length: 3 }, () =>
		publisher.prepare(timestamp, input),
	);
	const staged = await client.$transaction((transaction) =>
		publisher.stage(transaction, batch),
	);
	const emptyRegistry = definitions.registry({
		streams: [author],
		events: [],
		handlers: [],
	});
	const runtime = createPostgresRuntime({
		client,
		registry: emptyRegistry,
		schema: database.journalSchema,
	});
	const other = createPostgresRuntime({
		client,
		registry: emptyRegistry,
		schema: database.otherJournalSchema,
	});
	const sorted = [...staged.references].sort((a, b) =>
		a.occurrenceId.localeCompare(b.occurrenceId),
	);
	const first = await runtime.outbox.readPendingOccurrences({ limit: 2 });
	expect(first.envelopes.map(({ occurrenceId }) => occurrenceId)).toEqual(
		sorted.slice(0, 2).map(({ occurrenceId }) => occurrenceId),
	);
	expect(first.nextCursor).toBe(sorted[1]?.occurrenceId);
	if (!first.nextCursor) throw new Error("Missing page cursor");
	const last = await runtime.outbox.readPendingOccurrences({
		limit: 2,
		after: first.nextCursor,
	});
	expect(last.envelopes.map(({ occurrenceId }) => occurrenceId)).toEqual(
		sorted.slice(2).map(({ occurrenceId }) => occurrenceId),
	);
	expect(last.nextCursor).toBeNull();
	expect((await runtime.outbox.readPending()).envelopes).toEqual([]);
	expect((await other.outbox.readPendingOccurrences()).envelopes).toEqual([]);
	const envelope = first.envelopes[0];
	if (!envelope) throw new Error("Missing pending envelope");
	const reference = { occurrenceId: envelope.occurrenceId };
	await expect(other.outbox.read(reference)).rejects.toBeInstanceOf(
		PendingOccurrenceIdentityError,
	);
	for (const changed of [
		{ ...envelope, eventName: "different" },
		{ ...envelope, eventVersion: 2 },
		{
			...envelope,
			encodedInput: JSON.stringify({ ...input, extra: "changed" }),
		},
		{
			...envelope,
			target: { streamName: "author", streamKey: '{"id":"other"}' },
		},
	]) {
		await expect(runtime.outbox.acknowledge(changed)).rejects.toBeInstanceOf(
			PendingOccurrenceIdentityError,
		);
	}
	await runtime.outbox.acknowledge(envelope);
	await runtime.outbox.acknowledge(envelope);
	expect(await runtime.outbox.read(reference)).toEqual({
		envelope,
		acknowledged: true,
	});
	expect(
		(await runtime.outbox.readPendingOccurrences()).envelopes,
	).toHaveLength(2);
});
