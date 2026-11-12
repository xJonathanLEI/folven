import { randomUUID } from "node:crypto";
import { expect } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { test } from "../../tests/postgres.js";
import { classifyFailure } from "./failures.js";
import { createJournalStorage } from "./storage.js";
import type { JournalOccurrence, PostgresTransactionHost } from "./types.js";

function occurrence(
	overrides: Partial<JournalOccurrence> = {},
): JournalOccurrence {
	return {
		occurrenceId: randomUUID(),
		streamName: "author",
		streamKey: '{"authorId":"author-1"}',
		eventName: "post.rename",
		eventVersion: 1,
		payloadFormat: "json-v1",
		encodedInput: '{"title":"Renamed"}',
		...overrides,
	};
}

function transaction<Result>(
	client: PostgresTransactionHost<Prisma.TransactionClient>,
	run: (tx: Prisma.TransactionClient) => Promise<Result>,
) {
	return client.$transaction(run, {
		isolationLevel: "Serializable",
		maxWait: 5_000,
		timeout: 12_000,
	});
}

test("reserves once per append, preserves exact input, and isolates metadata schemas", async ({
	database,
}) => {
	const client = database.client("append");
	const journal = createJournalStorage(database.journalSchema);
	const queries: string[] = [];
	client.$on("query", (event) => queries.push(event.query));
	const input = occurrence({
		streamName: 'authors"; DROP TABLE users; --',
		streamKey: '{"id":"\'; SELECT pg_sleep(10); --"}',
		eventName: "rename'; --",
		eventVersion: Number.MAX_SAFE_INTEGER,
		encodedInput:
			'{"title":"\'; DROP SCHEMA public CASCADE; --","at":"2026-01-01T00:00:00Z"}',
	});
	expect(await transaction(client, (tx) => journal.append(tx, input))).toEqual({
		sequence: 1n,
		previousIncomingSequence: 0n,
	});
	const journalQueries = queries.filter((query) =>
		query.includes('"event_log"'),
	);
	expect(journalQueries).toHaveLength(1);
	expect(journalQueries[0]).toMatch(/^INSERT INTO/);
	expect(journalQueries[0]).toContain("RETURNING stream_sequence");
	expect(journalQueries[0]).toContain(
		"0::bigint AS previous_incoming_sequence",
	);
	expect(journalQueries[0]).not.toContain("MAX(incoming_channel_sequence)");
	expect(journalQueries[0]).not.toContain(input.encodedInput);

	// Event identity/version are not part of the stream-position boundary.
	expect(
		await transaction(client, (tx) =>
			journal.append(tx, {
				...input,
				occurrenceId: randomUUID(),
				eventName: "post.publish",
				eventVersion: 2,
			}),
		),
	).toEqual({ sequence: 2n, previousIncomingSequence: 0n });
	const other = createJournalStorage(database.otherJournalSchema);
	expect(await transaction(client, (tx) => other.append(tx, input))).toEqual({
		sequence: 1n,
		previousIncomingSequence: 0n,
	});
	const rows = z
		.array(
			z.object({
				occurrence_id: z.string(),
				stream_name: z.string(),
				stream_key: z.string(),
				stream_sequence: z.string(),
				event_name: z.string(),
				event_version: z.string(),
				payload_format: z.literal("json-v1"),
				payload: z.string(),
			}),
		)
		.parse(
			(
				await database.admin.query<Record<string, unknown>>(
					`SELECT * FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
				)
			).rows,
		);
	expect(rows[0]).toEqual({
		occurrence_id: input.occurrenceId,
		stream_name: input.streamName,
		stream_key: input.streamKey,
		stream_sequence: "1",
		event_name: input.eventName,
		event_version: String(Number.MAX_SAFE_INTEGER),
		payload_format: "json-v1",
		payload: input.encodedInput,
	});
	expect(rows).toHaveLength(2);
	expect(await client.user.count()).toBe(0);
});

test("matches full incoming identities after rollback and reads prior tails excluding tentative rows", async ({
	database,
}) => {
	const client = database.client("incoming");
	const journal = createJournalStorage(database.journalSchema);
	const first = {
		...occurrence(),
		incoming: {
			source: { streamName: "post", streamKey: '{"id":"p"}' },
			channelSequence: 1n,
		},
	};
	await transaction(client, async (tx) => {
		expect(await journal.append(tx, first)).toEqual({
			sequence: 1n,
			previousIncomingSequence: 0n,
		});
	});
	expect(await journal.matches(client, first)).toBe(true);
	const next = {
		...first,
		occurrenceId: randomUUID(),
		incoming: { ...first.incoming, channelSequence: 9_007_199_254_740_992n },
	};
	await expect(
		transaction(client, async (tx) => {
			expect(await journal.append(tx, next)).toEqual({
				sequence: 2n,
				previousIncomingSequence: 1n,
			});
			// Storage does not admit/reject gaps; the ingestion executor owns that decision.
			throw new Error("Rollback tentative input.");
		}),
	).rejects.toThrow("Rollback tentative input.");
	expect(await journal.matches(client, next)).toBe(false);
	for (const incoming of [
		{
			...first.incoming,
			source: { ...first.incoming.source, streamKey: '{"id":"other"}' },
		},
		{ ...first.incoming, channelSequence: 2n },
	]) {
		expect(await journal.matches(client, { ...first, incoming })).toBe(false);
	}
	for (const override of [
		{ encodedInput: '{"title":"Different"}' },
		{ eventName: "other" },
		{ eventVersion: 2 },
		{ streamKey: '{"authorId":"other"}' },
		{ occurrenceId: randomUUID() },
	]) {
		expect(await journal.matches(client, { ...first, ...override })).toBe(
			false,
		);
	}
	const { incoming: _incoming, ...external } = first;
	expect(await journal.matches(client, external)).toBe(false);
	// Same incoming tuple, different occurrence ID: exercise the new exact index through Prisma.
	try {
		await transaction(client, (tx) =>
			journal.append(tx, { ...first, occurrenceId: randomUUID() }),
		);
		throw new Error("Expected incoming uniqueness failure.");
	} catch (error) {
		expect(classifyFailure(error, "append")).toBe("duplicate");
		expect(classifyFailure(error, "handler")).toBe("other");
		expect(error).toMatchObject({
			code: "P2010",
			meta: {
				driverAdapterError: {
					cause: { constraint: { index: "event_log_incoming_position_key" } },
				},
			},
		});
	}
	// A foreign occurrence ID colliding with this incoming tuple must not pass either lookup.
	const unrelated = occurrence({ streamKey: '{"authorId":"other"}' });
	await transaction(client, (tx) => journal.append(tx, unrelated));
	expect(
		await journal.matches(client, {
			...first,
			occurrenceId: unrelated.occurrenceId,
		}),
	).toBe(false);
	await transaction(client, async (tx) => {
		expect(await journal.append(tx, next)).toEqual({
			sequence: 2n,
			previousIncomingSequence: 1n,
		});
		// A later statement sees this transaction's own earlier append, across
		// event names/versions, without including its own tentative row.
		expect(
			await journal.append(tx, {
				...next,
				occurrenceId: randomUUID(),
				eventName: "post.publish",
				eventVersion: 2,
				incoming: { ...next.incoming, channelSequence: 9_007_199_254_740_993n },
			}),
		).toEqual({
			sequence: 3n,
			previousIncomingSequence: 9_007_199_254_740_992n,
		});
		const otherChannel = {
			...next,
			occurrenceId: randomUUID(),
			incoming: {
				source: { streamName: "post", streamKey: '{"id":"other"}' },
				channelSequence: 1n,
			},
		};
		expect(await journal.append(tx, otherChannel)).toEqual({
			sequence: 4n,
			previousIncomingSequence: 0n,
		});
		// Every component of the directed source/destination tuple matters.
		for (const override of [
			{
				incoming: {
					...first.incoming,
					source: { ...first.incoming.source, streamName: "other-post" },
				},
			},
			{ streamName: "other-author" },
			{ streamKey: '{"authorId":"different"}' },
		]) {
			expect(
				await journal.append(tx, {
					...first,
					...override,
					occurrenceId: randomUUID(),
				}),
			).toEqual({
				sequence: "incoming" in override ? 5n : 1n,
				previousIncomingSequence: 0n,
			});
		}
	});
	const after = {
		...next,
		occurrenceId: randomUUID(),
		incoming: { ...next.incoming, channelSequence: 9_007_199_254_740_994n },
	};
	await transaction(client, async (tx) => {
		expect(await journal.append(tx, after)).toEqual({
			sequence: 6n,
			previousIncomingSequence: 9_007_199_254_740_993n,
		});
	});
});

test("enforces the byte bound in SQL and preserves bigint positions without rounding", async ({
	database,
}) => {
	const client = database.client("bounds");
	const journal = createJournalStorage(database.journalSchema);
	const input = occurrence({ streamName: "a".repeat(1_022), streamKey: "{}" });
	expect(await transaction(client, (tx) => journal.append(tx, input))).toEqual({
		sequence: 1n,
		previousIncomingSequence: 0n,
	});
	await expect(
		transaction(client, (tx) =>
			journal.append(
				tx,
				occurrence({
					streamName: "é".repeat(512),
					streamKey: "{}",
				}),
			),
		),
	).rejects.toMatchObject({
		code: "P2010",
		meta: { driverAdapterError: { cause: { originalCode: "23514" } } },
	});
	// Administrative seed only: normal journal access never rewrites history.
	await database.admin.query(
		`UPDATE "${database.journalSchema}".event_log SET stream_sequence = $1 WHERE occurrence_id = $2`,
		["9007199254740991", input.occurrenceId],
	);
	expect(
		await transaction(client, (tx) =>
			journal.append(tx, {
				...input,
				occurrenceId: randomUUID(),
			}),
		),
	).toEqual({
		sequence: 9_007_199_254_740_992n,
		previousIncomingSequence: 0n,
	});
	await database.admin.query(
		`UPDATE "${database.journalSchema}".event_log SET stream_sequence = $1 WHERE occurrence_id = $2`,
		["9223372036854775807", input.occurrenceId],
	);
	await expect(
		transaction(client, (tx) =>
			journal.append(tx, {
				...input,
				occurrenceId: randomUUID(),
			}),
		),
	).rejects.toMatchObject({
		code: "P2010",
		meta: { driverAdapterError: { cause: { originalCode: "22003" } } },
	});
});
