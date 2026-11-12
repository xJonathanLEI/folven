import { describe, expect, it } from "vitest";
import { postgresSchemaStatements } from "./admin.js";
import {
	JournalResultError,
	OutgoingResultError,
	PostgresIdentityError,
} from "./errors.js";
import { createOutgoingStorage } from "./outgoing.js";
import { createJournalStorage } from "./storage.js";

describe("journal boundaries", () => {
	it("refuses coercible schema objects without invoking their serialization", () => {
		const schema = {
			toString() {
				throw new Error("Schema coercion must not run.");
			},
		};
		expect(() =>
			// @ts-expect-error JavaScript callers can bypass the string input type.
			postgresSchemaStatements({ schema }),
		).toThrow(PostgresIdentityError);
	});

	it.each(["", "a".repeat(64), 'x"; DROP SCHEMA public; --', "a.b"])(
		"refuses an unsupported schema identifier %j without database access",
		(schema) => {
			expect(() => postgresSchemaStatements({ schema })).toThrow(
				PostgresIdentityError,
			);
			expect(() => createJournalStorage(schema)).toThrow(PostgresIdentityError);
			expect(() => createOutgoingStorage(schema)).toThrow(
				PostgresIdentityError,
			);
		},
	);

	it("refuses malformed or incomplete output batch results", async () => {
		for (const result of [
			[],
			[{ inserted_count: 1 }],
			[{ inserted_count: 0n }],
			[{ inserted_count: 2n }],
			[{ inserted_count: 1n }, { inserted_count: 1n }],
		]) {
			await expect(
				createOutgoingStorage("journal").appendOutputs(
					{ $queryRawUnsafe: () => Promise.resolve(result) },
					{ streamName: "author", streamKey: "{}" },
					[
						{
							target: { streamName: "post", streamKey: "{}" },
							eventName: "comment.add",
							eventVersion: 1,
							payloadFormat: "json-v1",
							encodedInput: "{}",
						},
					],
				),
			).rejects.toThrow(OutgoingResultError);
		}
	});

	it("refuses malformed prior positions and partial origins rather than treating them as empty history", async () => {
		const occurrence = {
			occurrenceId: "3eb35e18-ae64-4d47-8b90-b71f8504d782",
			streamName: "author",
			streamKey: "{}",
			eventName: "rename",
			eventVersion: 1,
			payloadFormat: "json-v1" as const,
			encodedInput: "{}",
			incoming: {
				source: { streamName: "post", streamKey: "{}" },
				channelSequence: 1n,
			},
		};
		const journal = createJournalStorage("journal");
		for (const result of [
			[{ stream_sequence: 1n }],
			[{ stream_sequence: 1n, previous_incoming_sequence: null }],
			[{ stream_sequence: 1n, previous_incoming_sequence: 1 }],
			[{ stream_sequence: 1n, previous_incoming_sequence: "1" }],
			[{ stream_sequence: 1n, previous_incoming_sequence: -1n }],
			[
				{
					stream_sequence: 1n,
					previous_incoming_sequence: 9_223_372_036_854_775_808n,
				},
			],
		]) {
			await expect(
				journal.append(
					{ $queryRawUnsafe: () => Promise.resolve(result) },
					occurrence,
				),
			).rejects.toThrow(JournalResultError);
		}
		await expect(
			journal.matches(
				{
					$queryRawUnsafe: () =>
						Promise.resolve([
							{
								occurrence_id: occurrence.occurrenceId,
								stream_name: "author",
								stream_key: "{}",
								event_name: "rename",
								event_version: 1n,
								payload_format: "json-v1",
								payload: "{}",
								incoming_source_stream_name: "post",
								incoming_source_stream_key: null,
								incoming_channel_sequence: 1n,
							},
						]),
				},
				occurrence,
			),
		).rejects.toThrow(JournalResultError);
	});

	it.each(
		[
			[],
			[{ previous_incoming_sequence: 0n }],
			[{ stream_sequence: 1, previous_incoming_sequence: 0n }],
			[{ stream_sequence: "1", previous_incoming_sequence: 0n }],
			[{ stream_sequence: 0n, previous_incoming_sequence: 0n }],
			[{ stream_sequence: -1n, previous_incoming_sequence: 0n }],
			[
				{
					stream_sequence: 9_223_372_036_854_775_808n,
					previous_incoming_sequence: 0n,
				},
			],
			[
				{ stream_sequence: 1n, previous_incoming_sequence: 0n },
				{ stream_sequence: 2n, previous_incoming_sequence: 0n },
			],
			[{ stream_sequence: 1n, previous_incoming_sequence: 0n, extra: true }],
		].map((result) => ({ result })),
	)("refuses malformed reservation results %#", async ({ result }) => {
		// This malformed external result is intentional, not a substitute for
		// the real Prisma compatibility and PostgreSQL storage tests.
		await expect(
			createJournalStorage("journal").append(
				{ $queryRawUnsafe: () => Promise.resolve(result) },
				{
					occurrenceId: "3eb35e18-ae64-4d47-8b90-b71f8504d782",
					streamName: "authors",
					streamKey: "{}",
					eventName: "rename",
					eventVersion: 1,
					payloadFormat: "json-v1",
					encodedInput: "{}",
				},
			),
		).rejects.toThrow(JournalResultError);
	});
});
