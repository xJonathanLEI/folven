import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import { channelOccurrenceId } from "./channel.js";
import {
	createPostgresRuntime,
	OccurrenceIdentityError,
	type IngestionResult,
	type OutgoingEnvelope,
	type PendingOccurrenceEnvelope,
} from "./index.js";
import { prepareIncomingOccurrence } from "./occurrence.js";
import { createJournalStorage } from "./storage.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const comments = definitions.stream({
	name: "comment",
	key: z.strictObject({ postId: z.string(), number: z.int() }),
	owns: {
		comment: {
			kind: "unique",
			selector: "postId_number",
			fields: { postId: "postId", number: "number" },
		},
	},
});
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const schema = z.strictObject({
	postId: z.string(),
	number: z.int(),
	body: z.string().trim(),
	at: z.iso.datetime().transform((value) => new Date(value)),
	mode: z.enum(["accept", "reject", "throw"]).default("accept"),
});
const add = defineEvent({
	name: "comment.add",
	version: 1,
	stream: comments,
	schema,
	route: (payload) => ({ postId: payload.postId, number: payload.number }),
});
const addV2 = defineEvent({ ...add, version: 2 });
const edit = defineEvent({ ...add, name: "comment.edit" });
const notify = defineEvent({
	name: "author.notify",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), body: z.string() }),
	route: (payload) => ({ id: payload.id }),
});
const input = {
	postId: "p",
	number: 1,
	body: " First ",
	at: "2026-01-01T00:00:00.000Z",
};
function envelope(
	sequence: string,
	value: z.input<typeof schema> = input,
	source = "a",
): OutgoingEnvelope {
	return {
		source: { streamName: "author", streamKey: JSON.stringify({ id: source }) },
		target: {
			streamName: "comment",
			streamKey: JSON.stringify({ number: value.number, postId: value.postId }),
		},
		channelSequence: sequence,
		eventName: add.name,
		eventVersion: add.version,
		payloadFormat: "json-v1",
		encodedInput: JSON.stringify(value),
	};
}
const options = { timeout: 12_000, maxWait: 5_000 };
const failure = new Error("Comment rejected with an exception");
function commentHandler(beforeCommit = async (_body: string) => {}) {
	return defineHandler({
		event: add,
		handle: async ({ db, scope, event, emit }) => {
			expect(event.at).toEqual(new Date(input.at));
			if (event.mode === "reject") return { rejected: () => 1n };
			const previous = await db.comment.findUnique({
				where: scope.comment.unique,
			});
			await db.comment.upsert({
				where: scope.comment.unique,
				create: { ...scope.comment.fields, authorId: "a", body: event.body },
				update: { body: `${previous?.body ?? ""}|${event.body}` },
			});
			emit(notify, { id: "a", body: event.body });
			await beforeCommit(event.body);
			if (event.mode === "throw") throw failure;
			return { accepted: () => 2n };
		},
	});
}
function registry(handler = commentHandler()) {
	return definitions.registry({
		streams: [comments, author],
		events: [add, addV2, edit, notify],
		handlers: [
			handler,
			defineHandler({ event: addV2, handle: handler.handle }),
			defineHandler({ event: edit, handle: handler.handle }),
		],
	});
}
async function seed(client: Prisma.TransactionClient) {
	await client.user.create({
		data: { id: "a", email: "author@example.test", name: "Author" },
	});
	await client.post.create({
		data: {
			id: "p",
			authorId: "a",
			slug: "post",
			title: "Post",
			published: true,
			views: 0,
			createdAt: new Date(input.at),
			tags: [],
		},
	});
}

test("independent occurrences take destination acceptance order, retain exact null-origin evidence and emit ordinary channel work", async ({
	database,
}) => {
	const client = database.client("destination");
	await seed(client);
	const runtime = createPostgresRuntime({
		client,
		registry: registry(),
		schema: database.journalSchema,
	});
	const first = {
		occurrenceId: "11111111-1111-4111-8111-111111111111",
		target: envelope("1").target,
		eventName: add.name,
		eventVersion: add.version,
		payloadFormat: "json-v1",
		encodedInput: JSON.stringify(input),
	} satisfies PendingOccurrenceEnvelope;
	const second = {
		...first,
		occurrenceId: "22222222-2222-4222-8222-222222222222",
		encodedInput: JSON.stringify({ ...input, body: " Second " }),
	} satisfies PendingOccurrenceEnvelope;
	const journalQueries: string[] = [];
	client.$on("query", ({ query }) => {
		if (query.includes(`"${database.journalSchema}"."event_log"`))
			journalQueries.push(query);
	});
	// Neither UUID order nor the order in which these envelopes were created
	// reserves destination positions. B is accepted before A without a gap.
	for (const value of [second, first]) {
		journalQueries.length = 0;
		expect(await runtime.ingest(value)).toEqual({ status: "processed" });
		expect(journalQueries).toHaveLength(1);
		expect(journalQueries[0]).toMatch(/^INSERT INTO/);
		expect(journalQueries[0]).not.toContain("MAX(incoming_channel_sequence)");
	}
	for (const value of [first, second]) {
		expect(await runtime.ingest(value)).toEqual({ status: "alreadyProcessed" });
	}
	for (const conflict of [
		{ ...first, encodedInput: JSON.stringify({ ...input, body: "First" }) },
		{ ...first, eventName: edit.name },
		{ ...first, eventVersion: 2 },
		{
			...first,
			target: { ...first.target, streamKey: '{"number":2,"postId":"p"}' },
			encodedInput: JSON.stringify({ ...input, number: 2 }),
		},
	]) {
		await expect(runtime.ingest(conflict)).rejects.toBeInstanceOf(
			OccurrenceIdentityError,
		);
	}
	const rejected = {
		...first,
		occurrenceId: "33333333-3333-4333-8333-333333333333",
		encodedInput: JSON.stringify({ ...input, mode: "reject" }),
	} satisfies PendingOccurrenceEnvelope;
	await expect(
		runtime.ingest({
			...rejected,
			encodedInput: JSON.stringify({ ...input, mode: "throw" }),
		}),
	).rejects.toBe(failure);
	// The throwing attempt mutated and emitted, but left no evidence. A normal
	// rejection at the same UUID now commits without storing its non-JSON result.
	expect(await runtime.ingest(rejected)).toEqual({ status: "processed" });
	expect(await runtime.ingest(rejected)).toEqual({
		status: "alreadyProcessed",
	});
	expect(await client.comment.findMany({ select: { body: true } })).toEqual([
		{ body: "Second|First" },
	]);
	expect(
		(
			await database.admin.query(
				`SELECT occurrence_id, stream_sequence, payload, incoming_source_stream_name, incoming_source_stream_key, incoming_channel_sequence FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
			)
		).rows,
	).toEqual(
		[second, first, rejected].map((value, index) => ({
			occurrence_id: value.occurrenceId,
			stream_sequence: String(index + 1),
			payload: value.encodedInput,
			incoming_source_stream_name: null,
			incoming_source_stream_key: null,
			incoming_channel_sequence: null,
		})),
	);
	expect(
		(
			await database.admin.query(
				`SELECT source_stream_name, source_stream_key, target_stream_name, channel_sequence, payload FROM "${database.journalSchema}".outgoing_event ORDER BY channel_sequence`,
			)
		).rows,
	).toEqual(
		["Second", "First"].map((body, index) => ({
			source_stream_name: "comment",
			source_stream_key: first.target.streamKey,
			target_stream_name: "author",
			channel_sequence: String(index + 1),
			payload: JSON.stringify({ id: "a", body }),
		})),
	);
	// The exact input/target alone is insufficient for duplicate evidence:
	// a channel occurrence with the same UUID has a different incoming origin.
	const channel = envelope("1", { ...input, mode: "reject" });
	expect(await runtime.ingest(channel)).toEqual({ status: "processed" });
	await expect(
		runtime.ingest({
			...rejected,
			occurrenceId: channelOccurrenceId(channel),
		}),
	).rejects.toBeInstanceOf(OccurrenceIdentityError);
});

test("ingestion rolls gaps back, consumes normal rejections, and detects conflicting or missing retained history", async ({
	database,
}) => {
	const client = database.client("destination");
	await seed(client);
	const handled = vi.fn<() => void>();
	const implementation = commentHandler();
	const handler = defineHandler({
		event: add,
		handle(context) {
			handled();
			return implementation.handle(context);
		},
	});
	const runtime = createPostgresRuntime({
		client,
		registry: registry(handler),
		schema: database.journalSchema,
	});
	const journalQueries: string[] = [];
	client.$on("query", ({ query }) => {
		if (query.includes(`"${database.journalSchema}"."event_log"`))
			journalQueries.push(query);
	});
	async function ingestOnce(value: OutgoingEnvelope) {
		journalQueries.length = 0;
		const result = await runtime.ingest(value);
		// Count actual journal SQL executions, not transaction control, handler
		// queries or outgoing persistence. Duplicates/retries have separate costs.
		expect(journalQueries).toHaveLength(1);
		expect(journalQueries[0]).toMatch(/^WITH appended AS \(\nINSERT INTO/);
		expect(journalQueries[0]).toContain("MAX(incoming_channel_sequence)");
		return result;
	}
	const first = envelope("1");
	const second = envelope("2", { ...input, mode: "reject" });
	expect(await ingestOnce(second)).toEqual({
		status: "gap",
		expectedSequence: "1",
	});
	expect(handled).not.toHaveBeenCalled();
	expect(await client.comment.count()).toBe(0);
	for (const table of ["event_log", "outgoing_event"]) {
		expect(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}"."${table}"`,
				)
			).rows,
		).toEqual([]);
	}
	expect(await ingestOnce(first)).toEqual({ status: "processed" });
	expect(await ingestOnce(second)).toEqual({ status: "processed" });
	expect(await runtime.ingest(first)).toEqual({ status: "alreadyProcessed" });
	for (const conflict of [
		{ ...first, encodedInput: JSON.stringify({ ...input, body: "First" }) },
		{ ...first, eventVersion: 2 },
		{ ...first, eventName: edit.name },
	]) {
		await expect(runtime.ingest(conflict)).rejects.toBeInstanceOf(
			OccurrenceIdentityError,
		);
	}
	await expect(
		runtime.ingest(envelope("3", { ...input, mode: "throw" })),
	).rejects.toBe(failure);
	expect(await ingestOnce(envelope("4"))).toEqual({
		status: "gap",
		expectedSequence: "3",
	});
	expect(await ingestOnce(envelope("3", { ...input, body: "Third" }))).toEqual({
		status: "processed",
	});
	// Public dispatch shares destination ordering but never advances a channel.
	journalQueries.length = 0;
	await runtime.dispatch(add, { ...input, mode: "reject" });
	expect(journalQueries).toHaveLength(1);
	expect(journalQueries[0]).toMatch(/^INSERT INTO/);
	expect(journalQueries[0]).not.toContain("MAX(incoming_channel_sequence)");
	expect(await client.comment.findMany({ select: { body: true } })).toEqual([
		{ body: "First|Third" },
	]);
	expect(
		(
			await database.admin.query(
				`SELECT stream_sequence, incoming_channel_sequence, payload FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
			)
		).rows,
	).toEqual([
		{
			stream_sequence: "1",
			incoming_channel_sequence: "1",
			payload: first.encodedInput,
		},
		{
			stream_sequence: "2",
			incoming_channel_sequence: "2",
			payload: second.encodedInput,
		},
		{
			stream_sequence: "3",
			incoming_channel_sequence: "3",
			payload: JSON.stringify({ ...input, body: "Third" }),
		},
		{
			stream_sequence: "4",
			incoming_channel_sequence: null,
			payload: JSON.stringify({ ...input, mode: "reject" }),
		},
	]);
	expect(
		(
			await database.admin.query(
				`SELECT source_stream_name, source_stream_key, target_stream_name, channel_sequence, payload FROM "${database.journalSchema}".outgoing_event ORDER BY channel_sequence`,
			)
		).rows,
	).toEqual(
		["First", "Third"].map((body, index) => ({
			source_stream_name: "comment",
			source_stream_key: first.target.streamKey,
			target_stream_name: "author",
			channel_sequence: String(index + 1),
			payload: JSON.stringify({ id: "a", body }),
		})),
	);
	// The follow-up event has no local handler and has not changed author state.
	expect(
		await client.user.findUnique({
			where: { id: "a" },
			select: { name: true },
		}),
	).toEqual({ name: "Author" });
	// Deliberate corruption, not supported retention: progress alone cannot
	// stand in for the missing exact duplicate record.
	await database.admin.query(
		`DELETE FROM "${database.journalSchema}".event_log WHERE incoming_channel_sequence = 2`,
	);
	await expect(runtime.ingest(second)).rejects.toBeInstanceOf(
		OccurrenceIdentityError,
	);
	expect(
		(
			await database.admin.query(
				`SELECT stream_sequence FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
			)
		).rows,
	).toEqual([
		{ stream_sequence: "1" },
		{ stream_sequence: "3" },
		{ stream_sequence: "4" },
	]);
});

test.for(["duplicate", "next", "other-channel"] as const)(
	"concurrent %s ingestion uses the destination fence while other destination streams progress",
	async (kind, { database }) => {
		const firstClient = database.client("first");
		const secondClient = database.client("second");
		const independentClient = database.client("independent");
		await seed(firstClient);
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const handler = commentHandler(async (body) => {
			if (body === "First") {
				entered.resolve();
				await release.promise;
			}
		});
		const registered = registry(handler);
		const runtime = (client: typeof firstClient) =>
			createPostgresRuntime({
				client,
				registry: registered,
				schema: database.journalSchema,
				transaction: options,
			});
		const first = settle(runtime(firstClient).ingest(envelope("1")));
		let contender: ReturnType<typeof settle<IngestionResult>> | undefined;
		try {
			await bounded(entered.promise);
			contender = settle(
				runtime(secondClient).ingest(
					kind === "duplicate"
						? envelope("1")
						: envelope(
								kind === "next" ? "2" : "1",
								{ ...input, body: "Second" },
								kind === "next" ? "a" : "b",
							),
				),
			);
			await database.waitForBlock(
				"first",
				"second",
				`WITH appended AS (\nINSERT INTO "${database.journalSchema}"."event_log"`,
			);
			expect(
				await bounded(
					runtime(independentClient).ingest(
						envelope("1", { ...input, number: 2, body: "Independent" }),
					),
				),
			).toEqual({ status: "processed" });
			release.resolve();
			const firstResult = await bounded(first);
			const secondResult = await bounded(contender);
			if (kind === "duplicate") {
				// Either invocation may win after a serialization abort; exactly
				// one commits, and the other acknowledges that same occurrence.
				expect([firstResult, secondResult]).toEqual(
					expect.arrayContaining([
						{ status: "fulfilled", value: { status: "processed" } },
						{ status: "fulfilled", value: { status: "alreadyProcessed" } },
					]),
				);
			} else {
				expect(firstResult).toEqual({
					status: "fulfilled",
					value: { status: "processed" },
				});
				if (
					kind === "next" &&
					secondResult.status === "fulfilled" &&
					secondResult.value.status === "gap"
				) {
					// If the first callback aborts, sequence two may observe the
					// gap before its retry commits. Only explicit redelivery advances it.
					expect(secondResult.value).toEqual({
						status: "gap",
						expectedSequence: "1",
					});
					expect(
						await runtime(secondClient).ingest(
							envelope("2", { ...input, body: "Second" }),
						),
					).toEqual({ status: "processed" });
				} else {
					expect(secondResult).toEqual({
						status: "fulfilled",
						value: { status: "processed" },
					});
				}
			}
			const rows = z
				.array(
					z.strictObject({
						stream_sequence: z.string(),
						incoming_source_stream_key: z.string(),
						incoming_channel_sequence: z.string(),
					}),
				)
				.parse(
					(
						await database.admin.query(
							`SELECT stream_sequence, incoming_source_stream_key, incoming_channel_sequence FROM "${database.journalSchema}".event_log WHERE stream_key = $1 ORDER BY stream_sequence`,
							[envelope("1").target.streamKey],
						)
					).rows,
				);
			// The independent transaction can force a serialization retry of
			// the first callback. Different channels have no predetermined order;
			// their committed effects must agree with destination journal order.
			const reversed =
				kind === "other-channel" &&
				rows[0]?.incoming_source_stream_key === '{"id":"b"}';
			expect(
				await firstClient.comment.findUnique({
					where: { postId_number: { postId: "p", number: 1 } },
					select: { body: true },
				}),
			).toEqual({
				body:
					kind === "duplicate"
						? "First"
						: reversed
							? "Second|First"
							: "First|Second",
			});
			const count = kind === "duplicate" ? 1 : 2;
			expect(rows).toEqual([
				{
					stream_sequence: "1",
					incoming_source_stream_key: reversed ? '{"id":"b"}' : '{"id":"a"}',
					incoming_channel_sequence: "1",
				},
				...(kind === "duplicate"
					? []
					: [
							{
								stream_sequence: "2",
								incoming_source_stream_key:
									kind === "next" || reversed ? '{"id":"a"}' : '{"id":"b"}',
								incoming_channel_sequence: kind === "next" ? "2" : "1",
							},
						]),
			]);
			expect(
				(
					await database.admin.query(
						`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event WHERE source_stream_key = $1 ORDER BY channel_sequence`,
						[envelope("1").target.streamKey],
					)
				).rows,
			).toEqual(
				Array.from({ length: count }, (_, index) => ({
					channel_sequence: String(index + 1),
				})),
			);
		} finally {
			release.resolve();
			await first;
			if (contender) await contender;
		}
	},
);

test("emitted bytes and compound routing survive ingestion above the JavaScript safe integer limit", async ({
	database,
}) => {
	const sourceClient = database.client("source");
	const destinationClient = database.client("destination");
	await seed(sourceClient);
	const sourceHandler = defineHandler({
		event: notify,
		handle: ({ emit }) => emit(add, input),
	});
	const source = createPostgresRuntime({
		client: sourceClient,
		schema: database.otherJournalSchema,
		registry: definitions.registry({
			streams: [author, comments],
			events: [notify, add],
			handlers: [sourceHandler],
		}),
	});
	await source.dispatch(notify, { id: "a", body: "publish" });
	// Seed retained high-water history rather than executing 2^53 transitions.
	const previous = "9007199254740992";
	await database.admin.query(
		`UPDATE "${database.otherJournalSchema}".outgoing_event SET channel_sequence = $1::bigint`,
		[previous],
	);
	const handler = commentHandler();
	const prepared = prepareIncomingOccurrence(handler, envelope(previous));
	await destinationClient.$transaction((tx) =>
		createJournalStorage(database.journalSchema).append(
			tx,
			prepared.occurrence,
		),
	);
	await source.outbox.acknowledge(envelope(previous));
	await source.dispatch(notify, { id: "a", body: "publish again" });
	const page = await source.outbox.readPending();
	expect(page.envelopes).toHaveLength(1);
	const stored = page.envelopes[0];
	if (!stored) throw new Error("Expected the next pending envelope");
	expect(stored).toEqual(envelope("9007199254740993"));
	const destination = createPostgresRuntime({
		client: destinationClient,
		registry: registry(handler),
		schema: database.journalSchema,
	});
	expect(await destination.ingest(envelope("9007199254740994"))).toEqual({
		status: "gap",
		expectedSequence: "9007199254740993",
	});
	expect(await destination.ingest(stored)).toEqual({ status: "processed" });
	expect(await destination.ingest(stored)).toEqual({
		status: "alreadyProcessed",
	});
	await source.outbox.acknowledge(stored);
	expect((await source.outbox.readPending()).envelopes).toEqual([]);
	expect(
		await destinationClient.comment.findMany({ select: { body: true } }),
	).toEqual([{ body: "First" }]);
	expect(
		(
			await database.admin.query(
				`SELECT incoming_channel_sequence, payload FROM "${database.journalSchema}".event_log ORDER BY stream_sequence DESC LIMIT 1`,
			)
		).rows,
	).toEqual([
		{
			incoming_channel_sequence: stored.channelSequence,
			payload: stored.encodedInput,
		},
	]);
});
