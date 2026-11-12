import { createHash, randomUUID } from "node:crypto";
import { expect } from "vitest";
import { z } from "zod";
import { test } from "../../tests/postgres.js";
import {
	ChannelSequenceOverflowError,
	OutgoingIdentityError,
} from "./errors.js";
import { classifyFailure } from "./failures.js";
import { createOutgoingStorage } from "./outgoing.js";
import { createJournalStorage } from "./storage.js";
import type {
	CapturedOutput,
	JournalOccurrence,
	StreamIdentity,
} from "./types.js";

const source: StreamIdentity = {
	streamName: "author",
	streamKey: '{"id":"a"}',
};
const x: StreamIdentity = { streamName: "post", streamKey: '{"id":"x"}' };
const y: StreamIdentity = { streamName: "post", streamKey: '{"id":"y"}' };
const options = { isolationLevel: "Serializable" as const };

function output(target = x, version = 1): CapturedOutput {
	return {
		target,
		eventName: version === 1 ? "post.comment" : "post.comment.edited",
		eventVersion: version,
		payloadFormat: "json-v1",
		encodedInput: '{"body":"a\'; --","raw":"2026-01-01T00:00:00Z"}',
	};
}

function occurrence(identity = source): JournalOccurrence {
	return {
		occurrenceId: randomUUID(),
		streamName: identity.streamName,
		streamKey: identity.streamKey,
		eventName: "author.post",
		eventVersion: 1,
		payloadFormat: "json-v1",
		encodedInput: "{}",
	};
}

test("allocates one batch in capture order per channel, retaining acknowledged tails and exact bytes", async ({
	database,
}) => {
	const client = database.client("outgoing");
	const journal = createJournalStorage(database.journalSchema);
	const outgoing = createOutgoingStorage(database.journalSchema);
	const queries: string[] = [];
	client.$on("query", (event) => queries.push(event.query));
	expect(await outgoing.appendOutputs(client, source, [])).toEqual({
		references: [],
		afterCommit: undefined,
	});
	expect(queries).toEqual([]);
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence());
		const { references } = await outgoing.appendOutputs(tx, source, [
			output(x),
			output(y),
			output(x),
			output(x, 2),
		]);
		expect(references).toEqual(
			expect.arrayContaining([
				{ source, target: x, channelSequence: "1" },
				{ source, target: x, channelSequence: "2" },
				{ source, target: x, channelSequence: "3" },
				{ source, target: y, channelSequence: "1" },
			]),
		);
		expect(references).toHaveLength(4);
	}, options);
	const inserts = queries.filter((query) => query.includes('"outgoing_event"'));
	expect(inserts).toHaveLength(1);
	expect(inserts[0]).not.toContain(output().encodedInput);
	const reference = { source, target: x, channelSequence: "2" };
	const before = queries.length;
	expect(await outgoing.read(client, reference)).toEqual({
		envelope: { ...output(x), ...reference },
		acknowledged: false,
		hasPredecessor: true,
	});
	expect(queries.length - before).toBe(1);
	// A different target is not held behind x's unacknowledged head.
	expect(
		await outgoing.read(client, { source, target: y, channelSequence: "1" }),
	).toMatchObject({ acknowledged: false, hasPredecessor: false });
	await outgoing.acknowledge(client, {
		...output(x),
		source,
		channelSequence: "1",
	});
	expect(await outgoing.read(client, reference)).toMatchObject({
		acknowledged: false,
		hasPredecessor: false,
	});
	await expect(
		outgoing.read(client, { ...reference, channelSequence: "4" }),
	).rejects.toBeInstanceOf(OutgoingIdentityError);
	// Mark retained history directly to isolate allocation from acknowledgment.
	await database.admin.query(
		`UPDATE "${database.journalSchema}".outgoing_event SET acknowledged = true`,
	);
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence());
		expect(
			(await outgoing.appendOutputs(tx, source, [output(x, 2), output(y)]))
				.references,
		).toEqual(
			expect.arrayContaining([
				{ source, target: x, channelSequence: "4" },
				{ source, target: y, channelSequence: "2" },
			]),
		);
	}, options);
	expect(await outgoing.read(client, reference)).toMatchObject({
		acknowledged: true,
		hasPredecessor: false,
	});
	expect(
		await outgoing.read(client, { ...reference, channelSequence: "4" }),
	).toMatchObject({ acknowledged: false, hasPredecessor: false });
	const otherSource = { ...source, streamKey: '{"id":"b"}' };
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence(otherSource));
		await outgoing.appendOutputs(tx, otherSource, [output(x)]);
	}, options);
	const rows = z
		.array(
			z.object({
				source_stream_key: z.string(),
				target_stream_key: z.string(),
				channel_sequence: z.string(),
				event_version: z.string(),
				payload: z.string(),
				acknowledged: z.boolean(),
			}),
		)
		.parse(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}".outgoing_event
		 ORDER BY source_stream_key, target_stream_key, channel_sequence`,
				)
			).rows,
		);
	expect(
		rows.map((row) => [
			row.source_stream_key,
			row.target_stream_key,
			row.channel_sequence,
			row.event_version,
			row.acknowledged,
		]),
	).toEqual([
		[source.streamKey, x.streamKey, "1", "1", true],
		[source.streamKey, x.streamKey, "2", "1", true],
		[source.streamKey, x.streamKey, "3", "2", true],
		[source.streamKey, x.streamKey, "4", "2", false],
		[source.streamKey, y.streamKey, "1", "1", true],
		[source.streamKey, y.streamKey, "2", "1", false],
		[otherSource.streamKey, x.streamKey, "1", "1", false],
	]);
	expect(rows.every((row) => row.payload === output().encodedInput)).toBe(true);
});

test("keeps bigint arithmetic lossless and aborts a mixed batch at channel exhaustion", async ({
	database,
}) => {
	const client = database.client("overflow");
	const outgoing = createOutgoingStorage(database.journalSchema);
	const journal = createJournalStorage(database.journalSchema);
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence());
		await outgoing.appendOutputs(tx, source, [output()]);
	}, options);
	// Seed the retained tail administratively; runtime never edits history.
	await database.admin.query(
		`UPDATE "${database.journalSchema}".outgoing_event SET channel_sequence = 9007199254740991`,
	);
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence());
		expect(
			(await outgoing.appendOutputs(tx, source, [output()])).references,
		).toEqual([{ source, target: x, channelSequence: "9007199254740992" }]);
	}, options);
	expect(
		await outgoing.read(client, {
			source,
			target: x,
			channelSequence: "9007199254740992",
		}),
	).toEqual({
		envelope: { ...output(), source, channelSequence: "9007199254740992" },
		acknowledged: false,
		hasPredecessor: true,
	});
	expect(
		(
			await database.admin.query(
				`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event ORDER BY channel_sequence`,
			)
		).rows,
	).toEqual([
		{ channel_sequence: "9007199254740991" },
		{ channel_sequence: "9007199254740992" },
	]);
	await database.admin.query(
		`UPDATE "${database.journalSchema}".outgoing_event SET channel_sequence = 9223372036854775806 WHERE channel_sequence = 9007199254740992`,
	);
	await expect(
		client.$transaction(async (tx) => {
			await journal.append(tx, occurrence());
			await outgoing.appendOutputs(tx, source, [output(y), output(), output()]);
		}, options),
	).rejects.toBeInstanceOf(ChannelSequenceOverflowError);
	expect(
		(
			await database.admin.query(
				`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event ORDER BY channel_sequence`,
			)
		).rows,
	).toEqual([
		{ channel_sequence: "9007199254740991" },
		{ channel_sequence: "9223372036854775806" },
	]);
	await client.$transaction(async (tx) => {
		expect(await journal.append(tx, occurrence())).toEqual({
			sequence: 3n,
			previousIncomingSequence: 0n,
		});
		await outgoing.appendOutputs(tx, source, [output()]);
	}, options);
	expect(
		(
			await database.admin.query(
				`SELECT MAX(channel_sequence)::text AS tail FROM "${database.journalSchema}".outgoing_event`,
			)
		).rows,
	).toEqual([{ tail: "9223372036854775807" }]);
});

/** Deterministic high-entropy text avoids relying on B-tree compression. */
function maximumIdentity(seed: string): StreamIdentity {
	const text = Array.from({ length: 16 }, (_, index) =>
		createHash("sha256").update(`${seed}/${index}`).digest("hex"),
	).join("");
	return {
		streamName: text.slice(0, 512),
		streamKey: JSON.stringify({ id: text.slice(512, 1015) }),
	};
}

test("indexes two full-size identities and refuses invalid outgoing and incoming metadata", async ({
	database,
}) => {
	const client = database.client("constraints");
	const outgoing = createOutgoingStorage(database.journalSchema);
	const journal = createJournalStorage(database.journalSchema);
	const from = maximumIdentity("source");
	const to = maximumIdentity("target");
	expect(Buffer.byteLength(from.streamName + from.streamKey)).toBe(1024);
	expect(Buffer.byteLength(to.streamName + to.streamKey)).toBe(1024);
	const incoming = {
		...occurrence(to),
		incoming: { source: from, channelSequence: 1n },
	};
	await client.$transaction(async (tx) => {
		await journal.append(tx, occurrence(from));
		await outgoing.appendOutputs(tx, from, [output(to)]);
		await journal.append(tx, incoming);
	}, options);
	for (const [column, value] of [
		["source_stream_name", from.streamName + "x"],
		["target_stream_name", to.streamName + "x"],
		["channel_sequence", "0"],
		["channel_sequence", "9223372036854775808"],
		["event_version", "0"],
		["event_version", "9007199254740992"],
		["event_name", ""],
		["payload_format", "json-v2"],
	] as const) {
		await expect(
			database.admin.query(
				`UPDATE "${database.journalSchema}".outgoing_event SET ${column} = $1`,
				[value],
			),
		).rejects.toMatchObject({
			code: column === "channel_sequence" && value !== "0" ? "22003" : "23514",
		});
	}
	for (const [name, key, sequence] of [
		[null, from.streamKey, "1"],
		[from.streamName, null, "1"],
		[from.streamName, from.streamKey, null],
		["", from.streamKey, "1"],
		[from.streamName, "", "1"],
		[from.streamName, from.streamKey, "0"],
		[from.streamName + "x", from.streamKey, "1"],
	] as const) {
		await expect(
			database.admin.query(
				`UPDATE "${database.journalSchema}".event_log SET incoming_source_stream_name = $1,
			 incoming_source_stream_key = $2, incoming_channel_sequence = $3 WHERE occurrence_id = $4`,
				[name, key, sequence, incoming.occurrenceId],
			),
		).rejects.toMatchObject({ code: "23514" });
	}
	// Real Prisma uniqueness errors must not be mistaken for source duplicates.
	try {
		await client.$queryRawUnsafe(
			`INSERT INTO "${database.journalSchema}".outgoing_event SELECT * FROM "${database.journalSchema}".outgoing_event`,
		);
		throw new Error("Expected outgoing uniqueness failure.");
	} catch (error) {
		expect(error).toMatchObject({
			code: "P2010",
			meta: { driverAdapterError: { cause: { originalCode: "23505" } } },
		});
		expect(classifyFailure(error, "outputs")).toBe("other");
		expect(classifyFailure(error, "append")).toBe("other");
	}
});
