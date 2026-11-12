import { expect, test, vi } from "vitest";
import {
	PendingOccurrenceIdentityError,
	PendingOccurrenceResultError,
} from "./errors.js";
import { createPendingOccurrenceStorage } from "./pending.js";
import type { PendingOccurrenceEnvelope, PostgresRawClient } from "./types.js";

const envelope = {
	occurrenceId: "11111111-1111-4111-8111-111111111111",
	target: { streamName: "author", streamKey: '{"id":"a"}' },
	eventName: "author.rename",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"id":"a","name":"A"}',
} satisfies PendingOccurrenceEnvelope;
const reference = { occurrenceId: envelope.occurrenceId };

test("exact pending reads and acknowledgments reject malformed database results and missing history", async () => {
	const query = vi.fn<PostgresRawClient["$queryRawUnsafe"]>();
	const client = { $queryRawUnsafe: query };
	const storage = createPendingOccurrenceStorage("metadata");
	const row = { envelope, acknowledged: false };
	for (const result of [
		null,
		[row, row],
		[{ ...row, acknowledged: "false" }],
		[{ ...row, hasPredecessor: false }],
		[
			{
				...row,
				envelope: { ...envelope, encodedInput: "private malformed bytes" },
			},
		],
		[
			{
				...row,
				envelope: {
					...envelope,
					occurrenceId: "22222222-2222-4222-8222-222222222222",
				},
			},
		],
	]) {
		query.mockResolvedValue(result);
		await expect(storage.read(client, reference)).rejects.toBeInstanceOf(
			PendingOccurrenceResultError,
		);
	}
	query.mockResolvedValue([]);
	await expect(storage.read(client, reference)).rejects.toBeInstanceOf(
		PendingOccurrenceIdentityError,
	);
	await expect(storage.acknowledge(client, envelope)).rejects.toBeInstanceOf(
		PendingOccurrenceIdentityError,
	);
	for (const result of [
		null,
		[reference, reference],
		[{ occurrenceId: "invalid" }],
	]) {
		query.mockResolvedValue(result);
		await expect(storage.acknowledge(client, envelope)).rejects.toBeInstanceOf(
			PendingOccurrenceResultError,
		);
	}
	query.mockResolvedValue([row]);
	const read = await storage.read(client, reference);
	expect(read).toEqual(row);
	expect(Object.isFrozen(read)).toBe(true);
	expect(Object.isFrozen(read.envelope.target)).toBe(true);
	query.mockResolvedValue([reference]);
	await expect(storage.acknowledge(client, envelope)).resolves.toBeUndefined();
	expect(query.mock.lastCall?.slice(1)).toEqual([
		envelope.occurrenceId,
		envelope.target.streamName,
		envelope.target.streamKey,
		envelope.eventName,
		envelope.eventVersion,
		envelope.payloadFormat,
		envelope.encodedInput,
	]);
});

test("independent scan validates bounded options before SQL and rejects malformed or excessive rows", async () => {
	const query = vi
		.fn<PostgresRawClient["$queryRawUnsafe"]>()
		.mockResolvedValue([]);
	const client = { $queryRawUnsafe: query };
	const storage = createPendingOccurrenceStorage("metadata");
	for (const options of [
		null,
		{ limit: 0 },
		{ limit: 1_001 },
		{ limit: 1.5 },
		{ offset: 1 },
		{ after: "not-a-uuid" },
		{ after: "AAAA1111-1111-4111-8111-111111111111" },
		{ after: reference },
	]) {
		// @ts-expect-error Untyped callers cannot bypass cursor validation.
		await expect(storage.readPending(client, options)).rejects.toThrow();
	}
	expect(query).not.toHaveBeenCalled();
	expect(await storage.readPending(client)).toEqual({
		envelopes: [],
		nextCursor: null,
	});
	expect(query.mock.lastCall?.slice(1)).toEqual([null, 101]);
	await storage.readPending(client, {
		limit: 1_000,
		after: reference.occurrenceId,
	});
	expect(query.mock.lastCall?.slice(1)).toEqual([
		reference.occurrenceId,
		1_001,
	]);
	for (const value of [
		null,
		[{ envelope: {} }],
		[{ envelope }, { envelope }, { envelope }],
	]) {
		query.mockResolvedValue(value);
		await expect(
			storage.readPending(client, { limit: 1 }),
		).rejects.toBeInstanceOf(PendingOccurrenceResultError);
	}
});
