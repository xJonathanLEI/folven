import { expect, test, vi } from "vitest";
import {
	OutgoingIdentityError,
	OutgoingResultError,
	PendingReadOptionsError,
} from "./errors.js";
import { createOutgoingStorage } from "./outgoing.js";
import type {
	OutgoingEnvelope,
	PendingReadOptions,
	PostgresPublication,
	PostgresRawClient,
} from "./types.js";

const envelope = {
	source: { streamName: "author", streamKey: '{"id":"a"}' },
	target: { streamName: "post", streamKey: '{"id":"p"}' },
	channelSequence: "9007199254740993",
	eventName: "post.publish",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"id":"p"}',
} satisfies OutgoingEnvelope;
const reference = {
	source: envelope.source,
	target: envelope.target,
	channelSequence: envelope.channelSequence,
};

test("batch references are frozen, exact-count checked and source-owned without an extra query", async () => {
	const query = vi.fn<(...args: unknown[]) => Promise<unknown>>();
	const client = { $queryRawUnsafe: query };
	const storage = createOutgoingStorage("journal");
	expect(await storage.appendOutputs(client, envelope.source, [])).toEqual({
		references: [],
		afterCommit: undefined,
	});
	expect(query).not.toHaveBeenCalled();
	query.mockResolvedValue([
		{
			references: [reference],
			publications: [],
			overflow: false,
			effects: null,
			effectPublications: [],
		},
	]);
	const { references } = await storage.appendOutputs(client, envelope.source, [
		envelope,
	]);
	expect(references).toEqual([reference]);
	expect(query).toHaveBeenCalledTimes(1);
	expect(Object.isFrozen(references)).toBe(true);
	expect(Object.isFrozen(references[0])).toBe(true);
	expect(Object.isFrozen(references[0]?.target)).toBe(true);
	for (const result of [
		[],
		[reference, reference],
		[{ ...reference, channelSequence: 1 }],
		[{ ...reference, source: envelope.target }],
	]) {
		query.mockResolvedValue([
			{
				references: result,
				publications: [],
				overflow: false,
				effects: null,
				effectPublications: [],
			},
		]);
		await expect(
			storage.appendOutputs(client, envelope.source, [envelope]),
		).rejects.toBeInstanceOf(OutgoingResultError);
	}
});

test.each([0, 1, 10, 128])(
	"forwards one composed execution for %i outputs, with original binds and client identity",
	async (count) => {
		const action = vi.fn();
		const references = Array.from({ length: count }, (_, index) => ({
			...reference,
			channelSequence: String(index + 1),
		}));
		const validate = vi.fn(() => action);
		const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
			sql: "INSERT INTO test_tasks SELECT $4, encoded_payload FROM delivery_inputs RETURNING id",
			parameters: ["bound-task-name"],
			validate,
		}));
		const publicationRows = Array.from({ length: count }, (_, index) => ({
			id: String(index + 1),
		}));
		const query = vi.fn<PostgresRawClient["$queryRawUnsafe"]>(async () => [
			{
				references: references.toReversed(),
				publications: publicationRows,
				overflow: false,
				effects: null,
				effectPublications: [],
			},
		]);
		const client = {
			$queryRawUnsafe(sql: string, ...values: unknown[]) {
				expect(this).toBe(client);
				return query(sql, ...values);
			},
		};
		const result = await createOutgoingStorage("journal").appendOutputs(
			client,
			envelope.source,
			Array.from({ length: count }, () => envelope),
			{ prepare },
		);
		expect(query).toHaveBeenCalledTimes(count === 0 ? 0 : 1);
		expect(prepare).toHaveBeenCalledTimes(count === 0 ? 0 : 1);
		expect(action).not.toHaveBeenCalled();
		if (count === 0) {
			expect(result.afterCommit).toBeUndefined();
			expect(validate).not.toHaveBeenCalled();
			return;
		}
		expect(prepare).toHaveBeenCalledWith({
			source: { relation: "delivery_inputs", payloadColumn: "encoded_payload" },
			parameterOffset: 3,
		});
		const call = query.mock.calls[0];
		expect(call?.slice(1, 3)).toEqual([
			envelope.source.streamName,
			envelope.source.streamKey,
		]);
		expect(call?.at(-1)).toBe("bound-task-name");
		expect(validate).toHaveBeenCalledExactlyOnceWith(publicationRows, count);
		await result.afterCommit?.();
		expect(action).toHaveBeenCalledOnce();
	},
);

test("exact reads validate metadata, payload and reference without exposing stored data", async () => {
	const query = vi.fn<(...args: unknown[]) => Promise<unknown>>();
	const client = { $queryRawUnsafe: query };
	const storage = createOutgoingStorage("journal");
	const row = { envelope, acknowledged: false, hasPredecessor: true };
	query.mockResolvedValue([row]);
	const result = await storage.read(client, reference);
	expect(result).toEqual(row);
	expect(Object.isFrozen(result)).toBe(true);
	expect(Object.isFrozen(result.envelope)).toBe(true);
	expect(query.mock.calls[0]?.slice(1)).toEqual([
		reference.source.streamName,
		reference.source.streamKey,
		reference.target.streamName,
		reference.target.streamKey,
		reference.channelSequence,
	]);
	for (const rows of [
		null,
		[row, row],
		[{ ...row, acknowledged: 1 }],
		[{ ...row, hasPredecessor: undefined }],
		[
			{
				...row,
				envelope: { ...envelope, encodedInput: "private malformed payload" },
			},
		],
		[{ ...row, envelope: { ...envelope, channelSequence: "1" } }],
	]) {
		query.mockResolvedValue(rows);
		await expect(storage.read(client, reference)).rejects.toBeInstanceOf(
			OutgoingResultError,
		);
	}
	query.mockResolvedValue([]);
	await expect(storage.read(client, reference)).rejects.toBeInstanceOf(
		OutgoingIdentityError,
	);
	query.mockClear();
	await expect(
		storage.read(client, { ...reference, channelSequence: "01" }),
	).rejects.toThrow();
	expect(query).not.toHaveBeenCalled();
});

test("pending options fail before SQL; snapshots retain exact bigint text and are immutable", async () => {
	const query = vi.fn<(...args: unknown[]) => Promise<unknown>>();
	const client = { $queryRawUnsafe: query };
	const storage = createOutgoingStorage("journal");
	// The untyped edge deliberately represents callers outside TypeScript.
	const read = (options: unknown) =>
		// @ts-expect-error Deliberately exercise untyped composition callers.
		storage.readPending(client, options);
	for (const options of [
		null,
		{ limit: 0 },
		{ offset: 1 },
		{ after: { source: envelope.source } },
	]) {
		await expect(read(options)).rejects.toThrow();
	}
	expect(query).not.toHaveBeenCalled();
	await expect(read({ limit: 1_001 })).rejects.toBeInstanceOf(
		PendingReadOptionsError,
	);
	query.mockResolvedValue([envelope]);
	const page = await storage.readPending(client);
	expect(page).toEqual({ envelopes: [envelope], nextCursor: null });
	expect(query.mock.calls[0]?.slice(1)).toEqual([null, null, null, null, 101]);
	expect(Object.isFrozen(page)).toBe(true);
	expect(Object.isFrozen(page.envelopes)).toBe(true);
	expect(Object.isFrozen(page.envelopes[0]?.source)).toBe(true);
	expect(Object.isFrozen(page.envelopes[0])).toBe(true);
	const after = { source: envelope.source, target: envelope.target };
	const options = { limit: 1_000, after } satisfies PendingReadOptions;
	await storage.readPending(client, options);
	expect(query.mock.calls[1]?.slice(1)).toEqual([
		"author",
		'{"id":"a"}',
		"post",
		'{"id":"p"}',
		1_001,
	]);
});

test("storage fails closed on malformed results without leaking persisted payloads", async () => {
	const query = vi.fn<(...args: unknown[]) => Promise<unknown>>();
	const client = { $queryRawUnsafe: query };
	const storage = createOutgoingStorage("journal");
	for (const rows of [
		null,
		[{ ...envelope, encodedInput: "private malformed payload" }],
		[envelope, envelope, envelope],
	]) {
		query.mockResolvedValue(rows);
		await expect(
			storage.readPending(client, { limit: 1 }),
		).rejects.toBeInstanceOf(OutgoingResultError);
	}
	query.mockResolvedValue([]);
	await expect(storage.acknowledge(client, envelope)).rejects.toBeInstanceOf(
		OutgoingIdentityError,
	);
	for (const rows of [null, [{ sequence: 1n }], [{ sequence: "1" }]]) {
		query.mockResolvedValue(rows);
		await expect(storage.acknowledge(client, envelope)).rejects.toBeInstanceOf(
			OutgoingResultError,
		);
	}
});
