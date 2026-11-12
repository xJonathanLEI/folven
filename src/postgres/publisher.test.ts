import { expect, test, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { createDefinitions, defineEvent } from "../index.js";
import {
	createPostgresPublisher,
	PendingOccurrencePreparationError,
	PendingOccurrenceResultError,
	PublicationResultError,
	RuntimeConfigurationError,
	type PostgresPublication,
	type PostgresRawClient,
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
const input = {
	id: " a ",
	at: "2026-01-01T00:00:00Z",
	extra: { untouched: true },
};
const batchSchema = z.array(
	z.strictObject({
		occurrence_id: z.uuid(),
		target_stream_name: z.string(),
		target_stream_key: z.string(),
		event_name: z.string(),
		event_version: z.number(),
		payload_format: z.literal("json-v1"),
		payload: z.string(),
	}),
);
function readBatch(value: unknown) {
	return batchSchema.parse(JSON.parse(z.string().parse(value)));
}

test("preparation captures only immutable wire facts, canonical membership, and a stable ID without an implementation", async () => {
	const route = vi.fn(timestamp.route);
	const event = defineEvent({ ...timestamp, route });
	const publisher = createPostgresPublisher({
		registry: definitions.registry({
			streams: [author],
			events: [event],
			handlers: [],
		}),
		schema: "metadata",
	});
	const value = structuredClone(input);
	const prepared = publisher.prepare(event, value);
	expect(Object.isFrozen(prepared)).toBe(true);
	expect(route).toHaveBeenCalledExactlyOnceWith({
		id: "a",
		at: new Date(input.at),
	});
	value.id = "changed";
	value.extra.untouched = false;
	const query = vi.fn<PostgresRawClient["$queryRawUnsafe"]>(
		async (_sql, batch) => [
			{
				references: readBatch(batch).map((row) => ({
					occurrenceId: row.occurrence_id,
				})),
				publications: [],
			},
		],
	);
	const first = await publisher.stage({ $queryRawUnsafe: query }, [prepared]);
	const second = await publisher.stage({ $queryRawUnsafe: query }, [prepared]);
	expect(second.references).toEqual(first.references);
	expect(query.mock.calls[1]).toEqual(query.mock.calls[0]);
	expect(readBatch(query.mock.calls[0]?.[1])).toEqual([
		{
			occurrence_id: first.references[0]?.occurrenceId,
			target_stream_name: "author",
			target_stream_key: '{"id":"a"}',
			event_name: timestamp.name,
			event_version: 1,
			payload_format: "json-v1",
			payload: JSON.stringify(input),
		},
	]);
	expect(route).toHaveBeenCalledTimes(1);
	expect(() => publisher.prepare(defineEvent({ ...event }), input)).toThrow(
		/exact registered/,
	);
	expect(() => publisher.prepare(event, { id: "a", at: "invalid" })).toThrow();
});

test("staging rejects root clients, forged/foreign/sparse preparations and duplicates before SQL or publication preparation", async () => {
	const prepare = vi.fn<PostgresPublication["prepare"]>();
	const publisher = createPostgresPublisher({
		registry,
		schema: "metadata",
		publication: { prepare },
	});
	const prepared = publisher.prepare(timestamp, input);
	const other = createPostgresPublisher({
		registry,
		schema: "metadata",
	}).prepare(timestamp, input);
	const query = vi.fn<PostgresRawClient["$queryRawUnsafe"]>();
	const transaction = { $queryRawUnsafe: query };
	for (const values of [
		[prepared, prepared],
		[other],
		[{}],
		[null],
		Array(1),
		null,
	]) {
		// @ts-expect-error Untyped callers must not forge preparations.
		await expect(publisher.stage(transaction, values)).rejects.toBeInstanceOf(
			PendingOccurrencePreparationError,
		);
	}
	for (const root of [
		{ ...transaction, $connect: async () => {} },
		{ ...transaction, $disconnect: async () => {} },
	]) {
		// @ts-expect-error Root clients are not borrowed transactions.
		await expect(publisher.stage(root, [prepared])).rejects.toBeInstanceOf(
			RuntimeConfigurationError,
		);
	}
	expect(query).not.toHaveBeenCalled();
	expect(prepare).not.toHaveBeenCalled();
});

test.each([0, 2])(
	"stages %i occurrences with zero/one composed query and explicit isolated notifications",
	async (count) => {
		const notify = vi.fn(async () => {
			throw new Error("notification failed");
		});
		const validate = vi.fn(() => notify);
		const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
			sql: "INSERT INTO test_tasks SELECT $2, encoded_payload FROM delivery_inputs RETURNING id",
			parameters: ["bound-task-name"],
			validate,
		}));
		const publisher = createPostgresPublisher({
			registry,
			schema: "metadata",
			publication: { prepare },
		});
		const query = vi.fn<PostgresRawClient["$queryRawUnsafe"]>(
			async (_sql, batch) => [
				{
					references: readBatch(batch)
						.map((row) => ({ occurrenceId: row.occurrence_id }))
						.toReversed(),
					publications: Array.from({ length: count }, (_, id) => ({ id })),
				},
			],
		);
		const transaction = {
			$queryRawUnsafe(sql: string, ...values: unknown[]) {
				expect(this).toBe(transaction);
				return query(sql, ...values);
			},
		};
		const result = await publisher.stage(
			transaction,
			Array.from({ length: count }, () => publisher.prepare(timestamp, input)),
		);
		expect(query).toHaveBeenCalledTimes(count === 0 ? 0 : 1);
		expect(prepare).toHaveBeenCalledTimes(count === 0 ? 0 : 1);
		expect(notify).not.toHaveBeenCalled();
		expect(Object.isFrozen(result)).toBe(true);
		expect(Object.isFrozen(result.references)).toBe(true);
		for (const reference of result.references)
			expect(Object.isFrozen(reference)).toBe(true);
		if (count > 0) {
			expect(prepare).toHaveBeenCalledWith({
				source: {
					relation: "delivery_inputs",
					payloadColumn: "encoded_payload",
				},
				parameterOffset: 1,
			});
			expect(query.mock.calls[0]?.at(-1)).toBe("bound-task-name");
			expect(validate).toHaveBeenCalledTimes(1);
		}
		await expect(result.notifyCommitted()).resolves.toBeUndefined();
		await expect(result.notifyCommitted()).resolves.toBeUndefined();
		expect(notify).toHaveBeenCalledTimes(count === 0 ? 0 : 2);
		expect(query).toHaveBeenCalledTimes(count === 0 ? 0 : 1);
	},
);

test("staging validates exact unordered reference membership and action results before returning", async () => {
	const publisher = createPostgresPublisher({ registry, schema: "metadata" });
	const prepared = [
		publisher.prepare(timestamp, input),
		publisher.prepare(timestamp, input),
	];
	for (const mode of [
		"missing",
		"extra",
		"duplicate",
		"different",
		"mixed",
		"invalid",
	]) {
		const query: PostgresRawClient["$queryRawUnsafe"] = async (_sql, batch) => {
			const references: unknown[] = readBatch(batch).map((row) => ({
				occurrenceId: row.occurrence_id,
			}));
			if (mode === "missing") references.pop();
			if (mode === "extra") references.push(references[0]);
			if (mode === "duplicate") references[1] = references[0];
			if (mode === "different")
				references[0] = {
					occurrenceId: "11111111-1111-4111-8111-111111111111",
				};
			if (mode === "mixed")
				references[0] = {
					occurrenceId: readBatch(batch)[0]?.occurrence_id,
					source: {},
				};
			return mode === "invalid" ? null : [{ references, publications: [] }];
		};
		await expect(
			publisher.stage({ $queryRawUnsafe: query }, prepared),
		).rejects.toBeInstanceOf(PendingOccurrenceResultError);
	}
	const invalid = createPostgresPublisher({
		registry,
		schema: "metadata",
		publication: {
			prepare: () => ({
				sql: "SELECT 1",
				parameters: [],
				// @ts-expect-error An untyped adapter omitted its post-commit action.
				validate: () => undefined,
			}),
		},
	});
	await expect(
		invalid.stage(
			{
				$queryRawUnsafe: async (_sql, batch) => [
					{
						references: readBatch(batch).map((row) => ({
							occurrenceId: row.occurrence_id,
						})),
						publications: [],
					},
				],
			},
			[invalid.prepare(timestamp, input)],
		),
	).rejects.toBeInstanceOf(PublicationResultError);
});
