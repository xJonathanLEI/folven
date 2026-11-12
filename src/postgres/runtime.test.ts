import { expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { DefinitionError } from "../errors.js";
import {
	createDefinitions,
	defineEvent,
	defineHandler,
	type EventEmitter,
} from "../index.js";
import {
	createPostgresRuntime,
	ChannelEnvelopeError,
	PendingOccurrenceEnvelopeError,
	JournalResultError,
	OccurrenceAlreadyProcessedError,
	PayloadEncodingError,
	PostgresIdentityError,
	PublicationResultError,
	RetryExhaustedError,
	RuntimeConfigurationError,
	StreamSequenceOverflowError,
	type OutgoingEnvelope,
	type OutgoingReference,
	type PendingOccurrenceEnvelope,
	type PostgresPublication,
} from "./index.js";
import { occurrenceConstraint } from "./schema.js";
import type { PostgresRawClient, PostgresTransactionHost } from "./types.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});

it.each(["channel", "independent"] as const)(
	"validates %s ingress metadata, wire bytes, schema, route and implementation before opening a transaction",
	async (kind) => {
		const io = transport();
		const handle = vi.fn();
		const runtime = setup(io.host, handle);
		const incoming = kind === "channel" ? envelope : independentEnvelope;
		for (const invalid of [
			{ ...incoming, eventName: "unknown" },
			{ ...incoming, encodedInput: '{"id":"u1","id":"u1"}' },
			{
				...incoming,
				encodedInput: JSON.stringify({ ...input, at: "invalid" }),
			},
		]) {
			await expect(runtime.ingest(invalid)).rejects.toThrow();
		}
		await expect(
			runtime.ingest({
				...incoming,
				target: { ...incoming.target, streamKey: '{"id":"other"}' },
			}),
		).rejects.toBeInstanceOf(
			kind === "channel"
				? ChannelEnvelopeError
				: PendingOccurrenceEnvelopeError,
		);
		const withoutHandler = createPostgresRuntime({
			client: io.host,
			schema: "journal",
			registry: definitions.registry({
				streams: [stream],
				events: [event],
				handlers: [],
			}),
		});
		await expect(withoutHandler.ingest(incoming)).rejects.toBeInstanceOf(
			DefinitionError,
		);
		expect(io.options).toHaveLength(0);
		expect(io.append).not.toHaveBeenCalled();
		expect(io.lookup).not.toHaveBeenCalled();
		expect(handle).not.toHaveBeenCalled();
	},
);

it("ingress retries with fresh transformed payloads and a fixed channel identity and route", async () => {
	const io = transport();
	io.commit.mockRejectedValueOnce({ code: "P2034" });
	const payloads: Array<z.output<typeof schema>> = [];
	const runtime = setup(io.host, (payload) => {
		expect(payload.at).toEqual(new Date(input.at));
		expect(payload.tags).toEqual(input.tags);
		payloads.push(payload);
		payload.tags.push("mutated");
		payload.at.setTime(0);
		return { forbiddenResult: () => 1n };
	});
	route.mockClear();
	expect(await runtime.ingest(envelope)).toEqual({ status: "processed" });
	expect(payloads).toHaveLength(2);
	expect(payloads[0]).not.toBe(payloads[1]);
	expect(route).toHaveBeenCalledTimes(1);
	expect(io.append).toHaveBeenCalledTimes(2);
	expect(io.append.mock.calls[0]).toEqual(io.append.mock.calls[1]);
	expect(io.append.mock.calls[0]?.slice(7)).toEqual([
		envelope.encodedInput,
		envelope.source.streamName,
		envelope.source.streamKey,
		1n,
	]);
	expect(io.lookup).not.toHaveBeenCalled();
});
const schema = z.strictObject({
	id: z.string(),
	at: z.iso.datetime().transform((value) => new Date(value)),
	tags: z.array(z.string()),
});
const route = vi.fn((event: z.output<typeof schema>) => ({ id: event.id }));
const event = defineEvent({
	name: "author.edit",
	version: 1,
	stream,
	schema,
	route,
});
const input = { id: "u1", at: "2026-01-01T00:00:00.000Z", tags: ["original"] };
const envelope = {
	source: { streamName: "posts", streamKey: '{"id":"p1"}' },
	target: { streamName: "author", streamKey: '{"id":"u1"}' },
	channelSequence: "1",
	eventName: event.name,
	eventVersion: event.version,
	payloadFormat: "json-v1",
	encodedInput: JSON.stringify(input),
} satisfies OutgoingEnvelope;
const independentEnvelope = {
	occurrenceId: "81bb4c98-920d-4b94-9e8f-e2c5d7f34510",
	target: envelope.target,
	eventName: event.name,
	eventVersion: event.version,
	payloadFormat: "json-v1",
	encodedInput: envelope.encodedInput,
} satisfies PendingOccurrenceEnvelope;

/** Script transport outcomes only; no model operations or simulated database. */
function transport() {
	const append = vi.fn<PostgresRawClient["$queryRawUnsafe"]>(async () => [
		{ stream_sequence: 1n, previous_incoming_sequence: 0n },
	]);
	const lookup = vi.fn<PostgresRawClient["$queryRawUnsafe"]>(async () => []);
	const commit = vi.fn<() => Promise<void>>(async () => {});
	const connect = vi.fn(async () => {});
	const disconnect = vi.fn(async () => {});
	const options: unknown[] = [];
	const clients: PostgresRawClient[] = [];
	const host: PostgresTransactionHost<Prisma.TransactionClient> &
		PostgresRawClient = {
		$connect: connect,
		$disconnect: disconnect,
		$queryRawUnsafe: lookup,
		async $transaction(run, configuration) {
			options.push(configuration);
			const raw = { $queryRawUnsafe: append } satisfies PostgresRawClient;
			clients.push(raw);
			// Only the checked raw surface is exercised. No fake Prisma model
			// methods are supplied or used; integration tests own real writes.
			const result = await run(raw as unknown as Prisma.TransactionClient);
			await commit();
			return result;
		},
	};
	return {
		host,
		append,
		lookup,
		commit,
		connect,
		disconnect,
		options,
		clients,
	};
}

it("rejects an invalid publication result inside the transaction without retrying", async () => {
	const io = emittingTransport();
	const invalid = {
		prepare: () => ({
			sql: "SELECT 1",
			parameters: [],
			validate: () => undefined,
		}),
	};
	const handler = defineHandler({
		event,
		handle: ({ emit }) => {
			emit(event, input);
		},
	});
	const runtime = createPostgresRuntime({
		client: io.host,
		registry: definitions.registry({
			streams: [stream],
			events: [event],
			handlers: [handler],
		}),
		schema: "journal",
		// @ts-expect-error Exercise a malformed infrastructure implementation.
		publication: invalid,
	});
	await expect(runtime.dispatch(event, input)).rejects.toBeInstanceOf(
		PublicationResultError,
	);
	expect(io.options).toHaveLength(1);
	expect(io.commit).not.toHaveBeenCalled();
});

function setup<Result>(
	host: PostgresTransactionHost<Prisma.TransactionClient> & PostgresRawClient,
	handle: (
		payload: z.output<typeof schema>,
		client: unknown,
		emit: EventEmitter,
	) => Result,
	publication?: PostgresPublication,
) {
	const handler = defineHandler({
		event,
		handle: ({ event: payload, db, emit }) => handle(payload, db, emit),
	});
	const registry = definitions.registry({
		streams: [stream],
		events: [event],
		handlers: [handler],
	});
	return createPostgresRuntime({
		client: host,
		registry,
		schema: "journal",
		...(publication === undefined ? {} : { publication }),
	});
}

const reference = {
	source: envelope.target,
	target: envelope.target,
	channelSequence: "1",
} satisfies OutgoingReference;

function emittingTransport() {
	const io = transport();
	io.append.mockImplementation(async (sql) => {
		if (sql.startsWith("WITH captured"))
			return [
				{
					references: [reference],
					publications: [{ id: "1" }],
					overflow: false,
					effects: null,
					effectPublications: [],
				},
			];
		return [{ stream_sequence: 1n, previous_incoming_sequence: 0n }];
	});
	return io;
}

it.each(["commit", "publication"] as const)(
	"discards provisional publication actions on a known %s abort and notifies only after commit",
	async (failurePhase) => {
		const io = emittingTransport();
		const actions: Array<ReturnType<typeof vi.fn<() => Promise<void>>>> = [];
		const committed = vi.fn();
		io.commit.mockImplementation(async () => {
			committed();
		});
		if (failurePhase === "commit")
			io.commit.mockRejectedValueOnce({ code: "P2034" });
		const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
			sql: "SELECT 1",
			parameters: [],
			validate(rows, count) {
				expect(rows).toEqual([{ id: "1" }]);
				expect(count).toBe(1);
				expect(io.append.mock.calls.at(-1)?.[0]).toContain("WITH captured");
				const action = vi.fn(async () => {
					expect(committed).toHaveBeenCalledTimes(1);
				});
				actions.push(action);
				if (failurePhase === "publication" && actions.length === 1)
					throw { code: "P2034" };
				return action;
			},
		}));
		const runtime = setup(
			io.host,
			(_payload, db, emit) => {
				expect(db).toBe(io.clients.at(-1));
				emit(event, input);
				return { callback: () => 7n };
			},
			{ prepare },
		);
		expect((await runtime.dispatch(event, input)).callback()).toBe(7n);
		expect(prepare).toHaveBeenCalledTimes(2);
		expect(actions[0]).not.toHaveBeenCalled();
		expect(actions[1]).toHaveBeenCalledTimes(1);
		await actions[1]?.mock.results[0]?.value;
	},
);

it.each(["sync", "async"] as const)(
	"keeps %s post-commit failures outside retry handling for dispatch and ingestion",
	async (kind) => {
		const io = emittingTransport();
		const action = vi.fn<() => void | Promise<void>>(() => {
			expect(io.commit).toHaveBeenCalled();
			// Even a known-abort-shaped error after commit must never retry.
			if (kind === "sync") throw { code: "P2034" };
			return Promise.reject({ code: "P2034" });
		});
		const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
			sql: "SELECT 1",
			parameters: [],
			validate: () => action,
		}));
		const runtime = setup(
			io.host,
			(_payload, _db, emit) => {
				emit(event, input);
				return "committed";
			},
			{ prepare },
		);
		expect(await runtime.dispatch(event, input)).toBe("committed");
		expect(await runtime.ingest(envelope)).toEqual({ status: "processed" });
		expect(io.options).toHaveLength(2);
		expect(prepare).toHaveBeenCalledTimes(2);
		expect(action).toHaveBeenCalledTimes(2);
	},
);

it("does not publish empty outputs, handler rollbacks or incoming gaps", async () => {
	const io = emittingTransport();
	const action = vi.fn();
	const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
		sql: "SELECT 1",
		parameters: [],
		validate: () => action,
	}));
	await setup(io.host, () => "empty", { prepare }).dispatch(event, input);
	await expect(
		setup(
			io.host,
			(_payload, _db, emit) => {
				emit(event, input);
				throw new Error("Handler rollback");
			},
			{ prepare },
		).dispatch(event, input),
	).rejects.toThrow("Handler rollback");
	const handle = vi.fn();
	expect(
		await setup(io.host, handle, { prepare }).ingest({
			...envelope,
			channelSequence: "2",
		}),
	).toEqual({ status: "gap", expectedSequence: "1" });
	expect(handle).not.toHaveBeenCalled();
	expect(prepare).not.toHaveBeenCalled();
	expect(action).not.toHaveBeenCalled();
	expect(
		io.append.mock.calls.every(([sql]) => !sql.startsWith("WITH captured")),
	).toBe(true);
});

it("does not notify after an uncertain commit or reinterpret publication failures as journal conflicts", async () => {
	for (const phase of ["commit", "publication"] as const) {
		const io = emittingTransport();
		const action = vi.fn();
		const failure =
			phase === "commit"
				? { code: "ECONNRESET" }
				: {
						code: "P2010",
						meta: {
							driverAdapterError: {
								cause: {
									originalCode: "23505",
									kind: "UniqueConstraintViolation",
									constraint: { index: occurrenceConstraint },
								},
							},
						},
					};
		const prepare = vi.fn<PostgresPublication["prepare"]>(() => ({
			sql: "SELECT 1",
			parameters: [],
			validate() {
				if (phase === "publication") throw failure;
				return action;
			},
		}));
		// Transport double, not a claim to simulate a real network failure.
		if (phase === "commit") io.commit.mockRejectedValueOnce(failure);
		await expect(
			setup(
				io.host,
				(_payload, _db, emit) => {
					emit(event, input);
				},
				{ prepare },
			).dispatch(event, input),
		).rejects.toBe(failure);
		expect(io.options).toHaveLength(1);
		expect(action).not.toHaveBeenCalled();
		expect(io.lookup).not.toHaveBeenCalled();
	}
});

it("retries aborted transactions with fixed occurrence bytes, fresh input/client and no rerouting", async () => {
	const io = transport();
	const failure = { code: "P2034" };
	io.commit.mockRejectedValueOnce(failure);
	const handle = vi.fn((payload: z.output<typeof schema>, db: unknown) => {
		expect(db).toBe(io.clients.at(-1));
		expect(payload.at).toEqual(new Date(input.at));
		expect(payload.tags).toEqual(["original"]);
		expect(payload).not.toBe(input);
		payload.at.setTime(0);
		payload.tags.push("mutated");
		return { nonJson: () => 7n };
	});
	const runtime = setup(io.host, handle);
	route.mockClear();
	const result = await runtime.dispatch(event, input);
	expect(result.nonJson()).toBe(7n);
	expect(handle).toHaveBeenCalledTimes(2);
	expect(handle.mock.calls[0]?.[0]).not.toBe(handle.mock.calls[1]?.[0]);
	expect(io.clients[0]).not.toBe(io.clients[1]);
	expect(route).toHaveBeenCalledTimes(1);
	expect(io.append.mock.calls[0]).toEqual(io.append.mock.calls[1]);
	expect(io.append.mock.calls[0]?.[7]).toBe(JSON.stringify(input));
	expect(io.lookup).not.toHaveBeenCalled();
	expect(input.tags).toEqual(["original"]);
	expect(io.options).toEqual(
		Array.from({ length: 2 }, () => ({
			isolationLevel: "Serializable",
			maxWait: 2_000,
			timeout: 5_000,
		})),
	);
	expect(io.connect).not.toHaveBeenCalled();
	expect(io.disconnect).not.toHaveBeenCalled();
});

it("never invokes a handler before reservation and bounds known-abort retries", async () => {
	const io = transport();
	const handle = vi.fn(() => null);
	io.append.mockRejectedValue({
		code: "P2010",
		meta: {
			driverAdapterError: {
				cause: {
					originalCode: "40001",
					kind: "TransactionWriteConflict",
				},
			},
		},
	});
	await expect(
		setup(io.host, handle).dispatch(event, input),
	).rejects.toBeInstanceOf(RetryExhaustedError);
	expect(io.options).toHaveLength(3);
	expect(handle).not.toHaveBeenCalled();
	expect(io.lookup).not.toHaveBeenCalled();
});

it("does not reinterpret journal-looking uniqueness thrown by a handler", async () => {
	const failure = {
		code: "P2010",
		meta: {
			driverAdapterError: {
				cause: {
					originalCode: "23505",
					kind: "UniqueConstraintViolation",
					constraint: { index: occurrenceConstraint },
				},
			},
		},
	};
	const io = transport();
	await expect(
		setup(io.host, () => {
			throw failure;
		}).dispatch(event, input),
	).rejects.toBe(failure);
	expect(io.options).toHaveLength(1);
	expect(io.lookup).not.toHaveBeenCalled();
});

it("fails closed on malformed reservations and sequence overflow", async () => {
	const io = transport();
	io.append.mockResolvedValueOnce([]);
	const handle = vi.fn(() => null);
	await expect(
		setup(io.host, handle).dispatch(event, input),
	).rejects.toBeInstanceOf(JournalResultError);
	expect(handle).not.toHaveBeenCalled();
	expect(io.options).toHaveLength(1);
	expect(io.commit).not.toHaveBeenCalled();
	io.append.mockResolvedValueOnce([{ stream_sequence: 1n }]);
	await expect(setup(io.host, handle).ingest(envelope)).rejects.toBeInstanceOf(
		JournalResultError,
	);
	expect(handle).not.toHaveBeenCalled();
	expect(io.options).toHaveLength(2);
	expect(io.commit).not.toHaveBeenCalled();
	io.append.mockRejectedValueOnce({
		code: "P2010",
		meta: {
			driverAdapterError: {
				cause: {
					originalCode: "22003",
					kind: "ValueOutOfRange",
				},
			},
		},
	});
	await expect(
		setup(io.host, () => null).dispatch(event, input),
	).rejects.toBeInstanceOf(StreamSequenceOverflowError);
	expect(io.options).toHaveLength(3);
});

it("rejects an exceptional internally allocated occurrence collision without fabricating a result", async () => {
	const io = transport();
	const handle = vi.fn(() => "unrecoverable");
	const prepare = vi.fn<PostgresPublication["prepare"]>();
	io.append.mockImplementation(async (_sql, ...values) => {
		const [
			id,
			streamName,
			streamKey,
			eventName,
			eventVersion,
			format,
			payload,
		] = z
			.tuple([
				z.string(),
				z.string(),
				z.string(),
				z.string(),
				z.number(),
				z.literal("json-v1"),
				z.string(),
				z.null(),
				z.null(),
				z.null(),
			])
			.parse(values);
		io.lookup.mockImplementation(async (_lookupSql, lookupId) => {
			expect(lookupId).toBe(id);
			return [
				{
					occurrence_id: id,
					event_name: eventName,
					event_version: BigInt(eventVersion),
					stream_name: streamName,
					stream_key: streamKey,
					payload_format: format,
					payload,
					incoming_source_stream_name: null,
					incoming_source_stream_key: null,
					incoming_channel_sequence: null,
				},
			];
		});
		throw {
			code: "P2010",
			meta: {
				driverAdapterError: {
					cause: {
						originalCode: "23505",
						kind: "UniqueConstraintViolation",
						constraint: { index: occurrenceConstraint },
					},
				},
			},
		};
	});
	await expect(
		setup(io.host, handle, { prepare }).dispatch(event, input),
	).rejects.toBeInstanceOf(OccurrenceAlreadyProcessedError);
	expect(handle).not.toHaveBeenCalled();
	expect(io.lookup).toHaveBeenCalledTimes(1);
	expect(io.options).toHaveLength(1);
	expect(prepare).not.toHaveBeenCalled();
});

it("validates construction, canonical events, wire input, schema and routing before any SQL", async () => {
	const io = transport();
	const runtime = setup(io.host, () => null);
	expect(io.options).toHaveLength(0);
	await expect(runtime.dispatch({ ...event }, input)).rejects.toBeInstanceOf(
		DefinitionError,
	);
	await expect(
		runtime.dispatch(event, {
			...input,
			at: undefined,
		} as unknown as typeof input),
	).rejects.toBeInstanceOf(PayloadEncodingError);
	await expect(
		runtime.dispatch(event, { ...input, at: "invalid" }),
	).rejects.toBeInstanceOf(z.ZodError);
	await expect(
		runtime.dispatch(event, { ...input, id: "\0" }),
	).rejects.toBeInstanceOf(PostgresIdentityError);
	await expect(
		runtime.dispatch(event, { ...input, id: "" }),
	).rejects.toBeInstanceOf(DefinitionError);
	expect(io.options).toHaveLength(0);
	expect(io.lookup).not.toHaveBeenCalled();
	const handler = defineHandler({ event, handle: () => null });
	const registry = definitions.registry({
		streams: [stream],
		events: [event],
		handlers: [handler],
	});
	const prepare = vi.fn<PostgresPublication["prepare"]>();
	for (const lanes of [
		{ effectPublication: { prepare } },
		{ publication: { kind: "effect", prepare } },
	]) {
		const untyped: unknown = {
			client: io.host,
			registry,
			schema: "journal",
			...lanes,
		};
		// @ts-expect-error Exercise role validation at an untyped host boundary.
		expect(() => createPostgresRuntime(untyped)).toThrow(
			RuntimeConfigurationError,
		);
	}
	expect(prepare).not.toHaveBeenCalled();
	for (const transaction of [
		{ timeout: 0 },
		{ maxWait: NaN },
		{ timeout: 2 ** 32 },
		{ maxWait: 1.5 },
	]) {
		expect(() =>
			createPostgresRuntime({
				client: io.host,
				registry,
				schema: "journal",
				transaction,
			}),
		).toThrow(RuntimeConfigurationError);
	}
	expect(() =>
		createPostgresRuntime({
			client: { ...io.host, $connect: undefined } as unknown as typeof io.host,
			registry,
			schema: "journal",
		}),
	).toThrow(RuntimeConfigurationError);
	expect(io.options).toHaveLength(0);
});
