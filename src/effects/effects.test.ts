import { expect, test, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	createDefinitions,
	defineEffect,
	defineEffectHandler,
	defineEvent,
	eventInput,
} from "../index.js";
import {
	createPostgresEffectStore,
	EffectIdentityError,
	type EffectRead,
	type OutstandingEffect,
} from "../postgres/index.js";
import type {
	PostgresBorrowedTransaction,
	PostgresRawClient,
	PostgresTransactionHost,
} from "../postgres/types.js";
import {
	createEffectRunner,
	EffectExecutionError,
	type EffectStore,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const stream = definitions.stream({
	name: "users",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const effect = defineEffect({
	name: "user.index",
	version: 1,
	schema: z.object({ id: z.string() }),
});
const event = defineEvent({
	name: "user.indexed",
	version: 1,
	stream,
	schema: z.object({ id: z.string() }),
	route: ({ id }) => ({ id }),
});
const registry = definitions.registry({
	streams: [stream],
	events: [event],
	effects: [effect],
});
const outstanding: OutstandingEffect = {
	reference: {
		source: { streamName: "users", streamKey: '{"id":"u1"}' },
		effectSequence: "1",
	},
	effectName: effect.name,
	effectVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"id":"u1"}',
};

test("completion preparation is inert and immutable, with facade-bound proposals rejected before SQL", async () => {
	const client = {
		$connect: vi.fn(async () => {}),
		$disconnect: vi.fn(async () => {}),
		$queryRawUnsafe: vi.fn<PostgresRawClient["$queryRawUnsafe"]>(),
		$transaction: vi.fn(async (): Promise<never> => {
			throw new Error("Unexpected transaction");
		}),
	} satisfies PostgresTransactionHost<PostgresBorrowedTransaction> &
		PostgresRawClient;
	const store = createPostgresEffectStore({
		client,
		registry,
		schema: "metadata",
	});
	const other = createPostgresEffectStore({
		client,
		registry,
		schema: "metadata",
	});
	const proposal = store.prepareCompletion(outstanding, {
		events: [eventInput(event, { id: "u1" })],
	});
	expect(Object.isFrozen(proposal)).toBe(true);
	expect(JSON.stringify(proposal)).toBe("{}");
	await expect(other.complete(proposal)).rejects.toBeInstanceOf(
		EffectIdentityError,
	);
	// @ts-expect-error A structural imitation is not a completion proposal.
	await expect(store.complete({})).rejects.toBeInstanceOf(EffectIdentityError);
	expect(client.$transaction).not.toHaveBeenCalled();
	expect(client.$queryRawUnsafe).not.toHaveBeenCalled();
	expect(client.$connect).not.toHaveBeenCalled();
	expect(client.$disconnect).not.toHaveBeenCalled();
});

test("neutral runner bounds read/definition/input failures and never exposes arbitrary causes", async () => {
	const handle = vi.fn(() => ({ events: [] }));
	const executor = definitions.registry({
		streams: [stream],
		events: [event],
		effects: [effect],
		effectHandlers: [defineEffectHandler({ effect, handle })],
	});
	const read = vi.fn<EffectStore["read"]>();
	const prepareCompletion = vi.fn<EffectStore["prepareCompletion"]>();
	const complete = vi.fn<EffectStore["complete"]>();
	const runner = createEffectRunner({
		registry: executor,
		store: { read, prepareCompletion, complete },
	});
	const cases: { read: EffectRead; stage: string }[] = [
		{ read: { status: "notRegistered" }, stage: "read" },
		{
			read: {
				status: "outstanding",
				effect: {
					...outstanding,
					reference: { ...outstanding.reference, effectSequence: "2" },
				},
			},
			stage: "read",
		},
		{
			read: {
				status: "outstanding",
				effect: { ...outstanding, effectVersion: 2 },
			},
			stage: "definition",
		},
		{
			read: {
				status: "outstanding",
				effect: { ...outstanding, encodedInput: '{"id":4}' },
			},
			stage: "input",
		},
	];
	for (const entry of cases) {
		read.mockResolvedValue(entry.read);
		await expect(runner.attempt(outstanding.reference)).rejects.toMatchObject({
			stage: entry.stage,
			externalWork: "notInvoked",
		});
	}
	read.mockRejectedValue(new Error("secret provider response"));
	const failure = await runner
		.attempt(outstanding.reference)
		.catch((error: unknown) => error);
	expect(failure).toBeInstanceOf(EffectExecutionError);
	expect(String(failure)).not.toContain("secret");
	expect(failure).not.toHaveProperty("cause");
	expect(handle).not.toHaveBeenCalled();
	expect(prepareCompletion).not.toHaveBeenCalled();
	expect(complete).not.toHaveBeenCalled();
});
