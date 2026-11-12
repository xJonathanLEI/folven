import { randomUUID } from "node:crypto";
import { expect } from "vitest";
import { z } from "zod";
import { bounded, settle, test } from "../../tests/postgres.js";
import {
	prepareEffectRegistration,
	prepareNewEffects,
} from "./effect-registration.js";
import { createEffectStorage } from "./effect-storage.js";
import {
	EffectIdentityError,
	EffectRegistrationError,
	EffectResultError,
	EffectSequenceOverflowError,
} from "./errors.js";
import { classifyFailure } from "./failures.js";
import { createJournalStorage } from "./storage.js";
import type {
	OutstandingEffect,
	PostgresRawClient,
	StreamIdentity,
} from "./types.js";

const source = { streamName: "author", streamKey: '{"id":"a"}' };
const other = { ...source, streamKey: '{"id":"b"}' };
const transactionOptions = {
	isolationLevel: "Serializable",
	timeout: 12_000,
} as const;

function effect(
	sequence: string,
	identity: StreamIdentity = source,
): OutstandingEffect {
	return {
		reference: { source: identity, effectSequence: sequence },
		effectName: "post.index",
		effectVersion: 1,
		payloadFormat: "json-v1",
		encodedInput: '{"title":"a\'; --","views":100}',
	};
}

async function execute(
	client: PostgresRawClient,
	prepared: ReturnType<typeof prepareEffectRegistration>,
) {
	const rows = z
		.tuple([z.unknown()])
		.parse(
			await client.$queryRawUnsafe(
				`WITH ${prepared.ctes} ${prepared.resultSql}`,
				...prepared.parameters,
			),
		);
	return prepared.validate(rows[0]);
}

function registration(
	schema: string,
	sequences: readonly string[],
	identity = source,
) {
	return prepareEffectRegistration({
		schema,
		source: identity,
		registrations: sequences.map((sequence) => effect(sequence, identity)),
	});
}

test("retains a contiguous frontier after unordered completion and deduplicates fixed-coordinate replay", async ({
	database,
}) => {
	const client = database.client("effects");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	const register = (sequences: readonly string[]) =>
		client.$transaction(
			(tx) => execute(tx, registration(schema, sequences)),
			transactionOptions,
		);
	expect(await storage.read(client, effect("1").reference)).toEqual({
		status: "notRegistered",
	});
	expect(
		await client.$transaction(
			(tx) => storage.guardCompletion(tx, effect("1")),
			transactionOptions,
		),
	).toBe("notRegistered");
	expect(await register(["1", "2", "3"])).toHaveLength(3);
	expect(await storage.read(client, effect("1").reference)).toEqual({
		status: "outstanding",
		effect: effect("1"),
	});
	// Later effects can finish first, without touching the source journal.
	expect(
		await client.$transaction(
			(tx) => storage.guardCompletion(tx, effect("3")),
			transactionOptions,
		),
	).toBe("completed");
	expect(await register(["1", "2", "3"])).toEqual([]);
	expect(await storage.read(client, effect("3").reference)).toEqual({
		status: "alreadyCompleted",
	});
	expect(await storage.read(client, effect("4").reference)).toEqual({
		status: "notRegistered",
	});
	expect(new Set(await register(["2", "3", "4", "5"]))).toEqual(
		new Set([effect("4").reference, effect("5").reference]),
	);
	for (const sequence of ["2", "5", "1", "4"]) {
		await client.$transaction(
			(tx) => storage.guardCompletion(tx, effect(sequence)),
			transactionOptions,
		);
	}
	expect((await storage.readPending(client)).effects).toEqual([]);
	// Covered, deleted rows cannot verify their old contents; the authoritative
	// source is non-forking. They must still never be recreated.
	expect(
		await client.$transaction(
			(tx) =>
				execute(
					tx,
					prepareEffectRegistration({
						schema,
						source,
						registrations: [
							{ ...effect("3"), encodedInput: '{"changed":true}' },
						],
					}),
				),
			transactionOptions,
		),
	).toEqual([]);
	const journal = createJournalStorage(schema);
	const next = await client.$transaction(async (tx) => {
		await journal.append(tx, {
			occurrenceId: randomUUID(),
			streamName: source.streamName,
			streamKey: source.streamKey,
			eventName: "post.draft",
			eventVersion: 1,
			payloadFormat: "json-v1",
			encodedInput: "{}",
		});
		return execute(
			tx,
			prepareNewEffects({
				schema,
				source,
				effects: [
					{ ...effect("1"), effectName: "post.message", effectVersion: 2 },
					effect("1"),
				],
			}),
		);
	}, transactionOptions);
	expect(new Set(next)).toEqual(
		new Set([effect("6").reference, effect("7").reference]),
	);
	expect(
		(
			await database.admin.query(
				`SELECT registered_through FROM "${schema}".effect_frontier`,
			)
		).rows,
	).toEqual([{ registered_through: "7" }]);
	expect(await storage.read(client, effect("6").reference)).toMatchObject({
		status: "outstanding",
		effect: { effectName: "post.message", effectVersion: 2 },
	});
});

test("rejects gaps, duplicate or out-of-order positions, conflicting content and guard mismatches atomically", async ({
	database,
}) => {
	const client = database.client("rejection");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	const register = (prepared: ReturnType<typeof prepareEffectRegistration>) =>
		client.$transaction((tx) => execute(tx, prepared), transactionOptions);
	await expect(register(registration(schema, ["2"]))).rejects.toBeInstanceOf(
		EffectRegistrationError,
	);
	expect(
		(await database.admin.query(`SELECT * FROM "${schema}".effect_frontier`))
			.rows,
	).toEqual([]);
	await register(registration(schema, ["1", "2"]));
	for (const sequences of [
		["1", "1", "3"],
		["2", "1", "3"],
		["1", "4"],
		["3", "5"],
	]) {
		await expect(
			register(registration(schema, sequences)),
		).rejects.toBeInstanceOf(EffectRegistrationError);
	}
	for (const patch of [
		{ effectName: "post.other" },
		{ effectVersion: 2 },
		{ encodedInput: '{"title":"different"}' },
		// Semantically equivalent JSON still has different immutable bytes.
		{ encodedInput: '{"views":100,"title":"a\'; --"}' },
	]) {
		await expect(
			register(
				prepareEffectRegistration({
					schema,
					source,
					registrations: [{ ...effect("1"), ...patch }, effect("3")],
				}),
			),
		).rejects.toBeInstanceOf(EffectRegistrationError);
		await expect(
			client.$transaction(
				(tx) => storage.guardCompletion(tx, { ...effect("1"), ...patch }),
				transactionOptions,
			),
		).rejects.toBeInstanceOf(EffectIdentityError);
	}
	expect(
		(
			await database.admin.query(
				`SELECT registered_through FROM "${schema}".effect_frontier`,
			)
		).rows,
	).toEqual([{ registered_through: "2" }]);
	expect((await storage.readPending(client)).effects).toEqual([
		effect("1"),
		effect("2"),
	]);
});

test("uses wide allocation arithmetic and explicit exhaustion without consuming source positions", async ({
	database,
}) => {
	const client = database.client("overflow");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	await client.$transaction(
		(tx) => execute(tx, registration(schema, ["1"])),
		transactionOptions,
	);
	await client.$transaction(
		(tx) => storage.guardCompletion(tx, effect("1")),
		transactionOptions,
	);
	// Administrative fixture only: model long-lived source progress without
	// manufacturing quadrillions of historical registrations.
	await database.admin.query(
		`UPDATE "${schema}".effect_frontier SET registered_through = 9007199254740992`,
	);
	const allocate = () =>
		client.$transaction(
			(tx) =>
				execute(
					tx,
					prepareNewEffects({
						schema,
						source,
						effects: [effect("1"), effect("1")],
					}),
				),
			transactionOptions,
		);
	expect(new Set(await allocate())).toEqual(
		new Set([
			effect("9007199254740993").reference,
			effect("9007199254740994").reference,
		]),
	);
	await database.admin.query(
		`UPDATE "${schema}".effect_frontier SET registered_through = 9223372036854775806`,
	);
	await expect(allocate()).rejects.toBeInstanceOf(EffectSequenceOverflowError);
	expect(
		(
			await database.admin.query(
				`SELECT registered_through FROM "${schema}".effect_frontier`,
			)
		).rows,
	).toEqual([{ registered_through: "9223372036854775806" }]);
	expect(
		(await storage.readPending(client)).effects.map(
			(row) => row.reference.effectSequence,
		),
	).toEqual(["9007199254740993", "9007199254740994"]);
});

test("scans outstanding coordinates with restartable keysets, isolation and hard bounds", async ({
	database,
}) => {
	const client = database.client("scan");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	for (const identity of [source, other]) {
		await client.$transaction(
			(tx) => execute(tx, registration(schema, ["1", "2", "3"], identity)),
			transactionOptions,
		);
	}
	const first = await storage.readPending(client, { limit: 2 });
	expect(first).toEqual({
		effects: [effect("1"), effect("2")],
		nextCursor: effect("2").reference,
	});
	await client.$transaction(
		(tx) => storage.guardCompletion(tx, effect("2")),
		transactionOptions,
	);
	// Cursor row may disappear; comparison uses its coordinates, not existence.
	const second = await storage.readPending(client, {
		limit: 2,
		after: effect("2").reference,
	});
	expect(second).toEqual({
		effects: [effect("3"), effect("1", other)],
		nextCursor: effect("1", other).reference,
	});
	await client.$transaction(
		(tx) => execute(tx, registration(schema, ["4"])),
		transactionOptions,
	);
	expect(
		await storage.readPending(client, {
			limit: 2,
			after: effect("1", other).reference,
		}),
	).toEqual({
		effects: [effect("2", other), effect("3", other)],
		nextCursor: null,
	});
	expect((await storage.readPending(client)).effects).toContainEqual(
		effect("4"),
	);
	expect(
		(await createEffectStorage(database.otherJournalSchema).readPending(client))
			.effects,
	).toEqual([]);
	for (const limit of [0, 1.5, 1_001])
		await expect(storage.readPending(client, { limit })).rejects.toThrow();
});

test.for(["commit", "rollback"] as const)(
	"concurrent completion has one committed winner after %s and no source/global lock",
	async (outcome, { database }) => {
		const firstClient = database.client("guard_first");
		const secondClient = database.client("guard_second");
		const independent = database.client("guard_independent");
		const schema = database.journalSchema;
		const storage = createEffectStorage(schema);
		await firstClient.$transaction(
			(tx) => execute(tx, registration(schema, ["1"])),
			transactionOptions,
		);
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const failure = new Error("Expected rollback");
		const first = settle(
			firstClient.$transaction(async (tx) => {
				const status = await storage.guardCompletion(tx, effect("1"));
				entered.resolve();
				await release.promise;
				if (outcome === "rollback") throw failure;
				return status;
			}, transactionOptions),
		);
		let second: ReturnType<typeof settle<string>> | undefined;
		try {
			await bounded(entered.promise);
			// An uncommitted deletion is invisible.
			expect(
				await storage.read(independent, effect("1").reference),
			).toMatchObject({ status: "outstanding" });
			second = settle(
				secondClient.$transaction(
					(tx) => storage.guardCompletion(tx, effect("1")),
					transactionOptions,
				),
			);
			await database.waitForBlock(
				"guard_first",
				"guard_second",
				"WITH observed AS MATERIALIZED",
			);
			await independent.$transaction(async (tx) => {
				await execute(tx, registration(schema, ["1"], other));
				expect(await storage.guardCompletion(tx, effect("1", other))).toBe(
					"completed",
				);
				// Registration on the same source also has no completion lock.
				await execute(tx, registration(schema, ["2"]));
			}, transactionOptions);
		} finally {
			release.resolve();
			await bounded(first);
			if (second !== undefined) await bounded(second);
		}
		const firstResult = await bounded(first);
		const secondResult =
			second !== undefined ? await bounded(second) : undefined;
		if (outcome === "commit") {
			// Concurrent frontier advancement can make either Serializable guard
			// abort. PostgreSQL chooses the victim; only committed winners count.
			const results = [firstResult, secondResult];
			expect(
				results.filter((result) => result?.status === "fulfilled"),
			).toEqual([{ status: "fulfilled", value: "completed" }]);
			for (const result of results) {
				if (result?.status === "rejected")
					expect(classifyFailure(result.reason, "outputs")).toBe("retry");
			}
			expect(
				await secondClient.$transaction(
					(tx) => storage.guardCompletion(tx, effect("1")),
					transactionOptions,
				),
			).toBe("alreadyCompleted");
		} else {
			expect(firstResult).toEqual({ status: "rejected", reason: failure });
			expect(secondResult).toEqual({ status: "fulfilled", value: "completed" });
		}
		expect(await storage.read(firstClient, effect("1").reference)).toEqual({
			status: "alreadyCompleted",
		});
		expect(
			await storage.read(firstClient, effect("2").reference),
		).toMatchObject({ status: "outstanding" });
	},
);

test("rolls a provisional deletion back and rejects racing registration rather than duplicating positions", async ({
	database,
}) => {
	const firstClient = database.client("register_first");
	const secondClient = database.client("register_second");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	const prepared = registration(schema, ["1"]);
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const first = settle(
		firstClient.$transaction(async (tx) => {
			await execute(tx, prepared);
			entered.resolve();
			await release.promise;
		}, transactionOptions),
	);
	let second: ReturnType<typeof settle<readonly unknown[]>> | undefined;
	try {
		await bounded(entered.promise);
		second = settle(
			secondClient.$transaction(
				(tx) => execute(tx, prepared),
				transactionOptions,
			),
		);
		await database.waitForBlock(
			"register_first",
			"register_second",
			"WITH effect_baseline AS",
		);
	} finally {
		release.resolve();
		await bounded(first);
		if (second !== undefined) await bounded(second);
	}
	expect((await bounded(first)).status).toBe("fulfilled");
	const result = second !== undefined ? await bounded(second) : undefined;
	expect(result?.status).toBe("rejected");
	if (result?.status === "rejected")
		expect(classifyFailure(result.reason, "outputs")).toBe("retry");
	expect(
		await secondClient.$transaction(
			(tx) => execute(tx, prepared),
			transactionOptions,
		),
	).toEqual([]);
	await expect(
		firstClient.$transaction(async (tx) => {
			expect(await storage.guardCompletion(tx, effect("1"))).toBe("completed");
			throw new Error("Follow-up staging failed");
		}, transactionOptions),
	).rejects.toThrow("Follow-up staging failed");
	expect(await storage.read(firstClient, effect("1").reference)).toEqual({
		status: "outstanding",
		effect: effect("1"),
	});
});

test("composes parameter offsets and requires valid admission results before commit", async ({
	database,
}) => {
	const client = database.client("results");
	const schema = database.journalSchema;
	const storage = createEffectStorage(schema);
	const prepared = prepareEffectRegistration({
		schema,
		source,
		registrations: [effect("1")],
		parameterOffset: 1,
	});
	const sql = `WITH unrelated AS (SELECT $1::text AS value), ${prepared.ctes} ${prepared.resultSql}`;
	for (const invalid of [
		{
			status: "accepted",
			previous: "0",
			advanced: 0,
			references: [effect("1").reference],
		},
		{ status: "accepted", previous: "0", advanced: 1, references: [] },
		{
			status: "accepted",
			previous: "0",
			advanced: 1,
			references: [effect("1", other).reference],
		},
	]) {
		await expect(
			client.$transaction(async (tx) => {
				await tx.$queryRawUnsafe(
					sql,
					"unrelated parameter",
					...prepared.parameters,
				);
				// Simulate corrupted adapter results after real writes, not a second
				// registration implementation. Validation must abort the transaction.
				prepared.validate(invalid);
			}, transactionOptions),
		).rejects.toBeInstanceOf(EffectResultError);
		expect(await storage.read(client, effect("1").reference)).toEqual({
			status: "notRegistered",
		});
	}
	expect(
		await client.$transaction(async (tx) => {
			const rows = z
				.tuple([z.unknown()])
				.parse(
					await tx.$queryRawUnsafe(
						sql,
						"unrelated parameter",
						...prepared.parameters,
					),
				);
			return prepared.validate(rows[0]);
		}, transactionOptions),
	).toEqual([effect("1").reference]);
});
