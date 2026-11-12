import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import { executeOccurrence } from "./execution.js";
import { classifyFailure } from "./failures.js";
import { createPostgresRuntime, OccurrenceIdentityError } from "./index.js";
import { prepareOccurrence, reconstructPayload } from "./occurrence.js";
import { createOutgoingStorage } from "./outgoing.js";
import { createHandlerLookup } from "./registry.js";
import { createJournalStorage } from "./storage.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: {
		user: { kind: "unique", selector: "id", fields: { id: "id" } },
		post: { kind: "partition", fields: { authorId: "id" } },
	},
});
const draft = defineEvent({
	name: "post.draft",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), postId: z.string() }),
	route: (payload) => ({ id: payload.id }),
});
const reserve = defineEvent({
	name: "post.reserve",
	version: 2,
	stream: author,
	schema: draft.schema,
	route: draft.route,
});
const comments = definitions.stream({
	name: "comments",
	key: z.strictObject({ postId: z.string() }),
	owns: { comment: { kind: "partition", fields: { postId: "postId" } } },
});
const comment = defineEvent({
	name: "comment.request",
	version: 1,
	stream: comments,
	schema: z.strictObject({ postId: z.string(), body: z.string() }),
	route: (payload) => ({ postId: payload.postId }),
});
const options = { maxWait: 5_000, timeout: 12_000 };
const historySchema = z.array(
	z.object({
		occurrence_id: z.uuid(),
		stream_key: z.string(),
		stream_sequence: z.string(),
		event_name: z.string(),
		event_version: z.string(),
	}),
);

test.for(["commit", "rollback"] as const)(
	"append-first final-slot contention after %s uses the committed snapshot and has no holes",
	async (outcome, { database }) => {
		const firstClient = database.client("first");
		const secondClient = database.client("second");
		await firstClient.user.create({
			data: { id: "a", email: "a@example.test", name: "Author" },
		});
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const failure = new Error("Roll back the held draft");
		const reads: Array<{ postId: string; count: number }> = [];
		const handler = defineHandler({
			event: draft,
			handle: async ({ db, scope, event, emit }) => {
				emit(comment, { postId: "discussion", body: event.postId });
				const count = await db.post.count({
					where: { ...scope.post.where, published: false },
				});
				reads.push({ postId: event.postId, count });
				if (count > 0) return { status: "full" as const };
				await db.post.create({
					data: {
						...scope.post.fields,
						id: event.postId,
						slug: event.postId,
						title: "Draft",
						published: false,
						views: 0,
						createdAt: new Date("2026-01-01T00:00:00Z"),
						tags: [],
					},
				});
				if (event.postId === "first") {
					entered.resolve();
					await release.promise;
					if (outcome === "rollback") throw failure;
				}
				return { status: "created" as const };
			},
		});
		const otherHandler = defineHandler({
			event: reserve,
			handle: handler.handle,
		});
		const registry = definitions.registry({
			streams: [author, comments],
			events: [draft, reserve, comment],
			handlers: [handler, otherHandler],
		});
		const runtime = createPostgresRuntime({
			client: firstClient,
			registry,
			schema: database.journalSchema,
			transaction: options,
		});
		// Observe actual adapter errors without replacing SQL or transaction behavior.
		const journal = { ...createJournalStorage(database.journalSchema) };
		const append = vi.spyOn(journal, "append");
		const prepared = prepareOccurrence(otherHandler, {
			id: "a",
			postId: "second",
		});
		const first = settle(runtime.dispatch(draft, { id: "a", postId: "first" }));
		let second: ReturnType<typeof settle<unknown>> | undefined;
		try {
			await bounded(entered.promise);
			second = settle(
				executeOccurrence(
					secondClient,
					journal,
					prepared.occurrence,
					prepared.firstPayload,
					options,
					{
						events: createHandlerLookup(registry),
						outgoing: createOutgoingStorage(database.journalSchema),
					},
				),
			);
			await database.waitForBlock("first", "second");
			expect(reads).toEqual([{ postId: "first", count: 0 }]);
			expect(
				(
					await database.admin.query(
						`SELECT occurrence_id FROM "${database.journalSchema}".event_log`,
					)
				).rows,
			).toEqual([]);
			expect(
				await database.admin.query(
					`SELECT id FROM "${database.commandSchema}".posts`,
				),
			).toMatchObject({ rows: [] });
			release.resolve();
			expect(await bounded(first)).toEqual(
				outcome === "commit"
					? { status: "fulfilled", value: { status: "created" } }
					: { status: "rejected", reason: failure },
			);
			expect(await bounded(second)).toEqual({
				status: "fulfilled",
				value: {
					status: "processed",
					sequence: outcome === "commit" ? 2n : 1n,
					result: { status: outcome === "commit" ? "full" : "created" },
				},
			});
			expect(reads).toEqual([
				{ postId: "first", count: 0 },
				{ postId: "second", count: outcome === "commit" ? 1 : 0 },
			]);
			if (outcome === "commit") {
				expect(append).toHaveBeenCalledTimes(2);
				const aborted = append.mock.results[0];
				expect(aborted?.type).toBe("return");
				if (aborted?.type !== "return")
					throw new Error("Missing append attempt");
				const error = await aborted.value.catch((error: unknown) => error);
				expect(error).toMatchObject({
					code: "P2010",
					meta: { driverAdapterError: { cause: { originalCode: "40001" } } },
				});
				expect(classifyFailure(error, "append")).toBe("retry");
				expect(append.mock.calls[0]?.[1]).toBe(append.mock.calls[1]?.[1]);
			}
			const history = historySchema.parse(
				(
					await database.admin.query(
						`SELECT * FROM "${database.journalSchema}".event_log ORDER BY stream_sequence`,
					)
				).rows,
			);
			expect(
				history.map(({ stream_sequence, event_name, event_version }) => [
					stream_sequence,
					event_name,
					event_version,
				]),
			).toEqual(
				outcome === "commit"
					? [
							["1", "post.draft", "1"],
							["2", "post.reserve", "2"],
						]
					: [["1", "post.reserve", "2"]],
			);
			expect(
				await secondClient.post.findMany({ select: { id: true } }),
			).toEqual([{ id: outcome === "commit" ? "first" : "second" }]);
			expect(
				(
					await database.admin.query(
						`SELECT channel_sequence, payload FROM "${database.journalSchema}".outgoing_event ORDER BY channel_sequence`,
					)
				).rows,
			).toEqual(
				(outcome === "commit" ? ["first", "second"] : ["second"]).map(
					(body, index) => ({
						channel_sequence: String(index + 1),
						payload: JSON.stringify({ postId: "discussion", body }),
					}),
				),
			);
		} finally {
			release.resolve();
			await Promise.all([first, second]);
		}
	},
);

test("another author commits while the first handler is held, without a shared execution lock", async ({
	database,
}) => {
	const firstClient = database.client("held");
	const secondClient = database.client("independent");
	await firstClient.user.createMany({
		data: ["a", "b"].map((id) => ({
			id,
			email: `${id}@example.test`,
			name: id,
			age: 0,
		})),
	});
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const attempts: string[] = [];
	const handler = defineHandler({
		event: draft,
		handle: async ({ db, scope, event, emit }) => {
			attempts.push(event.id);
			emit(comment, { postId: "discussion", body: event.id });
			await db.user.update({
				where: scope.user.unique,
				data: { age: { increment: 1 } },
			});
			if (event.id === "a") {
				entered.resolve();
				await release.promise;
			}
			return event.id;
		},
	});
	const registry = definitions.registry({
		streams: [author, comments],
		events: [draft, comment],
		handlers: [handler],
	});
	const runtime = (client: typeof firstClient) =>
		createPostgresRuntime({
			client,
			registry,
			schema: database.journalSchema,
			transaction: options,
		});
	const first = settle(
		runtime(firstClient).dispatch(draft, { id: "a", postId: "unused" }),
	);
	let second: ReturnType<typeof settle<string>> | undefined;
	try {
		await bounded(entered.promise);
		second = settle(
			runtime(secondClient).dispatch(draft, { id: "b", postId: "unused" }),
		);
		expect(await bounded(second)).toEqual({ status: "fulfilled", value: "b" });
		const visible = historySchema.parse(
			(
				await database.admin.query(
					`SELECT * FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		);
		expect(
			visible.map(({ stream_key, stream_sequence }) => [
				stream_key,
				stream_sequence,
			]),
		).toEqual([['{"id":"b"}', "1"]]);
		release.resolve();
		expect(await bounded(first)).toEqual({ status: "fulfilled", value: "a" });
		// SSI may conservatively abort an attempt; callbacks need not run just once.
		expect(attempts.filter((id) => id === "a").length).toBeLessThanOrEqual(3);
		expect(attempts.filter((id) => id === "b").length).toBeLessThanOrEqual(3);
		expect(
			await secondClient.user.findMany({
				select: { id: true, age: true },
				orderBy: { id: "asc" },
			}),
		).toEqual([
			{ id: "a", age: 1 },
			{ id: "b", age: 1 },
		]);
		expect(
			(
				await database.admin.query(
					`SELECT stream_sequence FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ stream_sequence: "1" }, { stream_sequence: "1" }]);
		expect(
			(
				await database.admin.query(
					`SELECT source_stream_key, channel_sequence FROM "${database.journalSchema}".outgoing_event ORDER BY source_stream_key`,
				)
			).rows,
		).toEqual([
			{ source_stream_key: '{"id":"a"}', channel_sequence: "1" },
			{ source_stream_key: '{"id":"b"}', channel_sequence: "1" },
		]);
	} finally {
		release.resolve();
		await Promise.all([first, second]);
	}
});

test("concurrent established occurrences commit one effect; fresh execution acknowledges without recovering a result", async ({
	database,
}) => {
	const firstClient = database.client("original");
	const secondClient = database.client("duplicate");
	const queries: string[] = [];
	secondClient.$on("query", ({ query }) => queries.push(query));
	await firstClient.user.create({
		data: { id: "a", email: "a@example.test", name: "Author", age: 0 },
	});
	const entered = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	const invoked = vi.fn();
	const handler = defineHandler({
		event: draft,
		handle: async ({ db, scope, emit }) => {
			invoked();
			emit(comment, { postId: "discussion", body: "Once" });
			await db.user.update({
				where: scope.user.unique,
				data: { age: { increment: 1 } },
			});
			entered.resolve();
			await release.promise;
			return { callback: () => 7n };
		},
	});
	const registry = definitions.registry({
		streams: [author, comments],
		events: [draft, comment],
		handlers: [handler],
	});
	const emission = {
		events: createHandlerLookup(registry),
		outgoing: createOutgoingStorage(database.journalSchema),
	};
	const prepared = prepareOccurrence(handler, { id: "a", postId: "unused" });
	const first = settle(
		executeOccurrence(
			firstClient,
			createJournalStorage(database.journalSchema),
			prepared.occurrence,
			prepared.firstPayload,
			options,
			emission,
		),
	);
	let second: ReturnType<typeof settle<unknown>> | undefined;
	try {
		await bounded(entered.promise);
		second = settle(
			executeOccurrence(
				secondClient,
				createJournalStorage(database.journalSchema),
				prepared.occurrence,
				reconstructPayload(prepared.occurrence),
				options,
				emission,
			),
		);
		await database.waitForBlock("original", "duplicate");
		expect(invoked).toHaveBeenCalledTimes(1);
		release.resolve();
		const completed = await bounded(first);
		expect(completed.status).toBe("fulfilled");
		if (
			completed.status !== "fulfilled" ||
			completed.value.status !== "processed"
		) {
			throw new Error("The original occurrence did not commit");
		}
		expect(completed.value.result.callback()).toBe(7n);
		expect(await bounded(second)).toEqual({
			status: "fulfilled",
			value: { status: "alreadyProcessed" },
		});
		const lookup = queries.findIndex((query) =>
			query.startsWith("SELECT occurrence_id, event_name"),
		);
		const rollback = queries.lastIndexOf("ROLLBACK", lookup);
		expect(rollback).toBeGreaterThanOrEqual(0);
		expect(lookup).toBeGreaterThan(rollback);
		// No process-local cache: a fresh client, storage and prepared descriptor
		// acknowledge the durable occurrence through the private execution path.
		const laterClient = database.client("later");
		const later = {
			...prepareOccurrence(handler, { id: "a", postId: "unused" }).occurrence,
			occurrenceId: prepared.occurrence.occurrenceId,
		};
		expect(
			await executeOccurrence(
				laterClient,
				createJournalStorage(database.journalSchema),
				later,
				reconstructPayload(later),
				options,
				emission,
			),
		).toEqual({ status: "alreadyProcessed" });
		for (const change of [
			{ encodedInput: '{"id":"a","postId":"different"}' },
			{ streamKey: '{"id":"another"}' },
			{ eventName: "different.event" },
			{ eventVersion: 2 },
		]) {
			const changed = { ...later, ...change };
			await expect(
				executeOccurrence(
					laterClient,
					createJournalStorage(database.journalSchema),
					changed,
					reconstructPayload(changed),
					options,
					emission,
				),
			).rejects.toBeInstanceOf(OccurrenceIdentityError);
		}
		expect(invoked).toHaveBeenCalledTimes(1);
		expect(
			await laterClient.user.findUnique({
				where: { id: "a" },
				select: { age: true },
			}),
		).toEqual({ age: 1 });
		expect(
			(
				await database.admin.query(
					`SELECT occurrence_id FROM "${database.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ occurrence_id: prepared.occurrence.occurrenceId }]);
		expect(
			(
				await database.admin.query(
					`SELECT channel_sequence FROM "${database.journalSchema}".outgoing_event`,
				)
			).rows,
		).toEqual([{ channel_sequence: "1" }]);
	} finally {
		release.resolve();
		await Promise.all([first, second]);
	}
});
