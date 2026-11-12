import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import { bounded, settle, test } from "../../tests/postgres.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import { classifyFailure } from "./failures.js";
import {
	createPostgresRuntime,
	PayloadEncodingError,
	RetryExhaustedError,
} from "./index.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const options = { maxWait: 5_000, timeout: 12_000 };

test.for([1, 3])(
	"real serialization aborts (%i) reconstruct schema input, roll back mutations and bound retries",
	async (aborts, { database }) => {
		const client = database.client("wire");
		await client.user.create({
			data: { id: "a", email: "a@example.test", name: "Author", age: 0 },
		});
		// Permissive input deliberately demonstrates that discarded fields remain
		// in the journal. Production callers should construct explicit payloads.
		const schema = z.object({
			id: z.string(),
			at: z.iso.datetime().transform((value) => new Date(value)),
			tags: z.array(z.string()),
		});
		const route = vi.fn((payload: z.output<typeof schema>) => ({
			id: payload.id,
		}));
		const event = defineEvent({
			name: "author.touch",
			version: 1,
			stream: author,
			schema,
			route,
		});
		const barriers = Array.from({ length: aborts }, () => ({
			entered: Promise.withResolvers<void>(),
			release: Promise.withResolvers<void>(),
		}));
		const input = {
			id: "a",
			at: "2026-01-01T00:00:00.000Z",
			tags: ["original"],
			discarded: "retained in schema input",
		};
		const encoded = JSON.stringify(input);
		const payloads: Array<z.output<typeof schema>> = [];
		const clients: unknown[] = [];
		const failures: unknown[] = [];
		const results: Array<{ at: Date; callback: () => bigint }> = [];
		const handler = defineHandler({
			event,
			handle: async ({ db, scope, event: payload }) => {
				const attempt = payloads.length;
				payloads.push(payload);
				clients.push(db);
				expect(payload).not.toBe(input);
				expect(payload.at).toEqual(new Date("2026-01-01T00:00:00.000Z"));
				expect(payload.tags).toEqual(["original"]);
				expect(payload).not.toHaveProperty("discarded");
				// Establish a real Serializable snapshot, then let another connection
				// change this row. The subsequent update must abort this attempt.
				const before = await db.user.findUniqueOrThrow({
					where: scope.user.unique,
					select: { age: true },
				});
				expect(before.age).toBe(attempt * 10);
				const barrier = barriers[attempt];
				if (barrier) {
					barrier.entered.resolve();
					await barrier.release.promise;
				}
				payload.tags.push("failed-attempt mutation");
				payload.at.setTime(0);
				try {
					await db.user.update({
						where: scope.user.unique,
						data: { age: { increment: 1 } },
					});
				} catch (error) {
					failures.push(error);
					throw error;
				}
				const result = { at: payload.at, callback: () => 7n };
				results.push(result);
				return result;
			},
		});
		const registry = definitions.registry({
			streams: [author],
			events: [event],
			handlers: [handler],
		});
		const runtime = createPostgresRuntime({
			client,
			registry,
			schema: database.journalSchema,
			transaction: options,
		});
		const journalParameters: string[] = [];
		const allQueries: string[] = [];
		client.$on("query", ({ query, params }) => {
			allQueries.push(query);
			if (query.startsWith("INSERT") && query.includes('"event_log"')) {
				journalParameters.push(params);
			}
		});
		// Invalid wire input must not even begin a transaction on the real host.
		const getter = vi.fn(() => input.at);
		const invalid = { ...input };
		Object.defineProperty(invalid, "at", { get: getter });
		await expect(runtime.dispatch(event, invalid)).rejects.toBeInstanceOf(
			PayloadEncodingError,
		);
		expect(getter).not.toHaveBeenCalled();
		expect(allQueries).toEqual([]);
		const pending = settle(runtime.dispatch(event, input));
		try {
			for (const barrier of barriers) {
				await bounded(barrier.entered.promise);
				input.tags.push("caller mutation after preparation");
				await database.admin.query(
					`UPDATE "${database.commandSchema}".users SET age = age + 10 WHERE id = $1`,
					["a"],
				);
				barrier.release.resolve();
			}
			const completed = await bounded(pending);
			if (aborts === 3) {
				expect(completed.status).toBe("rejected");
				if (completed.status !== "rejected")
					throw new Error("Retries were not bounded");
				expect(completed.reason).toBeInstanceOf(RetryExhaustedError);
			} else {
				expect(completed.status).toBe("fulfilled");
				if (completed.status !== "fulfilled")
					throw new Error("The retried dispatch failed");
				expect(completed.value).toBe(results[0]);
				expect(completed.value.at).toEqual(new Date(0));
				expect(completed.value.callback()).toBe(7n);
			}
			const attempts = aborts === 3 ? 3 : 2;
			expect(payloads).toHaveLength(attempts);
			expect(new Set(payloads).size).toBe(attempts);
			expect(new Set(clients).size).toBe(attempts);
			expect(route).toHaveBeenCalledTimes(1);
			expect(failures).toHaveLength(aborts);
			for (const error of failures) {
				expect(error).toMatchObject({ code: "P2034" });
				expect(classifyFailure(error, "handler")).toBe("retry");
			}
			expect(journalParameters).toHaveLength(attempts);
			expect(new Set(journalParameters).size).toBe(1);
			expect(
				await client.user.findUnique({
					where: { id: "a" },
					select: { age: true },
				}),
			).toEqual({ age: aborts * 10 + (aborts === 3 ? 0 : 1) });
			const rows = z
				.array(z.object({ payload: z.string(), stream_sequence: z.string() }))
				.parse(
					(
						await database.admin.query(
							`SELECT payload, stream_sequence FROM "${database.journalSchema}".event_log`,
						)
					).rows,
				);
			expect(rows).toEqual(
				aborts === 3 ? [] : [{ payload: encoded, stream_sequence: "1" }],
			);
			if (rows[0]) {
				const reconstructed = schema.parse(JSON.parse(rows[0].payload));
				expect(reconstructed.at).toEqual(new Date("2026-01-01T00:00:00.000Z"));
				expect(reconstructed.tags).toEqual(["original"]);
			}
		} finally {
			for (const barrier of barriers) barrier.release.resolve();
			await pending;
		}
	},
);

test("new runtimes continue compound and singleton stream histories with explicit keys", async ({
	database,
}) => {
	const firstClient = database.client("initial");
	await firstClient.user.create({
		data: { id: "a", email: "a@example.test", name: "Author" },
	});
	await firstClient.post.create({
		data: {
			id: "p",
			authorId: "a",
			slug: "post",
			title: "Post",
			published: false,
			views: 0,
			createdAt: new Date("2026-01-01T00:00:00Z"),
			tags: [],
		},
	});
	const comments = definitions.stream({
		name: "comment",
		key: z.strictObject({ postId: z.string(), number: z.int().safe() }),
		owns: {
			comment: {
				kind: "unique",
				selector: "postId_number",
				fields: { postId: "postId", number: "number" },
			},
		},
	});
	const users = definitions.stream({
		name: "all-users",
		key: z.strictObject({}),
		owns: { user: { kind: "table" } },
	});
	const annotate = defineEvent({
		name: "comment.write",
		version: 1,
		stream: comments,
		schema: z.strictObject({
			postId: z.string(),
			number: z.int().safe(),
			body: z.string(),
			reversed: z.boolean(),
		}),
		route: (payload) =>
			payload.reversed
				? { number: payload.number, postId: payload.postId }
				: { postId: payload.postId, number: payload.number },
	});
	const count = defineEvent({
		name: "users.count",
		version: 1,
		stream: users,
		schema: z.strictObject({}),
		route: () => ({}),
	});
	const write = defineHandler({
		event: annotate,
		handle: ({ db, scope, event }) =>
			db.comment.upsert({
				where: scope.comment.unique,
				create: { ...scope.comment.fields, authorId: "a", body: event.body },
				update: { body: event.body },
				select: { body: true },
			}),
	});
	const read = defineHandler({
		event: count,
		handle: ({ db, scope }) => db.user.count({ where: scope.user.where }),
	});
	const registry = definitions.registry({
		streams: [comments, users],
		events: [annotate, count],
		handlers: [write, read],
	});
	for (const [index, client] of [
		firstClient,
		database.client("restarted"),
	].entries()) {
		const runtime = createPostgresRuntime({
			client,
			registry,
			schema: database.journalSchema,
		});
		const body = `Revision ${index}`;
		expect(
			await runtime.dispatch(annotate, {
				postId: "p",
				number: 1,
				body,
				reversed: index === 1,
			}),
		).toEqual({ body });
		expect(await runtime.dispatch(count, {})).toBe(1);
	}
	const rows = z
		.array(
			z.object({
				stream_name: z.string(),
				stream_key: z.string(),
				stream_sequence: z.string(),
			}),
		)
		.parse(
			(
				await database.admin.query(
					`SELECT stream_name, stream_key, stream_sequence FROM "${database.journalSchema}".event_log
		 ORDER BY stream_name, stream_sequence`,
				)
			).rows,
		);
	expect(rows).toEqual([
		{ stream_name: "all-users", stream_key: "{}", stream_sequence: "1" },
		{ stream_name: "all-users", stream_key: "{}", stream_sequence: "2" },
		{
			stream_name: "comment",
			stream_key: '{"number":1,"postId":"p"}',
			stream_sequence: "1",
		},
		{
			stream_name: "comment",
			stream_key: '{"number":1,"postId":"p"}',
			stream_sequence: "2",
		},
	]);
	// Caller-owned clients stay usable, including the one behind the old runtime.
	expect(
		await firstClient.comment.findUnique({
			where: { postId_number: { postId: "p", number: 1 } },
			select: { body: true },
		}),
	).toEqual({ body: "Revision 1" });
});
