import { expect, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../../tests/fixtures/generated/client.js";
import {
	bounded,
	provisionTestDatabase,
	settle,
	test,
} from "../../tests/postgres.js";
import { createDeliveryRunner, DeliveryError } from "../delivery/index.js";
import { createDefinitions, defineEvent, defineHandler } from "../index.js";
import {
	createPostgresRuntime,
	OutgoingIdentityError,
	type IngestionResult,
	type OutgoingEnvelope,
	type PendingPage,
} from "./index.js";
import type { PostgresRawClient, PostgresTransactionHost } from "./types.js";

const definitions = createDefinitions<Prisma.TransactionClient>();
const author = definitions.stream({
	name: "author",
	key: z.strictObject({ id: z.string() }),
	owns: { user: { kind: "unique", selector: "id", fields: { id: "id" } } },
});
const comment = definitions.stream({
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
const commentInput = z.strictObject({
	postId: z.string(),
	number: z.int(),
	body: z.string(),
});
const publish = defineEvent({
	name: "author.publish",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), comments: z.array(commentInput) }),
	route: (event) => ({ id: event.id }),
});
const add = defineEvent({
	name: "comment.add",
	version: 1,
	stream: comment,
	schema: commentInput.extend({ authorId: z.string() }),
	route: ({ postId, number }) => ({ postId, number }),
});
const receipt = defineEvent({
	name: "author.commentAccepted",
	version: 1,
	stream: author,
	schema: z.strictObject({ id: z.string(), body: z.string() }),
	route: ({ id }) => ({ id }),
});
const review = defineEvent({
	name: "comment.review",
	version: 1,
	stream: comment,
	schema: z.strictObject({ postId: z.string(), number: z.int() }),
	route: (event) => event,
});
const sourceRegistry = definitions.registry({
	streams: [author, comment],
	events: [publish, add, receipt],
	handlers: [
		defineHandler({
			event: publish,
			handle: async ({ db, scope, event, emit }) => {
				await db.user.update({
					where: scope.user.unique,
					data: { name: "Published" },
				});
				for (const input of event.comments) {
					emit(add, { ...input, authorId: event.id });
				}
				return { published: event.comments.length };
			},
		}),
		defineHandler({
			event: receipt,
			handle: ({ db, scope, event }) =>
				db.user.update({
					where: scope.user.unique,
					data: { nickname: event.body },
				}),
		}),
	],
});
const blocked = new Error("Comment requires manual review");
function destinationRegistry(followUp: boolean) {
	return definitions.registry({
		streams: [comment, author],
		events: [add, receipt, review],
		handlers: [
			defineHandler({
				event: add,
				handle: async ({ db, scope, event, emit }) => {
					if (event.body === "blocked") throw blocked;
					const previous = await db.comment.findUnique({
						where: scope.comment.unique,
					});
					await db.comment.upsert({
						where: scope.comment.unique,
						create: {
							...scope.comment.fields,
							body: event.body,
							authorId: event.authorId,
						},
						update: { body: `${previous?.body ?? ""}|${event.body}` },
					});
					if (followUp) {
						emit(receipt, { id: event.authorId, body: event.body });
						emit(review, scope.comment.fields);
					}
					return { opaque: () => 1n };
				},
			}),
			defineHandler({
				event: review,
				handle: async ({ db, scope }) => {
					const previous = await db.comment.findUniqueOrThrow({
						where: scope.comment.unique,
					});
					await db.comment.update({
						where: scope.comment.unique,
						data: { body: `${previous.body}|reviewed` },
					});
				},
			}),
		],
	});
}

async function seed(client: Prisma.TransactionClient) {
	for (const id of ["a", "b"]) {
		await client.user.create({
			data: { id, email: `${id}@example.test`, name: "Author" },
		});
	}
	for (const id of ["p0", "p1", "p2"]) {
		await client.post.create({
			data: {
				id,
				authorId: "a",
				slug: id,
				title: "Post",
				published: true,
				views: 0,
				createdAt: new Date("2026-01-01T00:00:00Z"),
				tags: [],
			},
		});
	}
}

function first(page: PendingPage): OutgoingEnvelope {
	expect(page.envelopes).toHaveLength(1);
	const envelope = page.envelopes[0];
	if (!envelope) throw new Error("Expected a pending envelope");
	return envelope;
}

test("bounded delivery advances one output, isolates channels and safely competes through independent clients", async ({
	database: sourceDatabase,
}) => {
	const destinationDatabase = await provisionTestDatabase();
	try {
		const sourceClient = sourceDatabase.client("source");
		const destinationClient = destinationDatabase.client("destination");
		await seed(sourceClient);
		await seed(destinationClient);
		const source = createPostgresRuntime({
			client: sourceClient,
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		const destination = (client: typeof destinationClient) =>
			createPostgresRuntime({
				client,
				registry: destinationRegistry(true),
				schema: destinationDatabase.journalSchema,
			});
		const firstDestination = destination(destinationClient);
		const secondDestination = destination(
			destinationDatabase.client("competing_destination"),
		);
		const otherSource = createPostgresRuntime({
			client: sourceDatabase.client("competing_source"),
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		await source.dispatch(publish, {
			id: "a",
			comments: [
				{ postId: "p1", number: 1, body: "first" },
				{ postId: "p1", number: 1, body: "second" },
				{ postId: "p2", number: 1, body: "independent" },
			],
		});
		const [head, independent] = (await source.outbox.readPending()).envelopes;
		if (!head || !independent) throw new Error("Expected two channels");
		const reference = {
			source: head.source,
			target: head.target,
			channelSequence: head.channelSequence,
		};
		const next = { ...reference, channelSequence: "2" };
		const deliver = vi.fn(firstDestination.ingest);
		const runner = createDeliveryRunner({ source: source.outbox, deliver });
		expect(await runner.attempt(next)).toEqual({
			status: "waiting",
			reason: "predecessor",
		});
		expect(deliver).not.toHaveBeenCalled();
		expect(
			await runner.attempt({
				source: independent.source,
				target: independent.target,
				channelSequence: independent.channelSequence,
			}),
		).toEqual({ status: "completed" });
		expect((await source.outbox.read(reference)).acknowledged).toBe(false);

		// Both attempts read the pending source before either starts ingestion.
		// No source transaction/claim spans this bounded application barrier.
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const responses: IngestionResult[] = [];
		let arrivals = 0;
		const competingRunner = (
			outbox: typeof source.outbox,
			target: typeof firstDestination,
		) =>
			createDeliveryRunner({
				source: outbox,
				deliver: async (envelope) => {
					if (++arrivals === 2) entered.resolve();
					await release.promise;
					const result = await target.ingest(envelope);
					responses.push(result);
					return result;
				},
			});
		const attempts = [
			settle(
				competingRunner(source.outbox, firstDestination).attempt(reference),
			),
			settle(
				competingRunner(otherSource.outbox, secondDestination).attempt(
					reference,
				),
			),
		];
		try {
			await bounded(entered.promise);
			expect((await source.outbox.read(reference)).acknowledged).toBe(false);
			release.resolve();
			expect(await bounded(Promise.all(attempts))).toEqual([
				{ status: "fulfilled", value: { status: "completed" } },
				{ status: "fulfilled", value: { status: "completed" } },
			]);
		} finally {
			release.resolve();
			await Promise.all(attempts);
		}
		expect(responses.map(({ status }) => status).sort()).toEqual([
			"alreadyProcessed",
			"processed",
		]);
		expect((await source.outbox.read(reference)).acknowledged).toBe(true);
		expect((await source.outbox.read(next)).acknowledged).toBe(false);
		expect(await runner.attempt(next)).toEqual({ status: "completed" });
		expect(deliver).toHaveBeenCalledTimes(2);
		expect(await runner.attempt(reference)).toEqual({ status: "completed" });
		expect(deliver).toHaveBeenCalledTimes(2);
		await expect(
			runner.attempt({ ...reference, channelSequence: "99" }),
		).rejects.toEqual(new DeliveryError("read"));
		expect(deliver).toHaveBeenCalledTimes(2);
		expect((await source.outbox.readPending()).envelopes).toEqual([]);
		expect(
			await destinationClient.comment.findMany({
				select: { postId: true, body: true },
				orderBy: { postId: "asc" },
			}),
		).toEqual([
			{ postId: "p1", body: "first|second" },
			{ postId: "p2", body: "independent" },
		]);
		// Follow-ups were persisted, not delivered recursively by the runner.
		expect(
			(await firstDestination.outbox.readPending()).envelopes,
		).toHaveLength(4);
		expect(
			(
				await destinationDatabase.admin.query(
					`SELECT count(*)::int AS count FROM "${destinationDatabase.journalSchema}".event_log`,
				)
			).rows,
		).toEqual([{ count: 3 }]);
	} finally {
		await destinationDatabase.dispose();
	}
});

test("bounded channel scans bypass blocked heads, restart for earlier work, and retain acknowledged tails", async ({
	database: sourceDatabase,
}) => {
	const destinationDatabase = await provisionTestDatabase();
	try {
		const sourceClient = sourceDatabase.client("source");
		const destinationClient = destinationDatabase.client("destination");
		await seed(sourceClient);
		await seed(destinationClient);
		const source = createPostgresRuntime({
			client: sourceClient,
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		const destination = createPostgresRuntime({
			client: destinationClient,
			registry: destinationRegistry(false),
			schema: destinationDatabase.journalSchema,
		});
		await source.dispatch(publish, {
			id: "a",
			comments: [
				{ postId: "p2", number: 1, body: "second channel" },
				{ postId: "p1", number: 1, body: "blocked" },
				{ postId: "p1", number: 1, body: "behind blocked" },
				{ postId: "p2", number: 1, body: "next" },
			],
		});
		await source.dispatch(publish, {
			id: "b",
			comments: [{ postId: "p1", number: 1, body: "independent source" }],
		});
		const page = await source.outbox.readPending({ limit: 1 });
		const head = first(page);
		expect(head.target.streamKey).toBe('{"number":1,"postId":"p1"}');
		expect(head.channelSequence).toBe("1");
		expect(page.nextCursor).not.toBeNull();
		await expect(destination.ingest(head)).rejects.toBe(blocked);
		// Listing does not execute targets, claim work or change pending heads.
		expect(await source.outbox.readPending({ limit: 1 })).toEqual(page);
		expect(await destinationClient.comment.count()).toBe(0);
		if (!page.nextCursor) throw new Error("Expected continuation");
		const secondPage = await source.outbox.readPending({
			limit: 1,
			after: page.nextCursor,
		});
		const second = first(secondPage);
		expect(second.target.streamKey).toBe('{"number":1,"postId":"p2"}');
		expect(await destination.ingest(second)).toEqual({ status: "processed" });
		await source.outbox.acknowledge(second);
		await source.outbox.acknowledge(second);
		for (const conflict of [
			{
				...second,
				// Equivalent schema input with different property order still
				// does not match the exact retained bytes.
				encodedInput: JSON.stringify({
					authorId: "a",
					postId: "p2",
					number: 1,
					body: "second channel",
				}),
			},
			{ ...second, eventName: "comment.other" },
			{ ...second, eventVersion: 2 },
			{ ...second, channelSequence: "99" },
			{
				...second,
				source: { ...second.source, streamKey: '{"id":"missing"}' },
			},
			{
				...second,
				target: { ...second.target, streamKey: '{"id":"missing"}' },
			},
		]) {
			await expect(source.outbox.acknowledge(conflict)).rejects.toBeInstanceOf(
				OutgoingIdentityError,
			);
		}
		// New work sorts before the existing cursor. It is not a watermark.
		await source.dispatch(publish, {
			id: "a",
			comments: [{ postId: "p0", number: 1, body: "new earlier channel" }],
		});
		if (!secondPage.nextCursor) throw new Error("Expected continuation");
		const lastPage = await source.outbox.readPending({
			limit: 1,
			after: secondPage.nextCursor,
		});
		const last = first(lastPage);
		expect(last.source.streamKey).toBe('{"id":"b"}');
		expect(lastPage.nextCursor).toBeNull();
		expect(await destination.ingest(last)).toEqual({ status: "processed" });
		await source.outbox.acknowledge(last);
		const restarted = await source.outbox.readPending();
		expect(
			restarted.envelopes.map((envelope) => [
				envelope.target.streamKey,
				envelope.channelSequence,
			]),
		).toEqual([
			['{"number":1,"postId":"p0"}', "1"],
			['{"number":1,"postId":"p1"}', "1"],
			['{"number":1,"postId":"p2"}', "2"],
		]);
		const next = restarted.envelopes[2];
		if (!next) throw new Error("Expected next channel head");
		expect(await destination.ingest(next)).toEqual({ status: "processed" });
		await source.outbox.acknowledge(next);
		await source.dispatch(publish, {
			id: "a",
			comments: [{ postId: "p2", number: 1, body: "after acknowledgments" }],
		});
		expect(
			(await source.outbox.readPending()).envelopes.at(-1)?.channelSequence,
		).toBe("3");
		expect(
			(
				await sourceDatabase.admin.query(
					`SELECT channel_sequence, acknowledged FROM "${sourceDatabase.journalSchema}".outgoing_event
         WHERE source_stream_key = $1 AND target_stream_key = $2 ORDER BY channel_sequence`,
					[second.source.streamKey, second.target.streamKey],
				)
			).rows,
		).toEqual([
			{ channel_sequence: "1", acknowledged: true },
			{ channel_sequence: "2", acknowledged: true },
			{ channel_sequence: "3", acknowledged: false },
		]);
	} finally {
		await destinationDatabase.dispose();
	}
});

test("independent authorities recover lost responses and acknowledgments through manual multi-hop and self handoffs", async ({
	database: sourceDatabase,
}) => {
	const destinationDatabase = await provisionTestDatabase();
	try {
		const sourceClient = sourceDatabase.client("source");
		const destinationClient = destinationDatabase.client("destination");
		await seed(sourceClient);
		await seed(destinationClient);
		const source = createPostgresRuntime({
			client: sourceClient,
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		const lost = new Error("Destination response lost after commit");
		let loseResponse = true;
		// A response-loss double, not an actual network fault.
		const host = {
			$connect: () => destinationClient.$connect(),
			$disconnect: () => destinationClient.$disconnect(),
			$queryRawUnsafe: (query: string, ...values: unknown[]) =>
				destinationClient.$queryRawUnsafe(query, ...values),
			async $transaction<Result>(
				run: (transaction: Prisma.TransactionClient) => Promise<Result>,
				options: {
					isolationLevel: "Serializable";
					maxWait?: number;
					timeout?: number;
				},
			): Promise<Result> {
				const result = await destinationClient.$transaction(run, options);
				if (loseResponse) {
					loseResponse = false;
					throw lost;
				}
				return result;
			},
		} satisfies PostgresTransactionHost<Prisma.TransactionClient> &
			PostgresRawClient;
		const destination = createPostgresRuntime({
			client: host,
			registry: destinationRegistry(true),
			schema: destinationDatabase.journalSchema,
		});
		await source.dispatch(publish, {
			id: "a",
			comments: [{ postId: "p1", number: 1, body: "Hello" }],
		});
		const outgoing = first(await source.outbox.readPending());
		expect(await destinationClient.comment.count()).toBe(0);
		// Explicitly probe an out-of-order delivery; do not acknowledge its head.
		expect(
			await destination.ingest({ ...outgoing, channelSequence: "2" }),
		).toEqual({
			status: "gap",
			expectedSequence: "1",
		});
		expect(first(await source.outbox.readPending())).toEqual(outgoing);
		expect((await destination.outbox.readPending()).envelopes).toEqual([]);
		await expect(destination.ingest(outgoing)).rejects.toBe(lost);
		expect(first(await source.outbox.readPending())).toEqual(outgoing);
		// Reconstruct both clients/runtimes. No in-process receipt or shared
		// transaction is needed to redeliver the durable envelope.
		await sourceClient.$disconnect();
		await destinationClient.$disconnect();
		const resumedSourceClient = sourceDatabase.client("resumed_source");
		const resumedDestinationClient = destinationDatabase.client(
			"resumed_destination",
		);
		const resumedSource = createPostgresRuntime({
			client: resumedSourceClient,
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		const resumedDestination = createPostgresRuntime({
			client: resumedDestinationClient,
			registry: destinationRegistry(true),
			schema: destinationDatabase.journalSchema,
		});
		const retained = first(await resumedSource.outbox.readPending());
		expect(retained).toEqual(outgoing);
		expect(await resumedDestination.ingest(retained)).toEqual({
			status: "alreadyProcessed",
		});
		// Acknowledgment omitted/lost before source commit: resending stays safe.
		expect(await resumedDestination.ingest(retained)).toEqual({
			status: "alreadyProcessed",
		});
		const acknowledgmentLost = new Error("Source acknowledgment response lost");
		const acknowledgmentHost = {
			$connect: () => resumedSourceClient.$connect(),
			$disconnect: () => resumedSourceClient.$disconnect(),
			$transaction: resumedSourceClient.$transaction.bind(resumedSourceClient),
			async $queryRawUnsafe(query: string, ...values: unknown[]) {
				await resumedSourceClient.$queryRawUnsafe(query, ...values);
				throw acknowledgmentLost;
			},
		};
		const uncertainSource = createPostgresRuntime({
			client: acknowledgmentHost,
			registry: sourceRegistry,
			schema: sourceDatabase.journalSchema,
		});
		await expect(uncertainSource.outbox.acknowledge(retained)).rejects.toBe(
			acknowledgmentLost,
		);
		await resumedSource.outbox.acknowledge(retained);
		expect((await resumedSource.outbox.readPending()).envelopes).toEqual([]);
		expect(
			await resumedDestinationClient.comment.findMany({
				select: { body: true },
			}),
		).toEqual([{ body: "Hello" }]);
		expect(await resumedSourceClient.comment.count()).toBe(0);
		const followUps = await resumedDestination.outbox.readPending();
		expect(followUps.envelopes).toHaveLength(2);
		const response = followUps.envelopes[0];
		const self = followUps.envelopes[1];
		if (!response || !self) throw new Error("Expected both follow-up channels");
		expect(response.source).toEqual(outgoing.target);
		expect(self.source).toEqual(self.target);
		expect(await resumedSource.ingest(response)).toEqual({
			status: "processed",
		});
		await resumedDestination.outbox.acknowledge(response);
		expect(await resumedDestination.ingest(self)).toEqual({
			status: "processed",
		});
		await resumedDestination.outbox.acknowledge(self);
		expect(await resumedDestination.ingest(self)).toEqual({
			status: "alreadyProcessed",
		});
		expect((await resumedDestination.outbox.readPending()).envelopes).toEqual(
			[],
		);
		expect(
			await resumedSourceClient.user.findUnique({
				where: { id: "a" },
				select: { name: true, nickname: true },
			}),
		).toEqual({ name: "Published", nickname: "Hello" });
		expect(
			await resumedDestinationClient.comment.findMany({
				select: { body: true },
			}),
		).toEqual([{ body: "Hello|reviewed" }]);
		expect(
			(
				await destinationDatabase.admin.query(
					`SELECT stream_sequence FROM "${destinationDatabase.journalSchema}".event_log ORDER BY stream_sequence`,
				)
			).rows,
		).toEqual([{ stream_sequence: "1" }, { stream_sequence: "2" }]);
		// Acknowledgment uses retained facts only, not current event registrations.
		const metadataFree = createPostgresRuntime({
			client: resumedSourceClient,
			schema: sourceDatabase.journalSchema,
			registry: definitions.registry({
				streams: [author],
				events: [],
				handlers: [],
			}),
		});
		await metadataFree.outbox.acknowledge(retained);
	} finally {
		await destinationDatabase.dispose();
	}
});
