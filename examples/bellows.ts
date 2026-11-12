import {
	definePublishTask,
	TaskSuccess,
	type WorkerFactory,
} from "@xjonathanlei/bellows";
import { initializePostgresSchema } from "@xjonathanlei/bellows/backends/postgres";
import { createPostgresProcessorTask } from "@xjonathanlei/bellows/cloudflare/postgres";
import { z } from "zod";
import {
	createBellowsDelivery,
	type DeliveryTaskReceipt,
} from "../src/bellows/index.js";
import { postgresSchemaStatements } from "../src/postgres/admin.js";
import {
	createPostgresPublisher,
	createPostgresRuntime,
} from "../src/postgres/index.js";
import type { PrismaClient } from "../tests/fixtures/generated/client.js";
import { handOffExampleWebhook, ingressRegistry } from "./postgres.js";
import { registry } from "./registry.js";

// Administrative setup only, once on fresh schemas in the command database.
// The caller creates these schemas first. Each client below manages independent
// User/Post/Comment tables; never use two journals to own the same command state.
export async function installExampleDelivery(
	admin: PrismaClient,
	databaseUrl: string,
) {
	await admin.$transaction(async (transaction) => {
		for (const schema of ["posts_events", "community_events"]) {
			for (const statement of postgresSchemaStatements({ schema })) {
				await transaction.$executeRawUnsafe(statement);
			}
		}
	});
	// Bellows administration uses its own connection, after schema creation commits.
	await initializePostgresSchema(databaseUrl, "tasks");
}

const userMaintenance = definePublishTask<{ userId: string }>(
	"user_maintenance",
	{
		encode: (input) =>
			JSON.stringify(
				z.strictObject({ userId: z.string().min(1) }).parse(input),
			),
		decode: (input) =>
			z.strictObject({ userId: z.string().min(1) }).parse(JSON.parse(input)),
	},
);
const maintenanceFactory = {
	task: userMaintenance,
	build() {
		return {
			async process() {
				// Stand-in for an unrelated caller-owned task.
				return TaskSuccess.done(undefined);
			},
		};
	},
} satisfies WorkerFactory<typeof userMaintenance>;

export function createExampleDeliveryHost(
	clients: { posts: PrismaClient; community: PrismaClient },
	notify?: (receipts: readonly DeliveryTaskReceipt[]) => void | Promise<void>,
) {
	const postsDelivery = createBellowsDelivery({
		schema: "tasks",
		taskName: "posts_delivery",
		// Return a promise to await notification, or register it with a bound
		// waitUntil and return void. Never republish or detach unowned work here.
		onTasksCommitted: (receipts) => notify?.(receipts),
	});
	const communityDelivery = createBellowsDelivery({
		schema: "tasks",
		taskName: "community_delivery",
		onTasksCommitted: (receipts) => notify?.(receipts),
	});
	const posts = createPostgresRuntime({
		client: clients.posts,
		registry,
		schema: "posts_events",
		publication: postsDelivery.publication,
	});
	const community = createPostgresRuntime({
		client: clients.community,
		registry,
		schema: "community_events",
		publication: communityDelivery.publication,
	});
	const postsPublisher = createPostgresPublisher({
		registry: ingressRegistry,
		schema: "posts_events",
		publication: postsDelivery.publication,
	});
	// Each domain hosts both Post and Comment streams. The destination runtime
	// also publishes, so any follow-up emission gets its own asynchronous task.
	// For separate destination stores, supply that destination's ingest instead.
	const postsWorker = postsDelivery.createWorkerFactory({
		source: posts.outbox,
		deliver: posts.ingest,
	});
	const communityWorker = communityDelivery.createWorkerFactory({
		source: community.outbox,
		deliver: community.ingest,
	});
	return {
		posts,
		community,
		// Reuse the guarded transaction and post-commit notification in the manual
		// example. Here staging also persists a task in the same SQL statement.
		handOffWebhook(
			webhookId: string,
			input: Parameters<typeof handOffExampleWebhook>[3],
		) {
			return handOffExampleWebhook(
				clients.posts,
				postsPublisher,
				webhookId,
				input,
			);
		},
		// These are ordinary native Bellows factories, also usable with runTaskOnce.
		factories: [
			postsWorker.factory,
			communityWorker.factory,
			maintenanceFactory,
		],
		// Alternatively register these in a caller-owned Cloudflare processor.
		// Hosting must reject duplicate names and run recurring Bellows recovery.
		tasks: [
			createPostgresProcessorTask(postsWorker.factory),
			createPostgresProcessorTask(communityWorker.factory),
			createPostgresProcessorTask(maintenanceFactory),
		],
		async drain() {
			await Promise.all([postsWorker.drain(), communityWorker.drain()]);
		},
	};
}

// runHost owns processing/discovery and returns only after stopping admission.
// It must also settle unrelated tasks and host-owned notification promises.
// Bellows lease loss may stop waiting before a Folven business operation ends.
export async function runExampleDeliveryHost(
	clients: { posts: PrismaClient; community: PrismaClient },
	runHost: (
		host: ReturnType<typeof createExampleDeliveryHost>,
	) => Promise<void>,
) {
	const host = createExampleDeliveryHost(clients);
	try {
		await runHost(host);
	} finally {
		await host.drain();
		await Promise.all([
			clients.posts.$disconnect(),
			clients.community.$disconnect(),
		]);
	}
}
