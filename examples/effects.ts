import { createEffectRunner } from "../src/effects/index.js";
import { defineEffectHandler, eventInput } from "../src/index.js";
import {
	createPostgresEffectStore,
	type PostgresPublication,
} from "../src/postgres/index.js";
import type { PrismaClient } from "../tests/fixtures/generated/client.js";
import {
	authorStream,
	definitions,
	indexPost,
	postIndexed,
	renamePost,
} from "./definitions.js";
import { renameAndIndexPostHandler } from "./posts.handler.js";

// Application-owned interface and closure, not a Folven-injected service.
export interface PostIndexer {
	index(input: {
		postId: string;
		text: string;
		idempotencyKey: string;
	}): Promise<void>;
}

export function indexingHandler(indexer: PostIndexer) {
	return defineEffectHandler({
		effect: indexPost,
		async handle({ effect, effectId }) {
			await indexer.index({
				postId: effect.postId,
				text: effect.text,
				idempotencyKey: effectId,
			});
			return {
				events: [
					eventInput(postIndexed, {
						authorId: effect.authorId,
						postId: effect.postId,
					}),
				],
			};
		},
	});
}

/** The caller assigns work and awaits attempts before closing this client. */
export function createIndexingExecutor(
	client: PrismaClient,
	schema: string,
	indexer: PostIndexer,
	publication?: PostgresPublication,
) {
	const registry = executorDefinitions(indexer);
	const store = createPostgresEffectStore({
		client,
		registry,
		schema,
		// Completion publishes event delivery, not another effect task.
		...(publication === undefined ? {} : { publication }),
	});
	return { store, runner: createEffectRunner({ registry, store }) };
}

// No external implementation is needed to dispatch renamePost and register work.
// Registry construction itself never runs either kind of handler.
export const producerDefinitions = definitions.registry({
	streams: [authorStream],
	events: [renamePost, postIndexed],
	handlers: [renameAndIndexPostHandler],
	effects: [indexPost],
});

export function executorDefinitions(indexer: PostIndexer) {
	return definitions.registry({
		streams: [authorStream],
		events: [postIndexed],
		effects: [indexPost],
		effectHandlers: [indexingHandler(indexer)],
	});
}
