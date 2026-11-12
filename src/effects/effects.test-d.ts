import { expectTypeOf } from "vitest";
import { producerDefinitions as registry } from "../../examples/effects.js";
import type { PrismaClient } from "../../tests/fixtures/generated/client.js";
import {
	createPostgresEffectStore,
	type PostgresEffectPublication,
	type EffectReference,
} from "../postgres/index.js";
import {
	createEffectRunner,
	type EffectStore,
	type EffectCompletionResult,
	type PreparedEffectCompletion,
} from "./index.js";

declare const client: PrismaClient;
declare const reference: EffectReference;
declare const effectPublication: PostgresEffectPublication;

const store = createPostgresEffectStore({
	client,
	registry,
	schema: "metadata",
});
expectTypeOf(store).toExtend<EffectStore>();
const runner = createEffectRunner({ registry, store });
expectTypeOf(runner.attempt(reference)).toEqualTypeOf<
	Promise<EffectCompletionResult>
>();
declare const proposal: PreparedEffectCompletion;
expectTypeOf(store.complete(proposal)).toEqualTypeOf<
	Promise<EffectCompletionResult>
>();
createPostgresEffectStore({
	client,
	registry,
	schema: "metadata",
	// @ts-expect-error Completion uses event-delivery publication, never effect tasks.
	publication: effectPublication,
});
// @ts-expect-error Proposals cannot be constructed by a consumer.
void store.complete({});
