# Folven

PostgreSQL-backed state machines for TypeScript, using Prisma and Zod.

Define a stream's owned state, route events to it, and write handlers with explicit, type-checked Prisma keys. Folven orders transitions within each stream and atomically commits state changes, event history, outgoing events, and effect requests.

**Early development:** private and unpublished. Breaking changes are expected; incompatible development state must be recreated.

## Quick start

Use a generated PostgreSQL Prisma client and TypeScript with `strict` and `exactOptionalPropertyTypes`. Before publication, build this repository with `pnpm install --frozen-lockfile && pnpm build`, then add it to your application with `pnpm add folven@file:../path/to/folven`. Install the Prisma and Zod peers listed in [`package.json`](package.json).

The example assumes an existing `Post` model with `id`, `authorId`, and `title` fields, and your application's root Prisma client:

```ts
import { z } from "zod";
import { createDefinitions, defineEvent, defineHandler } from "folven";
import { createPostgresRuntime } from "folven/postgres";
import type { Prisma } from "./generated/client.js";
import { prisma } from "./database.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

const author = definitions.stream({
	name: "author",
	key: z.strictObject({ authorId: z.string().min(1) }),
	owns: {
		post: { kind: "partition", fields: { authorId: "authorId" } },
	},
});

const renamePost = defineEvent({
	name: "post.rename",
	version: 1,
	stream: author,
	schema: z.strictObject({
		authorId: z.string(),
		postId: z.string(),
		title: z.string().min(1),
	}),
	route: (event) => ({ authorId: event.authorId }),
});

const handler = defineHandler({
	event: renamePost,
	handle({ event, db, scope }) {
		return db.post.update({
			where: { ...scope.post.where, id: event.postId },
			data: { title: event.title },
			select: { id: true, title: true },
		});
	},
});

const registry = definitions.registry({
	streams: [author],
	events: [renamePost],
	handlers: [handler],
});

const runtime = createPostgresRuntime({
	client: prisma,
	registry,
	schema: "folven",
});

// Run after the administrative setup below.
const post = await runtime.dispatch(renamePost, {
	authorId: "user-1",
	postId: "post-1",
	title: "Updated",
});
```

Before dispatching, create a dedicated metadata schema in the **same database** as command state and install Folven's tables once using administrative credentials:

```ts
import { postgresSchemaStatements } from "folven/postgres/admin";

await adminClient.$transaction(async (tx) => {
	await tx.$executeRawUnsafe('CREATE SCHEMA "folven"');
	for (const statement of postgresSchemaStatements({ schema: "folven" })) {
		await tx.$executeRawUnsafe(statement);
	}
});
```

Runtime construction never installs schemas. `dispatch` returns after commit; a thrown handler error rolls back the transition. Known transaction conflicts retry the handler, so keep command handlers deterministic and put external calls in effects.

## Events, effects, and delivery

| Operation | Use it for |
| --- | --- |
| `runtime.dispatch(event, input)` | Execute a new occurrence synchronously. |
| Handler `emit(event, input)` | Atomically request a later event, ordered within its source/target channel. |
| `publisher.prepare(...)` + `publisher.stage(tx, batch)` | Persist independent occurrences alongside caller-owned writes, such as webhook consumption. |
| Handler `requestEffect(effect, input)` | Atomically request external work. |

Effect handlers receive decoded input and a stable `effectId`, use application-supplied dependencies, and return `{ events: [eventInput(event, input)] }`. Completion atomically stages those follow-up events; completion retries do not repeat the external handler. External actions themselves are **not exactly-once**—use provider idempotency or reconciliation.

Durable work needs a delivery/execution host. The optional `folven/bellows` adapters publish tasks transactionally and supply workers for caller-owned Bellows hosting. They do not deploy infrastructure or start a scheduler.

See the compiled examples for:

- [Owned-state definitions](examples/definitions.ts) and [handlers](examples/posts.handler.ts).
- [PostgreSQL dispatch, transactional ingress, and manual handoff](examples/postgres.ts).
- [Effect definitions and execution](examples/effects.ts).
- [Bellows event delivery](examples/bellows.ts) and [effect hosting](examples/bellows-effects.ts).

## Important boundaries

- Use one authoritative ownership registry and journal namespace per managed command-state domain. Ownership supports whole tables, equality partitions, and unique rows; each model has one stream-family owner.
- The Prisma fence is a **compile-time guardrail**, not runtime authorization. Queries are never rewritten or given implicit keys. Casts, unrestricted imports, and database triggers/cascades can bypass ownership; enforce handler-module boundaries in your application.
- Inputs must survive Folven's strict JSON roundtrip. Supply timestamps and IDs explicitly; schemas, routing, and command handlers must not depend on time, randomness, or external I/O.
- Each public dispatch is a new occurrence. Do not blindly repeat it after an uncertain commit response. Durable delivery retries use the existing occurrence identity.
- Prepare independent ingress before caller-owned transaction retries, propagate staging failures, and call `notifyCommitted()` only after confirmed outer commit.
- Retain journal, pending-occurrence and channel history, and effect frontiers. General compaction and identity resets are unsupported.
- Callers own clients and worker lifetimes: stop admission and drain started work before closing resources. Authentication, preflight, query projections, and replay orchestration are outside Folven.

## Development

Use Node from `.nvmrc` and the pinned pnpm version:

```sh
pnpm install --frozen-lockfile
pnpm validate
TEST_DATABASE_URL=<disposable-postgresql-url> pnpm test:integration
```

Ordinary validation needs no database. Integration tests require an explicitly selected disposable PostgreSQL database. See [AGENTS.md](AGENTS.md) for contribution and packaging checks. After local library changes, rebuild and reinstall the consumer's path dependency.
