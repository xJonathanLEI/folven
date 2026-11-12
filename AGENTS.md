# Folven

Keep Folven independent of consuming applications and deployment infrastructure. Use generic fixtures such as users, posts, comments, and webhooks.

## Architecture

- Events own their stream and routing. Handlers receive the original transaction client through static fences and must supply explicit bound keys; never rewrite queries or inject predicates.
- Stream ordering is established by the append-first journal constraint. State, history, outgoing events, and effect registrations commit atomically. External effects run outside transactions; completion retries must not repeat external work.
- Independent ingress borrows caller-owned transactions. Stage errors must propagate; notification is permitted only after confirmed outer commit.
- Bellows adapters are optional and isolated behind `folven/bellows`. Callers own clients, scheduling, and lifecycle. Stop admission and drain started work before closing resources.
- Schema installation is administrative-only. During active development, update the fresh-install baseline directly and recreate disposable state after incompatible changes. Do not add historical migrations, compatibility fixtures, or automatic resets without an explicit compatibility commitment.
- Preserve exact event encoding, canonical identities, source frontiers, and deduplication semantics. Review the owning implementation and protocol tests before changing these contracts.

## Development

Use Node from `.nvmrc` and the pinned pnpm version. Run `pnpm install --frozen-lockfile`, then `pnpm validate`.

GitHub Actions runs validation and PostgreSQL 17 integration tests on pull requests and pushes to `master`; both jobs must pass.

Run `TEST_DATABASE_URL=<explicit-disposable-postgresql-url> pnpm test:integration` for persistence changes and release validation. Never use a shared or production database. Fixtures own their schemas and must drain work and close clients before cleanup.

Keep tests colocated and focused on architectural boundaries, concurrency, failure recovery, and public type contracts. Review test excessiveness before delivery; remove temporary probes rather than creating a second test subsystem. Do not weaken assertions or static fences to pass checks.

For packaging or public API changes, test a final-tree tarball in isolated consumers with their own generated Prisma clients, both with and without Bellows. Compile with `strict`, `exactOptionalPropertyTypes`, and `skipLibCheck: false`; check public imports, private-subpath exclusion, and relevant PostgreSQL workflows. Inspect the archive for production ESM/declarations only, plus manifest and README. Keep smoke tooling in scratch. Do not publish without an explicit request.
