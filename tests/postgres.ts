import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { PrismaPg } from "@prisma/adapter-pg";
import pg from "pg";
import { test as baseTest } from "vitest";
import { z } from "zod";
import { postgresSchemaStatements } from "../src/postgres/admin.js";
import { PrismaClient } from "./fixtures/generated/client.js";

const databaseUrl = z.url().refine((value) => {
	const url = new URL(value);
	return (
		(url.protocol === "postgresql:" || url.protocol === "postgres:") &&
		url.hostname.length > 0 &&
		url.pathname.length > 1
	);
});

/** Only explicitly selected disposable databases; no application migrations. */
export async function provisionTestDatabase() {
	const parsed = databaseUrl.safeParse(process.env.TEST_DATABASE_URL);
	if (!parsed.success) {
		throw new Error(
			"TEST_DATABASE_URL must explicitly select a disposable PostgreSQL database.",
		);
	}
	const connectionString = parsed.data;
	const prefix = `folven_test_${randomUUID().replaceAll("-", "")}`;
	const commandSchema = `${prefix}_command`;
	const journalSchema = `${prefix}_journal`;
	const otherJournalSchema = `${prefix}_other`;
	const ownedSchemas: string[] = [];
	const clients: Array<{ $disconnect(): Promise<void> }> = [];
	const drains: Array<() => Promise<void>> = [];
	const admin = new pg.Client({
		connectionString,
		connectionTimeoutMillis: 5_000,
		statement_timeout: 10_000,
		query_timeout: 12_000,
		application_name: `${prefix}_admin`,
	});
	const failures: unknown[] = [];
	admin.on("error", (error: unknown) => failures.push(error));

	async function dispose() {
		for (const drain of drains) {
			try {
				await drain();
			} catch (error) {
				failures.push(error);
			}
		}
		const closed = await Promise.allSettled(
			clients.map((client) => client.$disconnect()),
		);
		for (const result of closed) {
			if (result.status === "rejected") failures.push(result.reason);
		}
		try {
			for (const schema of ownedSchemas.toReversed()) {
				try {
					await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
				} catch (error) {
					failures.push(error);
				}
			}
		} finally {
			try {
				await admin.end();
			} catch (error) {
				failures.push(error);
			}
		}
		if (failures.length > 0) {
			throw new AggregateError(
				failures,
				"Folven test database cleanup failed.",
			);
		}
	}

	try {
		await admin.connect();
		await admin.query("BEGIN");
		for (const schema of [commandSchema, journalSchema, otherJournalSchema]) {
			await admin.query(`CREATE SCHEMA "${schema}"`);
			ownedSchemas.push(schema);
		}
		await admin.query("SELECT set_config('search_path', $1, true)", [
			`"${commandSchema}"`,
		]);
		await admin.query(
			await readFile(
				new URL("./fixtures/prisma/bootstrap.sql", import.meta.url),
				"utf8",
			),
		);
		for (const schema of [journalSchema, otherJournalSchema]) {
			for (const statement of postgresSchemaStatements({ schema })) {
				await admin.query(statement);
			}
		}
		await admin.query("COMMIT");
	} catch (error) {
		try {
			await admin.query("ROLLBACK");
		} catch (rollbackError) {
			failures.push(rollbackError);
		}
		failures.push(error);
		await dispose();
		throw error;
	}

	return {
		admin,
		commandSchema,
		journalSchema,
		otherJournalSchema,
		/** Opt-in administrative setup; manual-runtime tests need no Bellows import. */
		async installBellows() {
			const schema = `${prefix}_tasks`;
			await admin.query(`CREATE SCHEMA "${schema}"`);
			ownedSchemas.push(schema);
			const { initializePostgresSchema } =
				await import("@xjonathanlei/bellows/backends/postgres");
			const url = new URL(connectionString);
			url.searchParams.set("connect_timeout", "5");
			url.searchParams.set("options", "-c statement_timeout=10000");
			await initializePostgresSchema(url.toString(), schema);
			return schema;
		},
		/** Each fresh host owns its backend pools; drain business work before closing them. */
		async bellowsHost(schema: string) {
			if (!ownedSchemas.includes(schema)) {
				throw new Error("Bellows test hosts require a fixture-owned schema.");
			}
			const { PostgresExecutionBackend } =
				await import("@xjonathanlei/bellows/backends/postgres-execution");
			const { PostgresDiscoveryBackend } =
				await import("@xjonathanlei/bellows/backends/postgres-discovery");
			const url = new URL(connectionString);
			url.searchParams.set("connect_timeout", "5");
			url.searchParams.set("options", "-c statement_timeout=10000");
			const execution = await PostgresExecutionBackend.connect(url.toString(), {
				schema,
			});
			clients.push({ $disconnect: () => execution.close() });
			const discovery = await PostgresDiscoveryBackend.connect(url.toString(), {
				schema,
			});
			clients.push({ $disconnect: () => discovery.close() });
			return { execution, discovery };
		},
		drainBeforeDisconnect(drain: () => Promise<void>) {
			drains.push(drain);
		},
		/** Observe a specific blocked SQL operation, not elapsed time as proof. */
		async waitForBlock(
			blocker: string,
			contender: string,
			queryPrefix = `INSERT INTO "${journalSchema}"."event_log"`,
		) {
			const deadline = Date.now() + 5_000;
			while (Date.now() < deadline) {
				const { rows } = await admin.query<Record<string, unknown>>(
					`SELECT waiting.pid
					 FROM pg_stat_activity waiting, pg_stat_activity holding
					 WHERE waiting.application_name = $1
					   AND holding.application_name = $2
					   AND holding.pid = ANY(pg_blocking_pids(waiting.pid))
					   AND waiting.wait_event_type = 'Lock'
					   AND waiting.query LIKE $3`,
					[`${prefix}_${contender}`, `${prefix}_${blocker}`, `${queryPrefix}%`],
				);
				if (rows.length === 1) return;
				await delay(10);
			}
			throw new Error(
				"No expected PostgreSQL lock was observed before the deadline.",
			);
		},
		/** Each invocation owns an independent adapter/pool/connection. */
		client(label: string) {
			const client = new PrismaClient({
				adapter: new PrismaPg(
					{
						connectionString,
						max: 1,
						connectionTimeoutMillis: 5_000,
						statement_timeout: 10_000,
						application_name: `${prefix}_${label}`,
					},
					{ schema: commandSchema },
				),
				log: [{ emit: "event", level: "query" }],
			});
			clients.push(client);
			return client;
		},
		dispose,
	};
}

export const test = baseTest.extend<{
	database: Awaited<ReturnType<typeof provisionTestDatabase>>;
}>({
	database: async ({ task: _task }, use) => {
		const database = await provisionTestDatabase();
		try {
			await use(database);
		} finally {
			await database.dispose();
		}
	},
});

export async function bounded<Value>(promise: Promise<Value>): Promise<Value> {
	const timeout = new AbortController();
	try {
		return await Promise.race([
			promise,
			delay(5_000, undefined, { signal: timeout.signal }).then(() => {
				throw new Error("Timed out waiting for the runtime protocol barrier.");
			}),
		]);
	} finally {
		timeout.abort();
	}
}

/** Attach rejection handling immediately; failed assertions must still drain work. */
export function settle<Value>(promise: Promise<Value>) {
	return promise.then(
		(value) => ({ status: "fulfilled" as const, value }),
		(reason: unknown) => ({ status: "rejected" as const, reason }),
	);
}
