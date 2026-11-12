import { defineConfig } from "vitest/config";

if (!process.env.TEST_DATABASE_URL) {
	throw new Error(
		"TEST_DATABASE_URL is required: explicitly select a disposable PostgreSQL database for Folven integration tests.",
	);
}

export default defineConfig({
	test: {
		environment: "node",
		include: ["src/**/*.integration.test.ts"],
		fileParallelism: false,
		testTimeout: 20_000,
		hookTimeout: 20_000,
	},
});
