import { defineConfig } from "prisma/config";

export default defineConfig({
	schema: "tests/fixtures/prisma/schema.prisma",
	...(process.env.TEST_DATABASE_URL
		? { datasource: { url: process.env.TEST_DATABASE_URL } }
		: {}),
});
