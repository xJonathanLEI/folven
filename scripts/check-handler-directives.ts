import { glob, readFile } from "node:fs/promises";
import { resolve } from "node:path";

// Oxlint disable comments can suppress even the rule that forbids them.
// Check tokens independently, conservatively including tokens in strings.
const root = resolve(import.meta.dirname, "..");
for await (const path of glob(["**/*.handler.ts", "examples/**/*.ts"], {
	cwd: root,
	exclude: ["node_modules/**", "dist/**", "tests/fixtures/generated/**"],
})) {
	const source = await readFile(resolve(root, path), "utf8");
	for (const [index, line] of source.split("\n").entries()) {
		if (/\b(?:eslint|oxlint)-disable\b/u.test(line)) {
			console.error(
				`${path}:${index + 1}: Lint-disable tokens are forbidden in handler/example modules.`,
			);
			process.exitCode = 1;
		}
	}
}
