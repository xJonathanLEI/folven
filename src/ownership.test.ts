import { expect, it } from "vitest";
import { validateOwnership } from "./ownership.js";

it("rejects unsupported declaration shapes at the runtime boundary", () => {
	const invalid: readonly unknown[] = [
		null,
		[],
		{ post: { kind: "predicate" } },
		{ post: { kind: "partition", fields: { authorId: () => "id" } } },
		{ post: { kind: "partition", fields: { authorId: "missing" } } },
		{
			post: { kind: "partition", fields: { authorId: "id" }, predicate: true },
		},
		{ post: { kind: "unique", selector: "", fields: { id: "id" } } },
		{ post: { kind: "partition", fields: { [Symbol("field")]: "id" } } },
	];
	for (const owns of invalid) {
		expect(() => validateOwnership("author", owns, ["id"], true)).toThrow(
			'Stream "author"',
		);
	}
});
