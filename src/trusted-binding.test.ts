import { expect, it, vi } from "vitest";
import { z } from "zod";
import type { Prisma } from "../tests/fixtures/generated/client.js";
import { createDefinitions, parseStreamKey } from "./index.js";
import { bindScope, narrowPrisma } from "./trusted-binding.js";

const definitions = createDefinitions<Prisma.TransactionClient>();

it("binds the parsed key into frozen flat fields and exact scalar/compound selectors", () => {
	const author = definitions.stream({
		name: "author",
		key: z.strictObject({ authorId: z.string().trim().toLowerCase() }),
		owns: {
			user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
			post: { kind: "partition", fields: { authorId: "authorId" } },
		},
	});
	const key = parseStreamKey(author, { authorId: " USER-1 " });
	const scope = bindScope(author, key);
	expect(scope).toEqual({
		user: {
			where: { id: "user-1" },
			fields: { id: "user-1" },
			unique: { id: "user-1" },
		},
		post: { where: { authorId: "user-1" }, fields: { authorId: "user-1" } },
	});
	expect(Object.isFrozen(scope)).toBe(true);
	expect(Object.isFrozen(scope.user)).toBe(true);
	expect(Object.isFrozen(scope.user.fields)).toBe(true);
	expect(Object.isFrozen(scope.user.unique)).toBe(true);
	expect(Object.isFrozen(scope.post.where)).toBe(true);
	expect(Object.hasOwn(scope.post, "unique")).toBe(false);
	key.authorId = "changed";
	expect(scope.post.fields.authorId).toBe("user-1");

	const comment = definitions.stream({
		name: "comment",
		key: z.strictObject({ post: z.string(), sequence: z.number().int() }),
		owns: {
			comment: {
				kind: "unique",
				selector: "postId_number",
				fields: { postId: "post", number: "sequence" },
			},
		},
	});
	const compound = bindScope(
		comment,
		parseStreamKey(comment, { post: "p1", sequence: 3 }),
	);
	expect(compound.comment).toEqual({
		fields: { postId: "p1", number: 3 },
		where: { postId: "p1", number: 3 },
		unique: { postId_number: { postId: "p1", number: 3 } },
	});
	expect(Object.isFrozen(compound.comment.unique.postId_number)).toBe(true);
	const singleton = definitions.stream({
		name: "all-users",
		key: z.strictObject({}),
		owns: { user: { kind: "table" } },
	});
	expect(bindScope(singleton, parseStreamKey(singleton, {}))).toEqual({
		user: { where: {}, fields: {} },
	});
});

it("preserves the caller client, delegates, arguments and promise without executing SQL", async () => {
	const stream = definitions.stream({
		name: "author",
		key: z.strictObject({ authorId: z.string() }),
		owns: { post: { kind: "partition", fields: { authorId: "authorId" } } },
	});
	const result = Promise.resolve([{ id: "p1" }]);
	const findMany = vi.fn((_args: Prisma.PostFindManyArgs) => result);
	// A call-recording sentinel, not an ORM/backend. Real delegate signatures
	// and all result inference are checked against the generated client by tsc.
	const client = { post: { findMany } } as unknown as Prisma.TransactionClient;
	const token = Symbol("invocation");
	const scope = bindScope<typeof stream, typeof token>(
		stream,
		parseStreamKey(stream, { authorId: "u1" }),
	);
	const db = narrowPrisma<typeof stream, typeof token>(client);
	expect(db).toBe(client);
	expect(db.post).toBe(client.post);
	expect(db.post.findMany).toBe(findMany);
	expect(findMany).not.toHaveBeenCalled();
	const args = {
		where: { ...scope.post.where, title: { startsWith: "Hello" } },
		select: { id: true },
	} satisfies Prisma.PostFindManyArgs;
	const promise = db.post.findMany(args);
	expect(promise).toBe(result);
	expect(await promise).toEqual([{ id: "p1" }]);
	expect(findMany).toHaveBeenCalledExactlyOnceWith(args);
	expect(findMany.mock.calls[0]?.[0]).toBe(args);
	expect(args).toEqual({
		where: { authorId: "u1", title: { startsWith: "Hello" } },
		select: { id: true },
	});
});
