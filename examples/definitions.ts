import { z } from "zod";
import { createDefinitions, defineEffect, defineEvent } from "../src/index.js";
import type { Prisma } from "../tests/fixtures/generated/client.js";

// Only composition imports the generated client type; handlers never do.
export const definitions = createDefinitions<Prisma.TransactionClient>();

export const authorStream = definitions.stream({
	name: "author",
	key: z.strictObject({ authorId: z.string().trim().min(1) }),
	owns: {
		user: { kind: "unique", selector: "id", fields: { id: "authorId" } },
		post: { kind: "partition", fields: { authorId: "authorId" } },
	},
});

export const indexPost = defineEffect({
	name: "post.index",
	version: 1,
	schema: z.strictObject({
		authorId: z.string(),
		postId: z.string(),
		text: z.string(),
	}),
});

export const postIndexed = defineEvent({
	name: "post.indexed",
	version: 1,
	stream: authorStream,
	schema: z.strictObject({ authorId: z.string(), postId: z.string() }),
	route: (event) => ({ authorId: event.authorId }),
});

export const publishPost = defineEvent({
	name: "post.publish",
	version: 1,
	stream: authorStream,
	route: (event) => ({ authorId: event.authorId }),
	schema: z.strictObject({
		authorId: z.string().min(1),
		postId: z.string().min(1),
		commentNumber: z.int().safe(),
		commentBody: z.string().min(1),
	}),
});

export const commentStream = definitions.stream({
	name: "comment",
	key: z.strictObject({ postId: z.string().min(1), number: z.int().safe() }),
	owns: {
		comment: {
			kind: "unique",
			selector: "postId_number",
			fields: { postId: "postId", number: "number" },
		},
	},
});

export const renamePost = defineEvent({
	name: "post.rename",
	version: 1,
	stream: authorStream,
	route: (event) => ({ authorId: event.authorId }),
	schema: z.strictObject({
		authorId: z.string().min(1),
		postId: z.string().min(1),
		title: z.string().trim().min(1),
	}),
});

export const readAuthor = defineEvent({
	name: "author.read",
	version: 1,
	stream: authorStream,
	route: (event) => ({ authorId: event.authorId }),
	schema: z.strictObject({ authorId: z.string().min(1) }),
});

export const addComment = defineEvent({
	name: "comment.add",
	version: 1,
	stream: commentStream,
	route: (event) => ({ postId: event.postId, number: event.number }),
	schema: z.strictObject({
		postId: z.string().min(1),
		number: z.int().safe(),
		authorId: z.string().min(1),
		body: z.string().min(1),
	}),
});

export const readComment = defineEvent({
	name: "comment.read",
	version: 1,
	stream: commentStream,
	route: (event) => ({ postId: event.postId, number: event.number }),
	schema: addComment.schema.pick({ postId: true, number: true }),
});
