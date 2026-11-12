import { defineHandler } from "../src/index.js";
import {
	addComment,
	indexPost,
	publishPost,
	readAuthor,
	renamePost,
} from "./definitions.js";

export const publishPostHandler = defineHandler({
	event: publishPost,
	async handle({ event, db, scope, emit }) {
		const post = await db.post.update({
			where: { ...scope.post.where, id: event.postId },
			data: { published: true },
			select: { id: true, published: true },
		});
		// Capture schema input, not a target invocation. The comment stays
		// pending until a separate composition explicitly hands it off.
		emit(addComment, {
			postId: event.postId,
			number: event.commentNumber,
			authorId: event.authorId,
			body: event.commentBody,
		});
		return post;
	},
});

/** Alternative rename implementation for the effects producer registry. */
export const renameAndIndexPostHandler = defineHandler({
	event: renamePost,
	async handle({ event, db, scope, requestEffect }) {
		const post = await db.post.update({
			where: { ...scope.post.where, id: event.postId },
			data: { title: event.title },
			select: { id: true, title: true },
		});
		requestEffect(indexPost, {
			authorId: event.authorId,
			postId: post.id,
			text: post.title,
		});
		return post;
	},
});

export const renamePostHandler = defineHandler({
	event: renamePost,
	handle({ event, db, scope }) {
		return db.post.update({
			where: { ...scope.post.where, id: event.postId },
			data: { title: event.title },
			select: { id: true, title: true },
		});
	},
});

export const readAuthorHandler = defineHandler({
	event: readAuthor,
	handle({ db, scope }) {
		return db.user.findUnique({
			where: scope.user.unique,
			select: { id: true, name: true },
		});
	},
});
