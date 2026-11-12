import { defineHandler } from "../src/index.js";
import { addComment, readComment } from "./definitions.js";

export const addCommentHandler = defineHandler({
	event: addComment,
	handle({ event, db, scope }) {
		return db.comment.create({
			data: {
				...scope.comment.fields,
				authorId: event.authorId,
				body: event.body,
			},
			select: { postId: true, number: true, body: true },
		});
	},
});

export const findCommentHandler = defineHandler({
	event: readComment,
	handle({ db, scope }) {
		return db.comment.findUnique({
			where: scope.comment.unique,
			select: { body: true },
		});
	},
});
