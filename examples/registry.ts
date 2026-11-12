import { addCommentHandler, findCommentHandler } from "./comments.handler.js";
import {
	addComment,
	authorStream,
	commentStream,
	definitions,
	publishPost,
	readAuthor,
	readComment,
	renamePost,
} from "./definitions.js";
import {
	publishPostHandler,
	readAuthorHandler,
	renamePostHandler,
} from "./posts.handler.js";

export const registry = definitions.registry({
	streams: [authorStream, commentStream],
	events: [renamePost, readAuthor, addComment, readComment, publishPost],
	handlers: [
		renamePostHandler,
		publishPostHandler,
		readAuthorHandler,
		addCommentHandler,
		findCommentHandler,
	],
});
