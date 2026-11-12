import type { PublishTaskDefinition } from "@xjonathanlei/bellows";
import { preparePostgresPublicationFromEncodedRows } from "@xjonathanlei/bellows/backends/postgres-publishing";
import { z } from "zod";
import { PublicationResultError } from "../postgres/errors.js";
import type {
	PostgresPublication,
	PostgresPublicationFragment,
} from "../postgres/types.js";
import type { DeliveryTaskReceipt } from "./types.js";

/** Bellows owns SQL and ID decoding; Folven checks the committed batch contract. */
export function publicationPreparer<Payload>(
	task: PublishTaskDefinition<Payload, undefined>,
	schema: string,
	notify: (receipts: readonly DeliveryTaskReceipt[]) => Promise<void>,
): PostgresPublication["prepare"] {
	return ({ source, parameterOffset }) => {
		const fragment = preparePostgresPublicationFromEncodedRows(task, {
			schema,
			source,
			parameterOffset,
		});
		return Object.freeze({
			sql: fragment.sql,
			parameters: fragment.parameters,
			validate(rows, expectedCount) {
				let committed: readonly DeliveryTaskReceipt[];
				try {
					const receipts = fragment.readReceipts(
						z.array(z.strictObject({ task_id: z.string() })).parse(rows),
					);
					if (
						receipts.length !== expectedCount ||
						new Set(receipts.map((receipt) => receipt.taskId)).size !==
							expectedCount ||
						receipts.some(
							(receipt) =>
								!Number.isSafeInteger(receipt.taskId) || receipt.taskId <= 0,
						)
					)
						throw new Error();
					committed = Object.freeze(
						receipts.map(({ taskId }) =>
							Object.freeze({ taskId, taskName: task.name }),
						),
					);
				} catch {
					throw new PublicationResultError(
						"Invalid Bellows publication identities or count.",
					);
				}
				return async () => {
					if (committed.length > 0) await notify(committed);
				};
			},
		} satisfies PostgresPublicationFragment);
	};
}
