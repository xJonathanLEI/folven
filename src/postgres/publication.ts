import { PublicationResultError, RuntimeConfigurationError } from "./errors.js";
import type {
	PostgresPublication,
	PostgresPublicationFragment,
	PostgresEffectPublication,
} from "./types.js";

export function validatePublication(
	publication: PostgresPublication | PostgresEffectPublication | undefined,
	kind: "delivery" | "effect" = "delivery",
) {
	if (
		publication !== undefined &&
		(typeof publication?.prepare !== "function" ||
			(kind === "effect"
				? publication.kind !== "effect"
				: publication.kind !== undefined && publication.kind !== "delivery"))
	) {
		throw new RuntimeConfigurationError(
			"Publication requires a SQL preparation operation for the configured lane.",
		);
	}
}

/** Shared trusted SQL composition boundary; never performs I/O. */
export function preparePublication(
	publication: PostgresPublication | PostgresEffectPublication | undefined,
	parameterOffset: number,
	relation: string,
) {
	const fragment = publication?.prepare({
		source: { relation, payloadColumn: "encoded_payload" },
		parameterOffset,
	});
	if (
		publication !== undefined &&
		(!fragment ||
			typeof fragment.sql !== "string" ||
			!Array.isArray(fragment.parameters) ||
			typeof fragment.validate !== "function")
	) {
		throw new PublicationResultError(
			"Publication must prepare a SQL fragment and result validator.",
		);
	}
	return fragment;
}

export async function validatePublicationAction(
	fragment: PostgresPublicationFragment | undefined,
	rows: readonly unknown[],
	expectedCount: number,
) {
	const action = await fragment?.validate(rows, expectedCount);
	if (fragment !== undefined && typeof action !== "function") {
		throw new PublicationResultError(
			"Publication must return an attempt-local post-commit action.",
		);
	}
	if (fragment === undefined && rows.length !== 0) {
		throw new PublicationResultError(
			"Unconfigured publication returned unexpected receipts.",
		);
	}
	return action;
}

/** Observe every lane independently; committed work can never be replayed here. */
export function combinePublicationActions(
	...actions: readonly ((() => void | Promise<void>) | undefined)[]
) {
	if (actions.every((action) => action === undefined)) return undefined;
	return async () => {
		await Promise.all(
			actions.map(async (action) => {
				try {
					await action?.();
				} catch {
					// Publishers own diagnostics. One lane must not hide another.
				}
			}),
		);
	};
}
