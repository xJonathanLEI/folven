import { z } from "zod";
import {
	parseEffectReference,
	parseOutstandingEffect,
} from "./effect-protocol.js";
import {
	EffectIdentityError,
	EffectResultError,
	PendingReadOptionsError,
} from "./errors.js";
import {
	qualifiedEffectFrontier,
	qualifiedOutstandingEffect,
} from "./schema.js";
import type {
	EffectCompletionGuard,
	EffectRead,
	EffectReference,
	OutstandingEffect,
	OutstandingEffectPage,
	OutstandingEffectReadOptions,
	PostgresBorrowedTransaction,
	PostgresRawClient,
} from "./types.js";

const optionsSchema = z.strictObject({
	limit: z.number().int().min(1).max(1_000).default(100),
	after: z.unknown().optional(),
});
const readRows = z.tuple([
	z.strictObject({
		covered: z.boolean(),
		effect: z.unknown(),
	}),
]);
const guardRows = z.tuple([
	z.strictObject({
		status: z.enum([
			"completed",
			"alreadyCompleted",
			"notRegistered",
			"conflict",
		]),
	}),
]);

/** Private primitives; constructors do no I/O and never own the client lifetime. */
export function createEffectStorage(schema: string) {
	const frontier = qualifiedEffectFrontier(schema);
	const table = qualifiedOutstandingEffect(schema);
	const envelope = `json_build_object(
    'reference', json_build_object(
      'source', json_build_object('streamName', current.source_stream_name, 'streamKey', current.source_stream_key),
      'effectSequence', current.effect_sequence::text),
    'effectName', current.effect_name, 'effectVersion', current.effect_version::double precision,
    'payloadFormat', current.payload_format, 'encodedInput', current.payload
  )`;
	const covered = `EXISTS (SELECT 1 FROM ${frontier}
    WHERE source_stream_name = $1 AND source_stream_key = $2 AND registered_through >= $3::bigint)`;
	const coordinate = `current.source_stream_name = $1 AND current.source_stream_key = $2 AND current.effect_sequence = $3::bigint`;
	return Object.freeze({
		async read(
			client: PostgresRawClient,
			reference: EffectReference,
		): Promise<EffectRead> {
			const parsed = parseEffectReference(reference);
			const rows = readRows.safeParse(
				await client.$queryRawUnsafe(
					`SELECT ${covered} AS covered,
  (SELECT ${envelope} FROM ${table} AS current WHERE ${coordinate}) AS effect`,
					parsed.source.streamName,
					parsed.source.streamKey,
					parsed.effectSequence,
				),
			);
			if (!rows.success)
				throw new EffectResultError("Invalid effect read result.");
			const [row] = rows.data;
			if (row.effect === null) {
				return Object.freeze({
					status: row.covered ? "alreadyCompleted" : "notRegistered",
				});
			}
			try {
				const effect = parseOutstandingEffect(row.effect);
				if (
					!row.covered ||
					effect.reference.source.streamName !== parsed.source.streamName ||
					effect.reference.source.streamKey !== parsed.source.streamKey ||
					effect.reference.effectSequence !== parsed.effectSequence
				)
					throw new Error();
				return Object.freeze({ status: "outstanding", effect });
			} catch {
				throw new EffectResultError(
					"The exact read did not return the referenced outstanding effect.",
				);
			}
		},
		async readPending(
			client: PostgresRawClient,
			options: OutstandingEffectReadOptions = {},
		): Promise<OutstandingEffectPage> {
			const parsed = optionsSchema.safeParse(options);
			if (!parsed.success)
				throw new PendingReadOptionsError(
					"Outstanding reads require a limit from 1 through 1000 and an optional effect cursor.",
				);
			const { limit } = parsed.data;
			const after =
				parsed.data.after === undefined
					? undefined
					: parseEffectReference(parsed.data.after);
			const result = await client.$queryRawUnsafe(
				`SELECT ${envelope} AS effect FROM ${table} AS current
WHERE ($1::text IS NULL OR (current.source_stream_name, current.source_stream_key, current.effect_sequence) > ($1, $2, $3::bigint))
ORDER BY current.source_stream_name, current.source_stream_key, current.effect_sequence LIMIT $4`,
				after?.source.streamName ?? null,
				after?.source.streamKey ?? null,
				after?.effectSequence ?? null,
				limit + 1,
			);
			const rows = z
				.array(z.strictObject({ effect: z.unknown() }))
				.max(limit + 1)
				.safeParse(result);
			if (!rows.success)
				throw new EffectResultError("Invalid outstanding effect scan result.");
			try {
				const all = rows.data.map((row) => parseOutstandingEffect(row.effect));
				const effects = Object.freeze(all.slice(0, limit));
				return Object.freeze({
					effects,
					nextCursor:
						all.length > limit ? (effects.at(-1)?.reference ?? null) : null,
				});
			} catch {
				throw new EffectResultError(
					"The outstanding scan returned invalid effects.",
				);
			}
		},
		/**
		 * Borrow an active transaction; a winning deletion is provisional until
		 * its owner stages follow-ups and commits. No source journal lock.
		 */
		async guardCompletion(
			transaction: PostgresBorrowedTransaction,
			effect: OutstandingEffect,
		): Promise<EffectCompletionGuard> {
			const parsed = parseOutstandingEffect(effect);
			const rows = guardRows.safeParse(
				await transaction.$queryRawUnsafe(
					`WITH observed AS MATERIALIZED (
  SELECT current.effect_name = $4 AND current.effect_version = $5::bigint
    AND current.payload_format = $6 AND current.payload = $7 AS matches
  FROM ${table} AS current WHERE ${coordinate}
), removed AS (
  DELETE FROM ${table} AS current WHERE ${coordinate}
    AND current.effect_name = $4 AND current.effect_version = $5::bigint
    AND current.payload_format = $6 AND current.payload = $7
  RETURNING current.effect_sequence
)
SELECT CASE
  WHEN EXISTS (SELECT 1 FROM removed) THEN 'completed'
  WHEN EXISTS (SELECT 1 FROM observed WHERE NOT matches) THEN 'conflict'
  WHEN ${covered} THEN 'alreadyCompleted'
  ELSE 'notRegistered' END AS status`,
					parsed.reference.source.streamName,
					parsed.reference.source.streamKey,
					parsed.reference.effectSequence,
					parsed.effectName,
					parsed.effectVersion,
					parsed.payloadFormat,
					parsed.encodedInput,
				),
			);
			if (!rows.success)
				throw new EffectResultError("Invalid effect completion guard result.");
			const status = rows.data[0].status;
			if (status === "conflict")
				throw new EffectIdentityError(
					"The outstanding effect does not match the expected immutable content.",
				);
			return status;
		},
	});
}
