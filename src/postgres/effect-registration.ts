import { z } from "zod";
import { parseStreamIdentity } from "./channel.js";
import {
	parseEffectReference,
	parseOutstandingEffect,
} from "./effect-protocol.js";
import {
	EffectRegistrationError,
	EffectResultError,
	EffectSequenceOverflowError,
} from "./errors.js";
import {
	qualifiedEffectFrontier,
	qualifiedOutstandingEffect,
} from "./schema.js";
import type {
	EffectReference,
	OutstandingEffect,
	StreamIdentity,
} from "./types.js";

/** Private in-memory input; no per-transition ordinal is persisted. */
export type CapturedEffect = Omit<OutstandingEffect, "reference">;

const resultSchema = z.strictObject({
	status: z.enum(["accepted", "overflow", "malformed", "gap", "conflict"]),
	previous: z.string().regex(/^(0|[1-9][0-9]{0,18})$/),
	advanced: z.number().int().min(0).max(1),
	references: z.array(z.unknown()),
});

/**
 * Both entrypoints feed this admission primitive. Only the input relation
 * differs: established coordinates never pass through the allocator.
 * Run in a Serializable transaction and propagate validation failures.
 */
function prepareRegistration(options: {
	readonly schema: string;
	readonly source: StreamIdentity;
	readonly entries: readonly OutstandingEffect[];
	readonly allocate: boolean;
	readonly parameterOffset: number;
}) {
	const { entries, allocate, parameterOffset } = options;
	if (!Number.isSafeInteger(parameterOffset) || parameterOffset < 0) {
		throw new EffectRegistrationError("Invalid registration parameter offset.");
	}
	const source = parseStreamIdentity(options.source);
	const frontier = qualifiedEffectFrontier(options.schema);
	const outstanding = qualifiedOutstandingEffect(options.schema);
	const name = `$${parameterOffset + 1}`;
	const key = `$${parameterOffset + 2}`;
	const batch = `$${parameterOffset + 3}`;
	const parameters: readonly unknown[] = Object.freeze([
		source.streamName,
		source.streamKey,
		JSON.stringify(
			entries.map((entry) => ({
				position: entry.reference.effectSequence,
				effect_name: entry.effectName,
				effect_version: entry.effectVersion,
				payload_format: entry.payloadFormat,
				payload: entry.encodedInput,
			})),
		),
	]);
	// All arithmetic before the explicit overflow check is numeric, not bigint.
	const ctes = `effect_baseline AS (
  SELECT COALESCE((SELECT registered_through FROM ${frontier}
    WHERE source_stream_name = ${name} AND source_stream_key = ${key}), 0)::numeric AS previous
), effect_registrations AS (
  SELECT ${allocate ? "effect_baseline.previous + item.ordinality" : "input.position"} AS position,
    input.effect_name, input.effect_version, input.payload_format, input.payload, item.ordinality
  FROM jsonb_array_elements(${batch}::jsonb) WITH ORDINALITY AS item(value, ordinality)
  CROSS JOIN LATERAL jsonb_to_record(item.value) AS input(
    position numeric, effect_name text, effect_version bigint, payload_format text, payload text
  ) CROSS JOIN effect_baseline
), effect_suffix AS (
  SELECT * FROM effect_registrations WHERE position > (SELECT previous FROM effect_baseline)
), effect_admission AS (
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM effect_registrations WHERE position > 9223372036854775807) THEN 'overflow'
    WHEN EXISTS (SELECT 1 FROM (
      SELECT position, LAG(position) OVER (ORDER BY ordinality) AS preceding FROM effect_registrations
    ) AS ordered WHERE position <= 0 OR position <= preceding) THEN 'malformed'
    WHEN EXISTS (SELECT 1 FROM effect_suffix HAVING COUNT(*) > 0 AND
      (MIN(position) <> (SELECT previous FROM effect_baseline) + 1
       OR MAX(position) - (SELECT previous FROM effect_baseline) <> COUNT(*))) THEN 'gap'
    WHEN EXISTS (
      SELECT 1 FROM effect_registrations AS incoming JOIN ${outstanding} AS retained
        ON retained.source_stream_name = ${name} AND retained.source_stream_key = ${key}
        AND retained.effect_sequence = incoming.position
      WHERE incoming.position <= (SELECT previous FROM effect_baseline)
        AND (retained.effect_name <> incoming.effect_name COLLATE "C"
          OR retained.effect_version <> incoming.effect_version
          OR retained.payload_format <> incoming.payload_format COLLATE "C"
          OR retained.payload <> incoming.payload COLLATE "C")
    ) THEN 'conflict'
    ELSE 'accepted' END AS status
), effect_advanced AS (
  INSERT INTO ${frontier} AS current (source_stream_name, source_stream_key, registered_through)
  SELECT ${name}, ${key}, CASE WHEN MAX(position) <= 9223372036854775807 THEN MAX(position)::bigint END
  FROM effect_suffix WHERE (SELECT status FROM effect_admission) = 'accepted'
  HAVING COUNT(*) > 0
  ON CONFLICT (source_stream_name, source_stream_key) DO UPDATE
    SET registered_through = EXCLUDED.registered_through
    WHERE current.registered_through = (SELECT previous FROM effect_baseline)
  RETURNING source_stream_name, source_stream_key, registered_through
), effect_inserted AS (
  INSERT INTO ${outstanding} (
    source_stream_name, source_stream_key, effect_sequence, effect_name, effect_version, payload_format, payload
  )
  SELECT advanced.source_stream_name, advanced.source_stream_key,
    CASE WHEN incoming.position <= 9223372036854775807 THEN incoming.position::bigint END,
    incoming.effect_name, incoming.effect_version, incoming.payload_format, incoming.payload
  FROM effect_suffix AS incoming CROSS JOIN effect_advanced AS advanced
  RETURNING source_stream_name, source_stream_key, effect_sequence
), effect_references AS (
  SELECT json_build_object(
    'source', json_build_object('streamName', source_stream_name, 'streamKey', source_stream_key),
    'effectSequence', effect_sequence::text
  ) AS reference FROM effect_inserted
)`;
	const resultSql = `SELECT
  (SELECT status FROM effect_admission) AS status,
  (SELECT previous::text FROM effect_baseline) AS previous,
  (SELECT COUNT(*)::integer FROM effect_advanced) AS advanced,
  COALESCE((SELECT json_agg(reference) FROM effect_references), '[]'::json) AS references`;
	return Object.freeze({
		ctes,
		parameters,
		resultSql,
		validate(value: unknown): readonly EffectReference[] {
			const result = resultSchema.safeParse(value);
			if (!result.success) {
				throw new EffectResultError("Invalid effect registration result.");
			}
			const row = result.data;
			if (row.status === "overflow") {
				throw new EffectSequenceOverflowError(
					"The source has exhausted PostgreSQL bigint effect positions.",
				);
			}
			if (row.status !== "accepted") {
				throw new EffectRegistrationError(
					"Effect registration is malformed, noncontiguous or conflicts with outstanding content.",
				);
			}
			const previous = BigInt(row.previous);
			if (previous > 9_223_372_036_854_775_807n) {
				throw new EffectResultError("Invalid effect registration frontier.");
			}
			const expected = new Set(
				entries
					.map((entry, index) =>
						allocate
							? previous + BigInt(index) + 1n
							: BigInt(entry.reference.effectSequence),
					)
					.filter((position) => position > previous)
					.map(String),
			);
			if (
				row.advanced !== (expected.size > 0 ? 1 : 0) ||
				row.references.length !== expected.size
			) {
				throw new EffectResultError(
					"Effect registration did not return its exact admitted suffix.",
				);
			}
			try {
				const references = row.references.map(parseEffectReference);
				for (const reference of references) {
					if (
						reference.source.streamName !== source.streamName ||
						reference.source.streamKey !== source.streamKey ||
						!expected.delete(reference.effectSequence)
					)
						throw new Error();
				}
				return Object.freeze(references);
			} catch {
				throw new EffectResultError(
					"Effect registration returned invalid source-owned references.",
				);
			}
		},
	});
}

/** Fixed, source-owned coordinates: covered positions never allocate again. */
export function prepareEffectRegistration(options: {
	readonly schema: string;
	readonly source: StreamIdentity;
	readonly registrations: readonly OutstandingEffect[];
	readonly parameterOffset?: number;
}) {
	const source = parseStreamIdentity(options.source);
	const entries = options.registrations.map(parseOutstandingEffect);
	for (const entry of entries) {
		if (
			entry.reference.source.streamName !== source.streamName ||
			entry.reference.source.streamKey !== source.streamKey
		) {
			throw new EffectRegistrationError(
				"An effect registration batch must belong to one source.",
			);
		}
	}
	return prepareRegistration({
		...options,
		source,
		entries,
		allocate: false,
		parameterOffset: options.parameterOffset ?? 0,
	});
}

/** Only for a newly admitted transition under its source journal fence. */
export function prepareNewEffects(options: {
	readonly schema: string;
	readonly source: StreamIdentity;
	readonly effects: readonly CapturedEffect[];
	readonly parameterOffset?: number;
}) {
	const source = parseStreamIdentity(options.source);
	// This placeholder validates content only. SQL allocates from durable progress.
	const entries = options.effects.map((effect) =>
		parseOutstandingEffect({
			...effect,
			reference: { source, effectSequence: "1" },
		}),
	);
	return prepareRegistration({
		...options,
		source,
		entries,
		allocate: true,
		parameterOffset: options.parameterOffset ?? 0,
	});
}
