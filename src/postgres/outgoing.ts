import { z } from "zod";
import {
	parseOutgoingEnvelope,
	parseOutgoingReference,
	parseStreamIdentity,
} from "./channel.js";
import { prepareNewEffects } from "./effect-registration.js";
import type { CapturedEffect } from "./effect-registration.js";
import {
	ChannelSequenceOverflowError,
	OutgoingIdentityError,
	OutgoingResultError,
	PendingReadOptionsError,
} from "./errors.js";
import {
	preparePublication,
	validatePublicationAction,
	combinePublicationActions,
} from "./publication.js";
import { qualifiedOutgoingEvent } from "./schema.js";
import type {
	CapturedOutput,
	OutgoingEnvelope,
	OutgoingRead,
	OutgoingReference,
	PendingPage,
	PendingReadOptions,
	PostgresRawClient,
	PostgresPublication,
	PostgresEffectPublication,
	StreamIdentity,
} from "./types.js";

const pendingOptions = z.strictObject({
	limit: z.number().int().min(1).max(1_000).default(100),
	after: z
		.strictObject({ source: z.unknown(), target: z.unknown() })
		.optional(),
});

function parsePendingOptions(value: unknown) {
	const parsed = pendingOptions.safeParse(value);
	if (!parsed.success) {
		throw new PendingReadOptionsError(
			"Pending reads require a limit from 1 through 1000 and an optional channel cursor.",
		);
	}
	const { limit, after } = parsed.data;
	return {
		limit,
		after: after
			? {
					source: parseStreamIdentity(after.source),
					target: parseStreamIdentity(after.target),
				}
			: undefined,
	};
}

function parsePendingRows(value: unknown): readonly OutgoingEnvelope[] {
	const rows = z.array(z.unknown()).safeParse(value);
	try {
		if (rows.success) return rows.data.map(parseOutgoingEnvelope);
	} catch {
		// Neither persisted payloads nor parser details belong in framework errors.
	}
	throw new OutgoingResultError(
		"The pending read did not return valid outgoing envelopes.",
	);
}

const exactRows = z
	.array(
		z.strictObject({
			envelope: z.unknown(),
			acknowledged: z.boolean(),
			hasPredecessor: z.boolean(),
		}),
	)
	.max(1);

const appendRows = z.tuple([
	z.strictObject({
		references: z.array(z.unknown()),
		publications: z.array(z.unknown()),
		overflow: z.boolean(),
		effects: z.unknown(),
		effectPublications: z.array(z.unknown()),
	}),
]);

/** Private source-output storage. Appending both lanes requires the journal fence. */
export function createOutgoingStorage(schema: string) {
	const table = qualifiedOutgoingEvent(schema);
	const channelColumns =
		"source_stream_name, source_stream_key, target_stream_name, target_stream_key";
	const readSql = `SELECT json_build_object(
  'source', json_build_object('streamName', current.source_stream_name, 'streamKey', current.source_stream_key),
  'target', json_build_object('streamName', current.target_stream_name, 'streamKey', current.target_stream_key),
  'channelSequence', current.channel_sequence::text, 'eventName', current.event_name,
  'eventVersion', current.event_version::double precision,
  'payloadFormat', current.payload_format, 'encodedInput', current.payload
) AS envelope, current.acknowledged,
EXISTS (
  SELECT 1 FROM ${table} AS earlier
  WHERE earlier.source_stream_name = current.source_stream_name
    AND earlier.source_stream_key = current.source_stream_key
    AND earlier.target_stream_name = current.target_stream_name
    AND earlier.target_stream_key = current.target_stream_key
    AND earlier.channel_sequence < current.channel_sequence AND NOT earlier.acknowledged
) AS "hasPredecessor"
FROM ${table} AS current
WHERE current.source_stream_name = $1 AND current.source_stream_key = $2
  AND current.target_stream_name = $3 AND current.target_stream_key = $4
  AND current.channel_sequence = $5::bigint`;
	const pendingSql = `SELECT DISTINCT ON (${channelColumns})
  json_build_object('streamName', source_stream_name, 'streamKey', source_stream_key) AS source,
  json_build_object('streamName', target_stream_name, 'streamKey', target_stream_key) AS target,
  channel_sequence::text AS "channelSequence", event_name AS "eventName",
  event_version::double precision AS "eventVersion",
  payload_format AS "payloadFormat", payload AS "encodedInput"
FROM ${table}
WHERE NOT acknowledged
  AND ($1::text IS NULL OR (${channelColumns}) > ($1, $2, $3, $4))
ORDER BY ${channelColumns}, channel_sequence
LIMIT $5`;
	const acknowledgeSql = `UPDATE ${table} SET acknowledged = true
WHERE source_stream_name = $1 AND source_stream_key = $2
  AND target_stream_name = $3 AND target_stream_key = $4
  AND channel_sequence = $5::bigint
  AND event_name = $6 AND event_version = $7::bigint
  AND payload_format = $8 AND payload = $9
RETURNING channel_sequence::text AS sequence`;
	// JSON is only the batch transport. Payload remains exact text, not JSONB.
	// Ordinality lives in this statement alone, never in retained metadata.
	const appendSql = `WITH captured AS (
  SELECT input.*, item.ordinality
  FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY AS item(value, ordinality)
  CROSS JOIN LATERAL jsonb_to_record(item.value) AS input(
    target_stream_name text, target_stream_key text,
    event_name text, event_version bigint, payload_format text, payload text
  )
), targets AS (
  SELECT DISTINCT target_stream_name COLLATE "C" AS target_stream_name,
    target_stream_key COLLATE "C" AS target_stream_key FROM captured
), tails AS (
  SELECT targets.*, COALESCE((
    SELECT MAX(channel_sequence) FROM ${table}
    WHERE source_stream_name = $1 AND source_stream_key = $2
      AND target_stream_name = targets.target_stream_name
      AND target_stream_key = targets.target_stream_key
  ), 0) AS tail FROM targets
), allocated AS (
  SELECT captured.*, tails.tail::numeric + ROW_NUMBER() OVER (
    PARTITION BY captured.target_stream_name COLLATE "C", captured.target_stream_key COLLATE "C"
    ORDER BY captured.ordinality
  ) AS position
  FROM captured JOIN tails
    ON captured.target_stream_name COLLATE "C" = tails.target_stream_name
    AND captured.target_stream_key COLLATE "C" = tails.target_stream_key
), exhausted AS (
  SELECT EXISTS (SELECT 1 FROM allocated WHERE position > 9223372036854775807) AS overflow
), inserted AS (
  INSERT INTO ${table} (
    source_stream_name, source_stream_key, target_stream_name, target_stream_key,
    channel_sequence, event_name, event_version, payload_format, payload
  )
  SELECT $1, $2, target_stream_name, target_stream_key,
    position::bigint, event_name, event_version, payload_format, payload
  FROM allocated WHERE NOT (SELECT overflow FROM exhausted)
  RETURNING source_stream_name, source_stream_key, target_stream_name, target_stream_key, channel_sequence
), outgoing_references AS (
  SELECT json_build_object(
    'source', json_build_object('streamName', source_stream_name, 'streamKey', source_stream_key),
    'target', json_build_object('streamName', target_stream_name, 'streamKey', target_stream_key),
    'channelSequence', channel_sequence::text
  ) AS reference FROM inserted
)`;

	return Object.freeze({
		async read(
			client: PostgresRawClient,
			reference: OutgoingReference,
		): Promise<OutgoingRead> {
			const parsed = parseOutgoingReference(reference);
			const rows = exactRows.safeParse(
				await client.$queryRawUnsafe(
					readSql,
					parsed.source.streamName,
					parsed.source.streamKey,
					parsed.target.streamName,
					parsed.target.streamKey,
					parsed.channelSequence,
				),
			);
			if (!rows.success) {
				throw new OutgoingResultError(
					"The exact read did not return valid outgoing metadata.",
				);
			}
			const row = rows.data[0];
			if (row === undefined) {
				throw new OutgoingIdentityError(
					"The referenced outgoing envelope is missing from retained history.",
				);
			}
			try {
				const envelope = parseOutgoingEnvelope(row.envelope);
				if (
					envelope.source.streamName === parsed.source.streamName &&
					envelope.source.streamKey === parsed.source.streamKey &&
					envelope.target.streamName === parsed.target.streamName &&
					envelope.target.streamKey === parsed.target.streamKey &&
					envelope.channelSequence === parsed.channelSequence
				) {
					return Object.freeze({ ...row, envelope });
				}
			} catch {
				// Do not expose persisted payloads or parser details.
			}
			throw new OutgoingResultError(
				"The exact read did not return the referenced outgoing envelope.",
			);
		},
		async readPending(
			client: PostgresRawClient,
			options: PendingReadOptions = {},
		): Promise<PendingPage> {
			const { limit, after } = parsePendingOptions(options);
			const rows = parsePendingRows(
				await client.$queryRawUnsafe(
					pendingSql,
					after?.source.streamName ?? null,
					after?.source.streamKey ?? null,
					after?.target.streamName ?? null,
					after?.target.streamKey ?? null,
					limit + 1,
				),
			);
			if (rows.length > limit + 1) {
				throw new OutgoingResultError("The pending read exceeded its bound.");
			}
			const envelopes = Object.freeze(rows.slice(0, limit));
			const last = envelopes.at(-1);
			return Object.freeze({
				envelopes,
				nextCursor:
					rows.length > limit && last
						? Object.freeze({ source: last.source, target: last.target })
						: null,
			});
		},
		async acknowledge(
			client: PostgresRawClient,
			envelope: OutgoingEnvelope,
		): Promise<void> {
			// Structural protocol validation only: acknowledgment must survive
			// removal/change of local event implementations and routing.
			const parsed = parseOutgoingEnvelope(envelope);
			const result = await client.$queryRawUnsafe(
				acknowledgeSql,
				parsed.source.streamName,
				parsed.source.streamKey,
				parsed.target.streamName,
				parsed.target.streamKey,
				parsed.channelSequence,
				parsed.eventName,
				parsed.eventVersion,
				parsed.payloadFormat,
				parsed.encodedInput,
			);
			const rows = z
				.array(z.strictObject({ sequence: z.literal(parsed.channelSequence) }))
				.max(1)
				.safeParse(result);
			if (!rows.success) {
				throw new OutgoingResultError(
					"The acknowledgment did not return a valid outgoing identity.",
				);
			}
			if (rows.data.length === 0) {
				throw new OutgoingIdentityError(
					"The outgoing envelope is missing or does not exactly match retained history.",
				);
			}
		},
		async appendOutputs(
			transaction: PostgresRawClient,
			source: StreamIdentity,
			outputs: readonly CapturedOutput[],
			publication?: PostgresPublication,
			effects: readonly CapturedEffect[] = [],
			effectPublication?: PostgresEffectPublication,
		) {
			if (outputs.length === 0 && effects.length === 0)
				return { references: Object.freeze([]), afterCommit: undefined };
			const parsedSource = parseStreamIdentity(source);
			const batch = JSON.stringify(
				outputs.map((output) => ({
					target_stream_name: output.target.streamName,
					target_stream_key: output.target.streamKey,
					event_name: output.eventName,
					event_version: output.eventVersion,
					payload_format: output.payloadFormat,
					payload: output.encodedInput,
				})),
			);
			const parameters: unknown[] = [
				parsedSource.streamName,
				parsedSource.streamKey,
				batch,
			];
			const registration =
				effects.length === 0
					? undefined
					: prepareNewEffects({
							schema,
							source: parsedSource,
							effects,
							parameterOffset: parameters.length,
						});
			parameters.push(...(registration?.parameters ?? []));
			const fragment = preparePublication(
				outputs.length === 0 ? undefined : publication,
				parameters.length,
				"delivery_inputs",
			);
			parameters.push(...(fragment?.parameters ?? []));
			const effectFragment = preparePublication(
				effects.length === 0 ? undefined : effectPublication,
				parameters.length,
				"effect_inputs",
			);
			parameters.push(...(effectFragment?.parameters ?? []));
			const sql = `${appendSql}${registration ? `,\n${registration.ctes}` : ""}${
				fragment
					? `,
delivery_inputs AS (
  SELECT reference::text AS encoded_payload FROM outgoing_references
), published_tasks AS (
  ${fragment.sql}
)`
					: ""
			}${
				effectFragment
					? `,
effect_inputs AS (
  SELECT reference::text AS encoded_payload FROM effect_references
), published_effect_tasks AS (
  ${effectFragment.sql}
)`
					: ""
			}
SELECT COALESCE((SELECT json_agg(reference) FROM outgoing_references), '[]'::json) AS references,
  ${fragment ? "COALESCE((SELECT json_agg(published_tasks) FROM published_tasks), '[]'::json)" : "'[]'::json"} AS publications,
  (SELECT overflow FROM exhausted) AS overflow,
  ${registration ? `(SELECT row_to_json(effect_result) FROM (${registration.resultSql}) AS effect_result)` : "NULL::json"} AS effects,
  ${effectFragment ? "COALESCE((SELECT json_agg(published_effect_tasks) FROM published_effect_tasks), '[]'::json)" : "'[]'::json"} AS "effectPublications"`;
			const rows = appendRows.safeParse(
				await transaction.$queryRawUnsafe(sql, ...parameters),
			);
			if (!rows.success) {
				throw new OutgoingResultError(
					"The outgoing batch did not return valid persistence results.",
				);
			}
			const [row] = rows.data;
			if (row.overflow) {
				throw new ChannelSequenceOverflowError(
					"The outgoing channel has exhausted PostgreSQL bigint positions.",
				);
			}
			if (row.references.length !== outputs.length) {
				throw new OutgoingResultError(
					"The outgoing batch did not return its exact inserted count.",
				);
			}
			let references: readonly OutgoingReference[];
			try {
				references = Object.freeze(row.references.map(parseOutgoingReference));
				const targets = new Map<string, number>();
				for (const output of outputs) {
					const key = JSON.stringify([
						output.target.streamName,
						output.target.streamKey,
					]);
					targets.set(key, (targets.get(key) ?? 0) + 1);
				}
				const identities = new Set<string>();
				for (const reference of references) {
					const key = JSON.stringify([
						reference.target.streamName,
						reference.target.streamKey,
					]);
					const remaining = targets.get(key) ?? 0;
					const identity = JSON.stringify(reference);
					if (
						reference.source.streamName !== parsedSource.streamName ||
						reference.source.streamKey !== parsedSource.streamKey ||
						remaining === 0 ||
						identities.has(identity)
					)
						throw new Error();
					targets.set(key, remaining - 1);
					identities.add(identity);
				}
			} catch {
				// Returned metadata is untrusted; never leak its contents.
				throw new OutgoingResultError(
					"The outgoing batch did not return valid source-owned references.",
				);
			}
			const effectReferences = registration?.validate(row.effects);
			if (registration === undefined && row.effects !== null) {
				throw new OutgoingResultError(
					"An empty effect lane returned registration metadata.",
				);
			}
			const deliveryAction = await validatePublicationAction(
				fragment,
				row.publications,
				references.length,
			);
			const effectAction = await validatePublicationAction(
				effectFragment,
				row.effectPublications,
				effectReferences?.length ?? 0,
			);
			const afterCommit = combinePublicationActions(
				deliveryAction,
				effectAction,
			);
			return { references, afterCommit };
		},
	});
}
