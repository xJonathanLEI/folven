import { z } from "zod";
import { JournalResultError } from "./errors.js";
import { qualifiedEventLog } from "./schema.js";
import type { JournalOccurrence, PostgresRawClient } from "./types.js";

const reservationRows = z
	.array(
		z.strictObject({
			stream_sequence: z.bigint().positive().max(9_223_372_036_854_775_807n),
			previous_incoming_sequence: z
				.bigint()
				.nonnegative()
				.max(9_223_372_036_854_775_807n),
		}),
	)
	.length(1);

const occurrenceRows = z
	.array(
		z
			.strictObject({
				occurrence_id: z.uuid(),
				event_name: z.string().min(1),
				event_version: z
					.bigint()
					.positive()
					.max(BigInt(Number.MAX_SAFE_INTEGER)),
				stream_name: z.string().min(1),
				stream_key: z.string().min(1),
				payload_format: z.literal("json-v1"),
				payload: z.string(),
				incoming_source_stream_name: z.string().min(1).nullable(),
				incoming_source_stream_key: z.string().min(1).nullable(),
				incoming_channel_sequence: z
					.bigint()
					.positive()
					.max(9_223_372_036_854_775_807n)
					.nullable(),
			})
			.refine(
				(row) =>
					(row.incoming_source_stream_name === null &&
						row.incoming_source_stream_key === null &&
						row.incoming_channel_sequence === null) ||
					(row.incoming_source_stream_name !== null &&
						row.incoming_source_stream_key !== null &&
						row.incoming_channel_sequence !== null),
			),
	)
	.max(2);

/** Private append-only metadata adapter. Construction never accesses a DB. */
export function createJournalStorage(schema: string) {
	const table = qualifiedEventLog(schema);
	// Only the validated identifier varies in SQL. Every event value is a
	// parameter, including the exact serialized input (never a JSONB rewrite).
	const appendSql = `INSERT INTO ${table} (
  occurrence_id, stream_name, stream_key, stream_sequence,
  event_name, event_version, payload_format, payload,
  incoming_source_stream_name, incoming_source_stream_key, incoming_channel_sequence
)
SELECT
  $1::uuid, $2, $3, COALESCE(MAX(stream_sequence), 0) + 1,
  $4, $5::bigint, $6, $7, $8, $9, $10::bigint
FROM ${table}
WHERE stream_name = $2 AND stream_key = $3
RETURNING stream_sequence`;
	// The statement snapshot excludes its own insert but includes earlier writes
	// in this transaction. The stream-position constraint still fences execution.
	const incomingAppendSql = `WITH appended AS (
${appendSql}
)
SELECT appended.stream_sequence, (
  SELECT COALESCE(MAX(incoming_channel_sequence), 0)
  FROM ${table}
  WHERE incoming_source_stream_name = $8 AND incoming_source_stream_key = $9
    AND stream_name = $2 AND stream_key = $3
) AS previous_incoming_sequence
FROM appended`;
	const externalAppendSql = `${appendSql}, 0::bigint AS previous_incoming_sequence`;

	return Object.freeze({
		/** Only after a failed occurrence append has fully rolled back. */
		async matches(
			client: PostgresRawClient,
			occurrence: JournalOccurrence,
		): Promise<boolean> {
			const rows = occurrenceRows.safeParse(
				await client.$queryRawUnsafe(
					`SELECT occurrence_id, event_name, event_version, stream_name, stream_key, payload_format, payload,
  incoming_source_stream_name, incoming_source_stream_key, incoming_channel_sequence
FROM ${table}
WHERE occurrence_id = $1::uuid OR (
  incoming_source_stream_name = $2 AND incoming_source_stream_key = $3
  AND stream_name = $4 AND stream_key = $5 AND incoming_channel_sequence = $6::bigint
)`,
					occurrence.occurrenceId,
					occurrence.incoming?.source.streamName ?? null,
					occurrence.incoming?.source.streamKey ?? null,
					occurrence.streamName,
					occurrence.streamKey,
					occurrence.incoming?.channelSequence ?? null,
				),
			);
			if (!rows.success) {
				throw new JournalResultError(
					"The journal occurrence lookup returned invalid data.",
				);
			}
			const row = rows.data[0];
			return (
				rows.data.length === 1 &&
				row !== undefined &&
				row.occurrence_id === occurrence.occurrenceId &&
				row.event_name === occurrence.eventName &&
				row.event_version === BigInt(occurrence.eventVersion) &&
				row.stream_name === occurrence.streamName &&
				row.stream_key === occurrence.streamKey &&
				row.payload_format === occurrence.payloadFormat &&
				row.payload === occurrence.encodedInput &&
				row.incoming_source_stream_name ===
					(occurrence.incoming?.source.streamName ?? null) &&
				row.incoming_source_stream_key ===
					(occurrence.incoming?.source.streamKey ?? null) &&
				row.incoming_channel_sequence ===
					(occurrence.incoming?.channelSequence ?? null)
			);
		},
		async append(
			transaction: PostgresRawClient,
			occurrence: JournalOccurrence,
		): Promise<{
			readonly sequence: bigint;
			readonly previousIncomingSequence: bigint;
		}> {
			const rows = reservationRows.safeParse(
				await transaction.$queryRawUnsafe(
					occurrence.incoming === undefined
						? externalAppendSql
						: incomingAppendSql,
					occurrence.occurrenceId,
					occurrence.streamName,
					occurrence.streamKey,
					occurrence.eventName,
					occurrence.eventVersion,
					occurrence.payloadFormat,
					occurrence.encodedInput,
					occurrence.incoming?.source.streamName ?? null,
					occurrence.incoming?.source.streamKey ?? null,
					occurrence.incoming?.channelSequence ?? null,
				),
			);
			const row = rows.success ? rows.data[0] : undefined;
			if (!row) {
				throw new JournalResultError(
					"The journal reservation returned invalid stream or incoming channel positions.",
				);
			}
			return {
				sequence: row.stream_sequence,
				previousIncomingSequence: row.previous_incoming_sequence,
			};
		},
	});
}
