import { z } from "zod";
import {
	PendingOccurrenceIdentityError,
	PendingOccurrenceResultError,
	PendingReadOptionsError,
} from "./errors.js";
import {
	parsePendingOccurrenceEnvelope,
	parsePendingOccurrenceReference,
} from "./protocol.js";
import {
	preparePublication,
	validatePublicationAction,
} from "./publication.js";
import { qualifiedPendingOccurrence } from "./schema.js";
import type {
	PendingOccurrenceEnvelope,
	PendingOccurrencePage,
	PendingOccurrenceRead,
	PendingOccurrenceReadOptions,
	PendingOccurrenceReference,
	PostgresPublication,
	PostgresRawClient,
} from "./types.js";

const scanOptions = z.strictObject({
	limit: z.number().int().min(1).max(1_000).default(100),
	after: z.string().optional(),
});
const appendRows = z.tuple([
	z.strictObject({
		references: z.array(z.unknown()),
		publications: z.array(z.unknown()),
	}),
]);
const exactRows = z
	.array(
		z.strictObject({
			envelope: z.unknown(),
			acknowledged: z.boolean(),
		}),
	)
	.max(1);

function parseEnvelopes(value: unknown): readonly PendingOccurrenceEnvelope[] {
	try {
		return z
			.array(z.strictObject({ envelope: z.unknown() }))
			.parse(value)
			.map(({ envelope }) => parsePendingOccurrenceEnvelope(envelope));
	} catch {
		throw new PendingOccurrenceResultError(
			"The pending read did not return valid independent envelopes.",
		);
	}
}

/** Source-local retained occurrences. No destination journal or ordering reads. */
export function createPendingOccurrenceStorage(schema: string) {
	const table = qualifiedPendingOccurrence(schema);
	const envelopeSql = `json_build_object(
  'occurrenceId', occurrence_id::text,
  'target', json_build_object('streamName', target_stream_name, 'streamKey', target_stream_key),
  'eventName', event_name, 'eventVersion', event_version::double precision,
  'payloadFormat', payload_format, 'encodedInput', payload
)`;
	return Object.freeze({
		async append(
			transaction: PostgresRawClient,
			envelopes: readonly PendingOccurrenceEnvelope[],
			publication?: PostgresPublication,
		) {
			if (envelopes.length === 0) {
				return { references: Object.freeze([]), afterCommit: undefined };
			}
			const batch = JSON.stringify(
				envelopes.map((envelope) => ({
					occurrence_id: envelope.occurrenceId,
					target_stream_name: envelope.target.streamName,
					target_stream_key: envelope.target.streamKey,
					event_name: envelope.eventName,
					event_version: envelope.eventVersion,
					payload_format: envelope.payloadFormat,
					payload: envelope.encodedInput,
				})),
			);
			const fragment = preparePublication(publication, 1, "delivery_inputs");
			// JSONB transports the batch only; the original wire bytes remain text.
			const sql = `WITH inserted AS (
  INSERT INTO ${table} (
    occurrence_id, target_stream_name, target_stream_key,
    event_name, event_version, payload_format, payload
  )
  SELECT occurrence_id, target_stream_name, target_stream_key,
    event_name, event_version, payload_format, payload
  FROM jsonb_to_recordset($1::jsonb) AS input(
    occurrence_id uuid, target_stream_name text, target_stream_key text,
    event_name text, event_version bigint, payload_format text, payload text
  )
  RETURNING occurrence_id
), pending_references AS (
  SELECT json_build_object('occurrenceId', occurrence_id::text) AS reference FROM inserted
)${
				fragment
					? `,
delivery_inputs AS (
  SELECT reference::text AS encoded_payload FROM pending_references
), published_tasks AS (
  ${fragment.sql}
)`
					: ""
			}
SELECT COALESCE((SELECT json_agg(reference) FROM pending_references), '[]'::json) AS references,
  ${fragment ? "COALESCE((SELECT json_agg(published_tasks) FROM published_tasks), '[]'::json)" : "'[]'::json"} AS publications`;
			const rows = appendRows.safeParse(
				await transaction.$queryRawUnsafe(
					sql,
					batch,
					...(fragment?.parameters ?? []),
				),
			);
			if (!rows.success) {
				throw new PendingOccurrenceResultError(
					"The pending batch did not return valid persistence results.",
				);
			}
			const [row] = rows.data;
			let references: readonly PendingOccurrenceReference[];
			try {
				references = Object.freeze(
					row.references.map(parsePendingOccurrenceReference),
				);
				const expected = new Set(
					envelopes.map((envelope) => envelope.occurrenceId),
				);
				if (references.length !== envelopes.length) throw new Error();
				for (const reference of references) {
					if (!expected.delete(reference.occurrenceId)) throw new Error();
				}
				if (expected.size !== 0) throw new Error();
			} catch {
				throw new PendingOccurrenceResultError(
					"The pending batch did not return its exact inserted identities.",
				);
			}
			const afterCommit = await validatePublicationAction(
				fragment,
				row.publications,
				references.length,
			);
			return { references, afterCommit };
		},
		async read(
			client: PostgresRawClient,
			reference: PendingOccurrenceReference,
		): Promise<PendingOccurrenceRead> {
			const parsed = parsePendingOccurrenceReference(reference);
			const rows = exactRows.safeParse(
				await client.$queryRawUnsafe(
					`SELECT ${envelopeSql} AS envelope, acknowledged FROM ${table}
WHERE occurrence_id = $1::uuid`,
					parsed.occurrenceId,
				),
			);
			if (!rows.success) {
				throw new PendingOccurrenceResultError(
					"The exact read returned invalid pending metadata.",
				);
			}
			const row = rows.data[0];
			if (!row) {
				throw new PendingOccurrenceIdentityError(
					"The pending occurrence is missing from retained history.",
				);
			}
			try {
				const envelope = parsePendingOccurrenceEnvelope(row.envelope);
				if (envelope.occurrenceId === parsed.occurrenceId) {
					return Object.freeze({ envelope, acknowledged: row.acknowledged });
				}
			} catch {
				// Persisted payloads and parser details never enter framework errors.
			}
			throw new PendingOccurrenceResultError(
				"The exact read returned a different pending occurrence.",
			);
		},
		async acknowledge(
			client: PostgresRawClient,
			envelope: PendingOccurrenceEnvelope,
		): Promise<void> {
			const parsed = parsePendingOccurrenceEnvelope(envelope);
			const rows = z
				.array(z.strictObject({ occurrenceId: z.literal(parsed.occurrenceId) }))
				.max(1)
				.safeParse(
					await client.$queryRawUnsafe(
						`UPDATE ${table} SET acknowledged = true
WHERE occurrence_id = $1::uuid AND target_stream_name = $2 AND target_stream_key = $3
  AND event_name = $4 AND event_version = $5::bigint AND payload_format = $6 AND payload = $7
RETURNING occurrence_id::text AS "occurrenceId"`,
						parsed.occurrenceId,
						parsed.target.streamName,
						parsed.target.streamKey,
						parsed.eventName,
						parsed.eventVersion,
						parsed.payloadFormat,
						parsed.encodedInput,
					),
				);
			if (!rows.success) {
				throw new PendingOccurrenceResultError(
					"The acknowledgment returned invalid pending metadata.",
				);
			}
			if (rows.data.length === 0) {
				throw new PendingOccurrenceIdentityError(
					"The pending occurrence is missing or does not exactly match retained history.",
				);
			}
		},
		async readPending(
			client: PostgresRawClient,
			options: PendingOccurrenceReadOptions = {},
		): Promise<PendingOccurrencePage> {
			const parsed = scanOptions.safeParse(options);
			if (!parsed.success) {
				throw new PendingReadOptionsError(
					"Pending occurrence reads require a limit from 1 through 1000 and an optional UUID cursor.",
				);
			}
			const { limit, after } = parsed.data;
			if (after !== undefined)
				parsePendingOccurrenceReference({ occurrenceId: after });
			const rows = parseEnvelopes(
				await client.$queryRawUnsafe(
					`SELECT ${envelopeSql} AS envelope FROM ${table}
WHERE NOT acknowledged AND ($1::uuid IS NULL OR occurrence_id > $1::uuid)
ORDER BY occurrence_id LIMIT $2`,
					after ?? null,
					limit + 1,
				),
			);
			if (rows.length > limit + 1) {
				throw new PendingOccurrenceResultError(
					"The pending occurrence read exceeded its bound.",
				);
			}
			const envelopes = Object.freeze(rows.slice(0, limit));
			return Object.freeze({
				envelopes,
				nextCursor:
					rows.length > limit ? (envelopes.at(-1)?.occurrenceId ?? null) : null,
			});
		},
	});
}
