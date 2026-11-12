import { streamIdentityByteLimit } from "./identity.js";
import {
	incomingPositionConstraint,
	occurrenceConstraint,
	outgoingPositionConstraint,
	qualifiedEventLog,
	qualifiedOutgoingEvent,
	qualifiedPendingOccurrence,
	qualifiedEffectFrontier,
	qualifiedOutstandingEffect,
	streamPositionConstraint,
} from "./schema.js";

/**
 * Fresh-install DDL for an existing, caller-owned schema. Run once in an
 * administrative transaction, never on the dispatch path.
 * Deliberately not IF NOT EXISTS: an unknown existing layout must not pass.
 */
export function postgresSchemaStatements(options: {
	readonly schema: string;
}): readonly string[] {
	const journal = qualifiedEventLog(options.schema);
	const outgoing = qualifiedOutgoingEvent(options.schema);
	const pending = qualifiedPendingOccurrence(options.schema);
	const frontier = qualifiedEffectFrontier(options.schema);
	const effects = qualifiedOutstandingEffect(options.schema);
	return Object.freeze([
		`CREATE TABLE ${journal} (
  occurrence_id uuid NOT NULL,
  stream_name text COLLATE "C" NOT NULL,
  stream_key text COLLATE "C" NOT NULL,
  stream_sequence bigint NOT NULL,
  event_name text COLLATE "C" NOT NULL,
  event_version bigint NOT NULL,
  payload_format text COLLATE "C" NOT NULL,
  payload text COLLATE "C" NOT NULL,
  incoming_source_stream_name text COLLATE "C",
  incoming_source_stream_key text COLLATE "C",
  incoming_channel_sequence bigint,
  CONSTRAINT ${occurrenceConstraint} PRIMARY KEY (occurrence_id),
  CONSTRAINT ${streamPositionConstraint} UNIQUE (stream_name, stream_key, stream_sequence),
  CONSTRAINT event_log_sequence_positive CHECK (stream_sequence > 0),
  CONSTRAINT event_log_version_valid CHECK (event_version BETWEEN 1 AND 9007199254740991),
  CONSTRAINT event_log_identity_valid CHECK (
    stream_name <> '' AND stream_key <> '' AND event_name <> ''
    AND octet_length(stream_name) + octet_length(stream_key) <= ${streamIdentityByteLimit}
  ),
  CONSTRAINT event_log_format_valid CHECK (payload_format = 'json-v1'),
  CONSTRAINT event_log_incoming_valid CHECK (
    (incoming_source_stream_name IS NULL AND incoming_source_stream_key IS NULL AND incoming_channel_sequence IS NULL)
    OR
    (incoming_source_stream_name IS NOT NULL AND incoming_source_stream_key IS NOT NULL AND incoming_channel_sequence IS NOT NULL
      AND incoming_source_stream_name <> '' AND incoming_source_stream_key <> ''
      AND incoming_channel_sequence > 0
      AND octet_length(incoming_source_stream_name) + octet_length(incoming_source_stream_key) <= ${streamIdentityByteLimit})
  )
)`,
		`CREATE UNIQUE INDEX ${incomingPositionConstraint} ON ${journal}
  (incoming_source_stream_name, incoming_source_stream_key, stream_name, stream_key, incoming_channel_sequence)
  WHERE incoming_source_stream_name IS NOT NULL`,
		`CREATE TABLE ${outgoing} (
  source_stream_name text COLLATE "C" NOT NULL,
  source_stream_key text COLLATE "C" NOT NULL,
  target_stream_name text COLLATE "C" NOT NULL,
  target_stream_key text COLLATE "C" NOT NULL,
  channel_sequence bigint NOT NULL,
  event_name text COLLATE "C" NOT NULL,
  event_version bigint NOT NULL,
  payload_format text COLLATE "C" NOT NULL,
  payload text COLLATE "C" NOT NULL,
  acknowledged boolean NOT NULL DEFAULT false,
  CONSTRAINT ${outgoingPositionConstraint} PRIMARY KEY
    (source_stream_name, source_stream_key, target_stream_name, target_stream_key, channel_sequence),
  CONSTRAINT outgoing_event_sequence_positive CHECK (channel_sequence > 0),
  CONSTRAINT outgoing_event_version_valid CHECK (event_version BETWEEN 1 AND 9007199254740991),
  CONSTRAINT outgoing_event_identity_valid CHECK (
    source_stream_name <> '' AND source_stream_key <> '' AND target_stream_name <> '' AND target_stream_key <> '' AND event_name <> ''
    AND octet_length(source_stream_name) + octet_length(source_stream_key) <= ${streamIdentityByteLimit}
    AND octet_length(target_stream_name) + octet_length(target_stream_key) <= ${streamIdentityByteLimit}
  ),
  CONSTRAINT outgoing_event_format_valid CHECK (payload_format = 'json-v1')
)`,
		`CREATE INDEX outgoing_event_pending_channel_idx ON ${outgoing}
  (source_stream_name, source_stream_key, target_stream_name, target_stream_key, channel_sequence)
  WHERE NOT acknowledged`,
		`CREATE TABLE ${pending} (
  occurrence_id uuid PRIMARY KEY,
  target_stream_name text COLLATE "C" NOT NULL,
  target_stream_key text COLLATE "C" NOT NULL,
  event_name text COLLATE "C" NOT NULL,
  event_version bigint NOT NULL,
  payload_format text COLLATE "C" NOT NULL,
  payload text COLLATE "C" NOT NULL,
  acknowledged boolean NOT NULL DEFAULT false,
  CONSTRAINT pending_occurrence_version_valid CHECK (event_version BETWEEN 1 AND 9007199254740991),
  CONSTRAINT pending_occurrence_identity_valid CHECK (
    target_stream_name <> '' AND target_stream_key <> '' AND event_name <> ''
    AND octet_length(target_stream_name) + octet_length(target_stream_key) <= ${streamIdentityByteLimit}
  ),
  CONSTRAINT pending_occurrence_format_valid CHECK (payload_format = 'json-v1')
)`,
		`CREATE INDEX pending_occurrence_pending_idx ON ${pending} (occurrence_id)
  WHERE NOT acknowledged`,
		`CREATE TABLE ${frontier} (
  source_stream_name text COLLATE "C" NOT NULL,
  source_stream_key text COLLATE "C" NOT NULL,
  registered_through bigint NOT NULL,
  PRIMARY KEY (source_stream_name, source_stream_key),
  CONSTRAINT effect_frontier_position_valid CHECK (registered_through > 0),
  CONSTRAINT effect_frontier_identity_valid CHECK (
    source_stream_name <> '' AND source_stream_key <> ''
    AND octet_length(source_stream_name) + octet_length(source_stream_key) <= ${streamIdentityByteLimit}
  )
)`,
		`CREATE TABLE ${effects} (
  source_stream_name text COLLATE "C" NOT NULL,
  source_stream_key text COLLATE "C" NOT NULL,
  effect_sequence bigint NOT NULL,
  effect_name text COLLATE "C" NOT NULL,
  effect_version bigint NOT NULL,
  payload_format text COLLATE "C" NOT NULL,
  payload text COLLATE "C" NOT NULL,
  PRIMARY KEY (source_stream_name, source_stream_key, effect_sequence),
  FOREIGN KEY (source_stream_name, source_stream_key)
    REFERENCES ${frontier} (source_stream_name, source_stream_key),
  CONSTRAINT outstanding_effect_sequence_positive CHECK (effect_sequence > 0),
  CONSTRAINT outstanding_effect_version_valid CHECK (effect_version BETWEEN 1 AND 9007199254740991),
  CONSTRAINT outstanding_effect_identity_valid CHECK (
    source_stream_name <> '' AND source_stream_key <> '' AND effect_name <> ''
    AND octet_length(source_stream_name) + octet_length(source_stream_key) <= ${streamIdentityByteLimit}
  ),
  CONSTRAINT outstanding_effect_format_valid CHECK (payload_format = 'json-v1')
)`,
	]);
}
