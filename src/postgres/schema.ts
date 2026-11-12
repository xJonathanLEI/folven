import { PostgresIdentityError } from "./errors.js";

/** Restrict identifiers rather than accepting truncation or SQL syntax. */
function quotedSchema(schema: string): string {
	if (
		typeof schema !== "string" ||
		!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(schema)
	) {
		throw new PostgresIdentityError(
			"The metadata schema must be a 1–63 character ASCII SQL identifier.",
		);
	}
	return `"${schema}"`;
}

export function qualifiedEventLog(schema: string): string {
	return `${quotedSchema(schema)}."event_log"`;
}

export function qualifiedOutgoingEvent(schema: string): string {
	return `${quotedSchema(schema)}."outgoing_event"`;
}

export function qualifiedPendingOccurrence(schema: string): string {
	return `${quotedSchema(schema)}."pending_occurrence"`;
}

export function qualifiedEffectFrontier(schema: string): string {
	return `${quotedSchema(schema)}."effect_frontier"`;
}

export function qualifiedOutstandingEffect(schema: string): string {
	return `${quotedSchema(schema)}."outstanding_effect"`;
}

export const occurrenceConstraint = "event_log_occurrence_pk";
export const streamPositionConstraint = "event_log_stream_position_key";
export const incomingPositionConstraint = "event_log_incoming_position_key";
export const outgoingPositionConstraint = "outgoing_event_channel_pk";
