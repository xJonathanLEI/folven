/** Framework failures never include caller payloads or resolved keys. */
export class PayloadEncodingError extends Error {
	override readonly name = "PayloadEncodingError";
}

export class ChannelEnvelopeError extends Error {
	override readonly name = "ChannelEnvelopeError";
}

export class PendingOccurrenceEnvelopeError extends Error {
	override readonly name = "PendingOccurrenceEnvelopeError";
}

export class PostgresIdentityError extends Error {
	override readonly name = "PostgresIdentityError";
}

export class JournalResultError extends Error {
	override readonly name = "JournalResultError";
}

export class RuntimeConfigurationError extends Error {
	override readonly name = "RuntimeConfigurationError";
}

export class RetryExhaustedError extends Error {
	override readonly name = "RetryExhaustedError";
}

export class OccurrenceIdentityError extends Error {
	override readonly name = "OccurrenceIdentityError";
}

export class OccurrenceAlreadyProcessedError extends Error {
	override readonly name = "OccurrenceAlreadyProcessedError";
}

export class StreamSequenceOverflowError extends Error {
	override readonly name = "StreamSequenceOverflowError";
}

export class ChannelSequenceOverflowError extends Error {
	override readonly name = "ChannelSequenceOverflowError";
}

export class OutgoingResultError extends Error {
	override readonly name = "OutgoingResultError";
}

export class PublicationResultError extends Error {
	override readonly name = "PublicationResultError";
}

export class PendingOccurrenceResultError extends Error {
	override readonly name = "PendingOccurrenceResultError";
}

export class PendingOccurrenceIdentityError extends Error {
	override readonly name = "PendingOccurrenceIdentityError";
}

export class PendingOccurrencePreparationError extends Error {
	override readonly name = "PendingOccurrencePreparationError";
}

export class OutgoingIdentityError extends Error {
	override readonly name = "OutgoingIdentityError";
}

export class PendingReadOptionsError extends Error {
	override readonly name = "PendingReadOptionsError";
}

export class EmissionClosedError extends Error {
	override readonly name = "EmissionClosedError";
}

export class EmissionFailedError extends Error {
	override readonly name = "EmissionFailedError";
}

export class EffectProtocolError extends Error {
	override readonly name = "EffectProtocolError";
}

export class EffectRegistrationError extends Error {
	override readonly name = "EffectRegistrationError";
}

export class EffectSequenceOverflowError extends Error {
	override readonly name = "EffectSequenceOverflowError";
}

export class EffectResultError extends Error {
	override readonly name = "EffectResultError";
}

export class EffectIdentityError extends Error {
	override readonly name = "EffectIdentityError";
}

export class EffectRequestClosedError extends Error {
	override readonly name = "EffectRequestClosedError";
}

export class EffectRequestFailedError extends Error {
	override readonly name = "EffectRequestFailedError";
}
