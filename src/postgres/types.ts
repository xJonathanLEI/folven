/** Canonical JSON key text, independently bounded with its stream name. */
export interface StreamIdentity {
	readonly streamName: string;
	readonly streamKey: string;
}

/** Source-wide registration position, not execution order or journal position. */
export interface EffectReference {
	readonly source: StreamIdentity;
	readonly effectSequence: string;
}

export interface OutstandingEffect {
	readonly reference: EffectReference;
	readonly effectName: string;
	readonly effectVersion: number;
	readonly payloadFormat: "json-v1";
	readonly encodedInput: string;
}

export type EffectRead =
	| { readonly status: "outstanding"; readonly effect: OutstandingEffect }
	| { readonly status: "alreadyCompleted" }
	| { readonly status: "notRegistered" };

export type EffectCompletionGuard =
	| "completed"
	| "alreadyCompleted"
	| "notRegistered";

/** Keyset continuation only. Restart scans to discover new work behind the cursor. */
export interface OutstandingEffectReadOptions {
	readonly limit?: number;
	readonly after?: EffectReference;
}

export interface OutstandingEffectPage {
	readonly effects: readonly OutstandingEffect[];
	readonly nextCursor: EffectReference | null;
}

/**
 * Borrowed active transaction, not a root client. This trusted structural
 * boundary excludes lifecycle methods, not Prisma's retained $transaction type.
 * It cannot prove a deliberately disguised client is in a transaction. The owner
 * must propagate staging failures and controls commit, retries and client lifetime.
 */
export interface PostgresBorrowedTransaction extends PostgresRawClient {
	readonly $connect?: never;
	readonly $disconnect?: never;
}

/** Provisional until the owning outer transaction confirms its commit. */
export interface StagedPendingOccurrences {
	/** Batch/UUID order does not establish destination ingestion order. */
	readonly references: readonly PendingOccurrenceReference[];
	/**
	 * Call only after the outer transaction promise confirms commit, never from
	 * its callback, on rollback or after an uncertain commit response. This
	 * cannot verify commit. Isolated notification failures never republish.
	 */
	readonly notifyCommitted: () => Promise<void>;
}

/** Private immutable capture, before source-owned channel allocation. */
export type CapturedOutput = Omit<
	OutgoingEnvelope,
	"source" | "channelSequence"
>;

/** Private journal metadata; external dispatch has no incoming origin. */
export interface IncomingOrigin {
	readonly source: StreamIdentity;
	readonly channelSequence: bigint;
}

/** Durable identity is source + target + sequence, not the event or payload. */
export interface OutgoingReference {
	readonly source: StreamIdentity;
	readonly target: StreamIdentity;
	readonly channelSequence: string;
}

export interface OutgoingEnvelope extends OutgoingReference {
	readonly eventName: string;
	readonly eventVersion: number;
	readonly payloadFormat: "json-v1";
	readonly encodedInput: string;
}

/** Retained output and channel readiness from one database snapshot. */
export interface OutgoingRead {
	readonly envelope: OutgoingEnvelope;
	readonly acknowledged: boolean;
	readonly hasPredecessor: boolean;
}

/** Durable independent identity; no source or channel ordering is implied. */
export interface PendingOccurrenceReference {
	readonly occurrenceId: string;
}

export interface PendingOccurrenceEnvelope extends PendingOccurrenceReference {
	readonly target: StreamIdentity;
	readonly eventName: string;
	readonly eventVersion: number;
	readonly payloadFormat: "json-v1";
	readonly encodedInput: string;
}

export interface PendingOccurrenceRead {
	readonly envelope: PendingOccurrenceEnvelope;
	readonly acknowledged: boolean;
}

/** Strictly parsed protocol alternatives, never an optional-field envelope. */
export type DeliveryReference = OutgoingReference | PendingOccurrenceReference;
export type DeliveryEnvelope = OutgoingEnvelope | PendingOccurrenceEnvelope;
export type DeliveryRead = OutgoingRead | PendingOccurrenceRead;

/** Independent envelopes can only process or deduplicate, never report a gap. */
export type IngestionResult =
	| { readonly status: "processed" }
	| { readonly status: "alreadyProcessed" }
	| { readonly status: "gap"; readonly expectedSequence: string };

/** Channel scan continuation, not an event watermark; restart scans for new work. */
export interface PendingCursor {
	readonly source: StreamIdentity;
	readonly target: StreamIdentity;
}

export interface PendingPage {
	readonly envelopes: readonly OutgoingEnvelope[];
	readonly nextCursor: PendingCursor | null;
}

export interface PendingReadOptions {
	/** Defaults to 100; must be an integer from 1 through 1,000. */
	readonly limit?: number;
	readonly after?: PendingCursor;
}

/** UUID scan continuation, not arrival order or a durable feed watermark. */
export interface PendingOccurrenceReadOptions {
	/** Defaults to 100; must be an integer from 1 through 1,000. */
	readonly limit?: number;
	/** Restart scan cycles to discover inserts before this canonical UUID. */
	readonly after?: string;
}

export interface PendingOccurrencePage {
	readonly envelopes: readonly PendingOccurrenceEnvelope[];
	readonly nextCursor: string | null;
}

/** Structural Prisma raw-query surface; only framework metadata uses it. */
export interface PostgresRawClient {
	$queryRawUnsafe(query: string, ...values: unknown[]): Promise<unknown>;
}

/**
 * Trusted infrastructure, never a handler capability. Prepare a single SQL body
 * for a data-modifying CTE, without I/O or transaction ownership. Each source row
 * contains one non-null text-encoded DeliveryReference (the strict channel or
 * independent alternative), not an event payload.
 * Bind numbering begins at parameterOffset + 1. Result rows have no order.
 * Validation must reject missing/extra/invalid receipts before commit; it must
 * not publish separately or perform external effects.
 * Only the successful attempt's action may run after confirmed commit: runtime
 * execution invokes it automatically; staging leaves notifyCommitted to the
 * outer transaction owner. Actions are awaited, but their failures cannot fail
 * or replay the committed transition.
 * Implementations own sanitized notification diagnostics; a host may register
 * notification with its lifetime owner and return without waiting for acceptance.
 */
export interface PostgresPublication {
	/** Optional for existing delivery publishers; incompatible with effects. */
	readonly kind?: "delivery";
	prepare(options: {
		readonly source: {
			readonly relation: string;
			readonly payloadColumn: string;
		};
		readonly parameterOffset: number;
	}): PostgresPublicationFragment;
}

/** Same trusted SQL boundary, but rows encode only EffectReference values. */
export interface PostgresEffectPublication {
	readonly kind: "effect";
	prepare: PostgresPublication["prepare"];
}

export interface PostgresPublicationFragment {
	readonly sql: string;
	readonly parameters: readonly unknown[];
	readonly validate: (
		rows: readonly unknown[],
		expectedCount: number,
	) => (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
}

/** A root client, not a transaction client. The caller owns its lifecycle. */
export interface PostgresTransactionHost<
	Transaction extends PostgresRawClient,
> {
	// Prisma 7.10 transaction types retain $transaction. Root-only lifecycle
	// methods distinguish a host; Folven must never call these methods.
	$connect(): Promise<void>;
	$disconnect(): Promise<void>;
	$transaction<Result>(
		run: (transaction: Transaction) => Promise<Result>,
		options: {
			isolationLevel: "Serializable";
			maxWait?: number;
			timeout?: number;
		},
	): Promise<Result>;
}

/** Immutable wire facts accepted by the private journal adapter. */
export interface JournalOccurrence {
	readonly occurrenceId: string;
	readonly streamName: string;
	readonly streamKey: string;
	readonly eventName: string;
	readonly eventVersion: number;
	readonly payloadFormat: "json-v1";
	readonly encodedInput: string;
	readonly incoming?: IncomingOrigin;
}
