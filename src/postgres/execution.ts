import type { EventPayload, EventRegistration } from "../event.js";
import type { StreamClient } from "../handler.js";
import { createOutputCollector } from "./collector.js";
import { createEffectCollector } from "./effect-collector.js";
import {
	OccurrenceIdentityError,
	RetryExhaustedError,
	StreamSequenceOverflowError,
} from "./errors.js";
import { classifyFailure } from "./failures.js";
import { bindOccurrenceAttempt, reconstructPayload } from "./occurrence.js";
import type { PreparedOccurrence } from "./occurrence.js";
import type { createOutgoingStorage } from "./outgoing.js";
import type {
	CanonicalEventLookup,
	CanonicalEffectLookup,
} from "./registry.js";
import type { createJournalStorage } from "./storage.js";
import type {
	PostgresPublication,
	PostgresEffectPublication,
	PostgresRawClient,
	PostgresTransactionHost,
} from "./types.js";

export interface ExecutionOptions {
	readonly maxWait: number;
	readonly timeout: number;
}

/** Private control signal: it must escape the transaction before acknowledgment. */
class IncomingGap extends Error {
	constructor(readonly expectedSequence: bigint) {
		super("The incoming channel has an unprocessed predecessor.");
	}
}

/** Private established-occurrence path. A duplicate has no recoverable result. */
export async function executeOccurrence<
	Event extends EventRegistration,
	Result,
>(
	client: PostgresTransactionHost<
		StreamClient<Event["stream"]> & PostgresRawClient
	> &
		PostgresRawClient,
	journal: ReturnType<typeof createJournalStorage>,
	occurrence: PreparedOccurrence<Event, Result>,
	firstPayload: EventPayload<Event>,
	options: ExecutionOptions,
	emission: {
		readonly events: CanonicalEventLookup & CanonicalEffectLookup;
		readonly outgoing: ReturnType<typeof createOutgoingStorage>;
		readonly publication?: PostgresPublication | undefined;
		readonly effectPublication?: PostgresEffectPublication | undefined;
	},
): Promise<
	| {
			readonly status: "processed";
			readonly result: Awaited<Result>;
			readonly sequence: bigint;
	  }
	| { readonly status: "alreadyProcessed" }
	| { readonly status: "gap"; readonly expectedSequence: string }
> {
	for (let attempt = 0; attempt < 3; attempt++) {
		const payload =
			attempt === 0 ? firstPayload : reconstructPayload(occurrence);
		let phase:
			| "begin"
			| "append"
			| "admission"
			| "handler"
			| "outputs"
			| "commit" = "begin";
		let completed: {
			readonly status: "processed";
			readonly result: Awaited<Result>;
			readonly sequence: bigint;
			readonly afterCommit: (() => void | Promise<void>) | undefined;
		};
		try {
			completed = await client.$transaction(
				async (transaction) => {
					phase = "append";
					const { sequence, previousIncomingSequence } = await journal.append(
						transaction,
						occurrence,
					);
					if (occurrence.incoming !== undefined) {
						phase = "admission";
						const expected = previousIncomingSequence + 1n;
						if (occurrence.incoming.channelSequence > expected) {
							throw new IncomingGap(expected);
						}
						if (occurrence.incoming.channelSequence < expected) {
							// An intact retained history would have conflicted at append.
							throw new OccurrenceIdentityError(
								"The incoming channel is missing previously accepted history.",
							);
						}
					}
					const collector = createOutputCollector(emission.events);
					const effects = createEffectCollector(emission.events);
					phase = "handler";
					let result: Awaited<Result>;
					try {
						result = await bindOccurrenceAttempt(
							occurrence,
							transaction,
							payload,
							(context) => occurrence.handler.handle(context),
							collector.emit,
							effects.requestEffect,
						);
					} finally {
						// Even a rejected callback must retire its capability before
						// rollback/retry; it can never append to a later attempt.
						collector.close();
						effects.close();
					}
					phase = "outputs";
					const { afterCommit } = await emission.outgoing.appendOutputs(
						transaction,
						{
							streamName: occurrence.streamName,
							streamKey: occurrence.streamKey,
						},
						collector.finish(),
						emission.publication,
						effects.finish(),
						emission.effectPublication,
					);
					phase = "commit";
					return {
						status: "processed" as const,
						result,
						sequence,
						afterCommit,
					};
				},
				{
					isolationLevel: "Serializable",
					maxWait: options.maxWait,
					timeout: options.timeout,
				},
			);
		} catch (error) {
			// The transaction promise has rejected: rollback is complete before
			// either a fresh transaction or a root-client duplicate lookup.
			if (error instanceof IncomingGap) {
				return {
					status: "gap",
					expectedSequence: error.expectedSequence.toString(),
				};
			}
			switch (classifyFailure(error, phase)) {
				case "duplicate":
					if (!(await journal.matches(client, occurrence))) {
						throw new OccurrenceIdentityError(
							"The committed occurrence does not match its prepared identity.",
						);
					}
					return { status: "alreadyProcessed" };
				case "retry":
					if (attempt === 2) {
						throw new RetryExhaustedError(
							"The transaction exhausted its three aborted-transaction attempts.",
						);
					}
					continue;
				case "overflow":
					throw new StreamSequenceOverflowError(
						"The stream has exhausted PostgreSQL bigint positions.",
					);
				case "other":
					throw error;
			}
		}
		// Deliberately outside the transaction retry boundary. Observe rejected
		// promises without retaining unowned background work. The publisher owns
		// sanitized diagnostics; notification is never authority for this commit.
		try {
			await completed.afterCommit?.();
		} catch {
			// A committed transition must still return its business outcome.
		}
		return {
			status: completed.status,
			result: completed.result,
			sequence: completed.sequence,
		};
	}
	throw new RetryExhaustedError(
		"The transaction exhausted its three aborted-transaction attempts.",
	);
}
