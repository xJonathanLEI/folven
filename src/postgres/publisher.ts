import { randomUUID } from "node:crypto";
import type { EventInput, EventRegistration } from "../event.js";
import type { HandlerRegistration } from "../handler.js";
import type { DefinitionRegistry } from "../registry.js";
import type { ScopeStream } from "../scope.js";
import {
	PendingOccurrencePreparationError,
	RuntimeConfigurationError,
} from "./errors.js";
import { prepareEvent } from "./occurrence.js";
import { createPendingOccurrenceStorage } from "./pending.js";
import { validatePublication } from "./publication.js";
import { createHandlerLookup } from "./registry.js";
import type {
	PendingOccurrenceEnvelope,
	PostgresBorrowedTransaction,
	PostgresPublication,
	StagedPendingOccurrences,
} from "./types.js";

const preparedOccurrence = Symbol("PreparedPendingOccurrence");

/** Opaque immutable capture, usable only with the publisher that prepared it. */
export interface PreparedPendingOccurrence {
	readonly [preparedOccurrence]: true;
}

/** No client, I/O, handler execution, or destination position at preparation. */
export function createPostgresPublisher<
	Definitions extends DefinitionRegistry<
		readonly ScopeStream[],
		readonly EventRegistration[],
		readonly HandlerRegistration[]
	>,
>(options: {
	readonly registry: Definitions;
	readonly schema: string;
	readonly publication?: PostgresPublication;
}) {
	const lookup = createHandlerLookup(options.registry);
	const storage = createPendingOccurrenceStorage(options.schema);
	const publication = options.publication;
	validatePublication(publication);
	const preparations = new WeakMap<
		PreparedPendingOccurrence,
		PendingOccurrenceEnvelope
	>();
	return Object.freeze({
		/**
		 * Captures schema-input bytes, event-owned routing and a fresh UUID in
		 * memory. Requires the canonical registered event, not a local handler.
		 * No destination order is reserved; caller mutation cannot alter capture.
		 */
		prepare<Event extends Definitions["events"][number]>(
			event: Event,
			input: EventInput<NoInfer<Event>>,
		): PreparedPendingOccurrence {
			const { prepared } = prepareEvent(lookup.getEvent(event), input);
			const token = Object.freeze({
				[preparedOccurrence]: true,
			} satisfies PreparedPendingOccurrence);
			preparations.set(
				token,
				Object.freeze({
					occurrenceId: randomUUID(),
					target: Object.freeze({
						streamName: prepared.streamName,
						streamKey: prepared.streamKey,
					}),
					eventName: prepared.eventName,
					eventVersion: prepared.eventVersion,
					payloadFormat: prepared.payloadFormat,
					encodedInput: prepared.encodedInput,
				}),
			);
			return token;
		},
		/**
		 * Borrows the caller's active transaction; every error must escape it.
		 * Reuse preparations only after known aborts, never to replay an uncertain
		 * commit. Keep only the winning callback's result when the owner retries.
		 * No handler, notification, transaction control, or retry runs here.
		 */
		async stage(
			transaction: PostgresBorrowedTransaction,
			prepared: readonly PreparedPendingOccurrence[],
		): Promise<StagedPendingOccurrences> {
			if (
				typeof transaction?.$queryRawUnsafe !== "function" ||
				"$connect" in transaction ||
				"$disconnect" in transaction
			) {
				throw new RuntimeConfigurationError(
					"Staging requires a borrowed active transaction, not a root Prisma client.",
				);
			}
			if (!Array.isArray(prepared)) {
				throw new PendingOccurrencePreparationError(
					"Staging requires a readonly batch of prepared occurrences.",
				);
			}
			const identities = new Set<string>();
			const envelopes = Array.from(
				prepared,
				(token: PreparedPendingOccurrence) => {
					const envelope = preparations.get(token);
					if (!envelope || identities.has(envelope.occurrenceId)) {
						throw new PendingOccurrencePreparationError(
							"Staging requires distinct occurrences prepared by this publisher.",
						);
					}
					identities.add(envelope.occurrenceId);
					return envelope;
				},
			);
			const { references, afterCommit } = await storage.append(
				transaction,
				envelopes,
				publication,
			);
			return Object.freeze({
				references,
				async notifyCommitted() {
					try {
						await afterCommit?.();
					} catch {
						// The optional adapter owns sanitized diagnostics. A notification
						// failure cannot change or replay a confirmed business commit.
					}
				},
			});
		},
	});
}
