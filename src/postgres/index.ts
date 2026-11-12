import { z } from "zod";
import type { EventInput, EventRegistration } from "../event.js";
import type { HandlerRegistration, StreamClient } from "../handler.js";
import type { DefinitionRegistry } from "../registry.js";
import type { ScopeStream } from "../scope.js";
import {
	OccurrenceAlreadyProcessedError,
	OccurrenceIdentityError,
	RuntimeConfigurationError,
} from "./errors.js";
export type {
	StreamIdentity,
	EffectReference,
	OutstandingEffect,
	EffectRead,
	EffectCompletionGuard,
	OutstandingEffectReadOptions,
	OutstandingEffectPage,
	OutgoingEnvelope,
	OutgoingReference,
	OutgoingRead,
	PendingOccurrenceReference,
	PendingOccurrenceEnvelope,
	PendingOccurrenceRead,
	DeliveryReference,
	DeliveryEnvelope,
	DeliveryRead,
	PostgresPublication,
	PostgresEffectPublication,
	PostgresPublicationFragment,
	PostgresRawClient,
	PostgresBorrowedTransaction,
	StagedPendingOccurrences,
	PendingOccurrencePage,
	PendingOccurrenceReadOptions,
	IngestionResult,
	PendingCursor,
	PendingPage,
	PendingReadOptions,
} from "./types.js";
export { createPostgresPublisher } from "./publisher.js";
export { createPostgresEffectStore } from "./effect-store.js";
export type { PreparedEffectCompletion } from "./effect-store.js";
export type { PreparedPendingOccurrence } from "./publisher.js";
export {
	effectId,
	parseEffectReference,
	parseOutstandingEffect,
} from "./effect-protocol.js";
import { executeOccurrence } from "./execution.js";
import { prepareIncomingOccurrence, prepareOccurrence } from "./occurrence.js";
import { createOutgoingStorage } from "./outgoing.js";
import { createPendingOccurrenceStorage } from "./pending.js";
import { parseDeliveryEnvelope, parseDeliveryReference } from "./protocol.js";
import { validatePublication } from "./publication.js";
import { createHandlerLookup } from "./registry.js";
import type {
	DispatchableEvent,
	DispatchResult,
	RegistryClient,
} from "./registry.js";
import { createJournalStorage } from "./storage.js";
import type {
	DeliveryEnvelope,
	DeliveryReference,
	DeliveryRead,
	IngestionResult,
	OutgoingReference,
	OutgoingRead,
	PendingOccurrenceReference,
	PendingOccurrenceRead,
	PendingOccurrenceReadOptions,
	PendingReadOptions,
	PostgresPublication,
	PostgresEffectPublication,
	PostgresRawClient,
	PostgresTransactionHost,
} from "./types.js";

export {
	EffectProtocolError,
	EffectRegistrationError,
	EffectSequenceOverflowError,
	EffectResultError,
	EffectIdentityError,
	EffectRequestClosedError,
	EffectRequestFailedError,
	ChannelEnvelopeError,
	PendingOccurrenceEnvelopeError,
	ChannelSequenceOverflowError,
	EmissionClosedError,
	EmissionFailedError,
	JournalResultError,
	OccurrenceAlreadyProcessedError,
	OccurrenceIdentityError,
	OutgoingResultError,
	OutgoingIdentityError,
	PendingReadOptionsError,
	PayloadEncodingError,
	PublicationResultError,
	PendingOccurrenceIdentityError,
	PendingOccurrencePreparationError,
	PendingOccurrenceResultError,
	PostgresIdentityError,
	RetryExhaustedError,
	RuntimeConfigurationError,
	StreamSequenceOverflowError,
} from "./errors.js";

const transactionOptions = z.strictObject({
	maxWait: z.number().int().positive().max(2_147_483_647).default(2_000),
	timeout: z.number().int().positive().max(2_147_483_647).default(5_000),
});

/** Construction validates only; the caller owns schema setup and client lifetime. */
export function createPostgresRuntime<
	Definitions extends DefinitionRegistry<
		readonly ScopeStream[],
		readonly EventRegistration[],
		readonly HandlerRegistration[]
	>,
>(options: {
	readonly client: PostgresTransactionHost<
		NoInfer<RegistryClient<Definitions>> & PostgresRawClient
	> &
		PostgresRawClient;
	readonly registry: Definitions;
	readonly schema: string;
	readonly publication?: PostgresPublication;
	readonly effectPublication?: PostgresEffectPublication;
	readonly transaction?: {
		readonly maxWait?: number;
		readonly timeout?: number;
	};
}) {
	const parsed = transactionOptions.safeParse(
		options.transaction === undefined ? {} : options.transaction,
	);
	if (!parsed.success) {
		throw new RuntimeConfigurationError(
			"Transaction wait and timeout must be positive bounded integer milliseconds.",
		);
	}
	const executionOptions = parsed.data;
	validatePublication(options.publication);
	validatePublication(options.effectPublication, "effect");
	const client = options.client;
	if (
		typeof client?.$transaction !== "function" ||
		typeof client.$queryRawUnsafe !== "function" ||
		typeof client.$connect !== "function" ||
		typeof client.$disconnect !== "function"
	) {
		throw new RuntimeConfigurationError(
			"The runtime requires a root Prisma transaction host and raw-query surface.",
		);
	}
	const lookup = createHandlerLookup(options.registry);
	const journal = createJournalStorage(options.schema);
	const outgoing = createOutgoingStorage(options.schema);
	const pending = createPendingOccurrenceStorage(options.schema);
	function read(reference: OutgoingReference): Promise<OutgoingRead>;
	function read(
		reference: PendingOccurrenceReference,
	): Promise<PendingOccurrenceRead>;
	function read(reference: DeliveryReference): Promise<DeliveryRead>;
	async function read(reference: DeliveryReference): Promise<DeliveryRead> {
		const parsed = parseDeliveryReference(reference);
		return "occurrenceId" in parsed
			? pending.read(client, parsed)
			: outgoing.read(client, parsed);
	}
	async function ingestEvent<Event extends Definitions["events"][number]>(
		event: Event,
		envelope: DeliveryEnvelope,
	): Promise<IngestionResult> {
		const { occurrence, firstPayload } = prepareIncomingOccurrence(
			lookup.requireHandler(event),
			envelope,
		);
		// As for dispatch, canonical registry membership proves the erased
		// event-to-stream/client association. Keep Event generic at this bridge;
		// the serialized boundary itself has no concrete descriptor type.
		const eventClient = client as PostgresTransactionHost<
			StreamClient<Event["stream"]> & PostgresRawClient
		> &
			PostgresRawClient;
		const outcome = await executeOccurrence<Event, unknown>(
			eventClient,
			journal,
			occurrence,
			firstPayload,
			executionOptions,
			{
				events: lookup,
				outgoing,
				publication: options.publication,
				effectPublication: options.effectPublication,
			},
		);
		// Results are deliberately neither persisted nor exposed on ingress.
		return outcome.status === "processed" ? { status: "processed" } : outcome;
	}
	return Object.freeze({
		outbox: Object.freeze({
			read,
			readPending: (options?: PendingReadOptions) =>
				outgoing.readPending(client, options),
			readPendingOccurrences: (options?: PendingOccurrenceReadOptions) =>
				pending.readPending(client, options),
			acknowledge: async (envelope: DeliveryEnvelope) => {
				const parsed = parseDeliveryEnvelope(envelope);
				return "occurrenceId" in parsed
					? pending.acknowledge(client, parsed)
					: outgoing.acknowledge(client, parsed);
			},
		}),
		async dispatch<Event extends DispatchableEvent<Definitions>>(
			event: Event,
			input: EventInput<NoInfer<Event>>,
		): Promise<DispatchResult<Definitions, Event>> {
			const { occurrence, firstPayload } = prepareOccurrence(
				lookup.get(event),
				input,
			);
			// Canonical lookup proves this event belongs to the registry's
			// stream/client association. TS cannot carry that tuple relation
			// through Event's generic indexed stream type. This private bridge
			// changes no object, method, query or transaction boundary.
			const eventClient = client as PostgresTransactionHost<
				StreamClient<Event["stream"]> & PostgresRawClient
			> &
				PostgresRawClient;
			const outcome = await executeOccurrence<
				Event,
				ReturnType<typeof occurrence.handler.handle>
			>(eventClient, journal, occurrence, firstPayload, executionOptions, {
				events: lookup,
				outgoing,
				publication: options.publication,
				effectPublication: options.effectPublication,
			});
			if (outcome.status === "alreadyProcessed") {
				throw new OccurrenceAlreadyProcessedError(
					"A newly allocated occurrence was already processed; its result cannot be recovered.",
				);
			}
			if (outcome.status === "gap") {
				throw new OccurrenceIdentityError(
					"An external occurrence cannot have an incoming channel gap.",
				);
			}
			return outcome.result;
		},
		async ingest(envelope: unknown): Promise<IngestionResult> {
			const parsedEnvelope = parseDeliveryEnvelope(envelope);
			const event = lookup.resolveEvent(
				parsedEnvelope.eventName,
				parsedEnvelope.eventVersion,
			);
			return ingestEvent(event, parsedEnvelope);
		},
	});
}
