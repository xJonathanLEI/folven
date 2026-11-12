import type {
	DeliveryEnvelope,
	DeliveryRead,
	DeliveryReference,
	IngestionResult,
	OutgoingRead,
	OutgoingReference,
	PendingOccurrenceRead,
	PendingOccurrenceReference,
} from "../postgres/types.js";

/** No client ownership, transaction, claim or scheduling capability. */
export interface DeliverySource {
	read(reference: OutgoingReference): Promise<OutgoingRead>;
	read(reference: PendingOccurrenceReference): Promise<PendingOccurrenceRead>;
	read(reference: DeliveryReference): Promise<DeliveryRead>;
	acknowledge(envelope: DeliveryEnvelope): Promise<void>;
}

export interface DeliveryOptions {
	readonly source: DeliverySource;
	/** Runtime validation also fences incorrect or uncertain callback responses. */
	readonly deliver: (envelope: DeliveryEnvelope) => Promise<IngestionResult>;
}

export type DeliveryResult =
	| { readonly status: "completed" }
	| { readonly status: "waiting"; readonly reason: "predecessor" }
	| {
			readonly status: "waiting";
			readonly reason: "gap";
			readonly expectedSequence: string;
	  };

export type DeliveryStage = "read" | "delivery" | "response" | "acknowledgment";
