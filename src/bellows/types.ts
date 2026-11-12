import type {
	PublishTaskDefinition,
	WorkerFactory,
} from "@xjonathanlei/bellows";
import type { DeliveryOptions, DeliveryStage } from "../delivery/types.js";
import type {
	DeliveryReference,
	PostgresPublication,
} from "../postgres/types.js";

export interface DeliveryTaskReceipt {
	readonly taskId: number;
	readonly taskName: string;
}

/** Fixed codes only: never includes envelopes, stream keys or arbitrary causes. */
export interface BellowsDeliveryDiagnostic extends DeliveryTaskReceipt {
	readonly stage: DeliveryStage | "processing" | "notification";
	readonly status: "failed" | "predecessor" | "gap";
}

export interface BellowsDeliveryOptions {
	/** Explicit, separately installed Bellows schema in the source database. */
	readonly schema: string;
	/** Default: folven_delivery. Use distinct names for distinct source namespaces. */
	readonly taskName?: string;
	/** Fixed retry delay, 1..86,400,000 ms; defaults to 5,000. No attempt counter. */
	readonly retryDelayMs?: number;
	/** Unix milliseconds; defaults to Date.now. Must return a valid safe timestamp. */
	readonly now?: () => number;
	/**
	 * Called only after confirmed commit. Awaited; alternatively register ID-only
	 * work with the host's lifetime owner and return immediately. Never republish.
	 */
	readonly onTasksCommitted?: (
		receipts: readonly DeliveryTaskReceipt[],
	) => void | Promise<void>;
	/** Awaited and failure-isolated. No automatic logging or detached work. */
	readonly onDiagnostic?: (
		diagnostic: BellowsDeliveryDiagnostic,
	) => void | Promise<void>;
}

export type DeliveryTask = PublishTaskDefinition<DeliveryReference, undefined>;

export interface BellowsDeliveryWorker {
	readonly factory: WorkerFactory<DeliveryTask>;
	/** Stop host admission first. Repeatable; waits even after Bellows lease loss. */
	drain(): Promise<void>;
}

export interface BellowsDelivery {
	readonly task: DeliveryTask;
	readonly publication: PostgresPublication;
	createWorkerFactory(options: DeliveryOptions): BellowsDeliveryWorker;
}
