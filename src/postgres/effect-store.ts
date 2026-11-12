import { z } from "zod";
import { parseEffectReceipt } from "../effect-handler.js";
import type { EffectReceipt } from "../effect-handler.js";
import { EffectExecutionError } from "../effects/errors.js";
import type { EffectCompletionResult } from "../effects/types.js";
import type { EventRegistration } from "../event.js";
import type { HandlerRegistration } from "../handler.js";
import type { DefinitionRegistry } from "../registry.js";
import type { ScopeStream } from "../scope.js";
import { parseOutstandingEffect } from "./effect-protocol.js";
import { createEffectStorage } from "./effect-storage.js";
import { EffectIdentityError, RuntimeConfigurationError } from "./errors.js";
import { classifyFailure } from "./failures.js";
import { createPostgresPublisher } from "./publisher.js";
import type { PreparedPendingOccurrence } from "./publisher.js";
import type {
	EffectReference,
	OutstandingEffect,
	OutstandingEffectReadOptions,
	PostgresBorrowedTransaction,
	PostgresPublication,
	PostgresRawClient,
	PostgresTransactionHost,
} from "./types.js";

const preparedCompletion = Symbol("preparedEffectCompletion");

/** Opaque, immutable and reusable only with its originating store. */
export interface PreparedEffectCompletion {
	readonly [preparedCompletion]: true;
}

const transactionOptions = z.strictObject({
	maxWait: z.number().int().positive().max(2_147_483_647).default(2_000),
	timeout: z.number().int().positive().max(2_147_483_647).default(5_000),
});

/** Caller-owned root client. Construction and preparation perform no I/O. */
export function createPostgresEffectStore<
	Definitions extends DefinitionRegistry<
		readonly ScopeStream[],
		readonly EventRegistration[],
		readonly HandlerRegistration[]
	>,
>(options: {
	readonly client: PostgresTransactionHost<PostgresBorrowedTransaction> &
		PostgresRawClient;
	readonly registry: Definitions;
	readonly schema: string;
	/** Follow-up event delivery, NOT effect-task publication. */
	readonly publication?: PostgresPublication;
	readonly transaction?: {
		readonly maxWait?: number;
		readonly timeout?: number;
	};
}) {
	const client = options.client;
	const parsed = transactionOptions.safeParse(
		options.transaction === undefined ? {} : options.transaction,
	);
	if (
		!parsed.success ||
		typeof client?.$transaction !== "function" ||
		typeof client.$queryRawUnsafe !== "function" ||
		typeof client.$connect !== "function" ||
		typeof client.$disconnect !== "function"
	) {
		throw new RuntimeConfigurationError(
			"Effect completion requires a root Prisma host and positive bounded transaction options.",
		);
	}
	const storage = createEffectStorage(options.schema);
	const publisher = createPostgresPublisher({
		registry: options.registry,
		schema: options.schema,
		...(options.publication === undefined
			? {}
			: { publication: options.publication }),
	});
	const proposals = new WeakMap<
		PreparedEffectCompletion,
		{
			readonly effect: OutstandingEffect;
			readonly events: readonly PreparedPendingOccurrence[];
		}
	>();
	return Object.freeze({
		read(reference: EffectReference) {
			return storage.read(client, reference);
		},
		readPending(options?: OutstandingEffectReadOptions) {
			return storage.readPending(client, options);
		},
		prepareCompletion(
			effect: OutstandingEffect,
			receipt: EffectReceipt,
		): PreparedEffectCompletion {
			const outstanding = parseOutstandingEffect(effect);
			const events = Object.freeze(
				parseEffectReceipt(receipt).events.map(({ event, input }) =>
					publisher.prepare(event, input),
				),
			);
			const proposal = Object.freeze({
				[preparedCompletion]: true,
			} satisfies PreparedEffectCompletion);
			proposals.set(proposal, Object.freeze({ effect: outstanding, events }));
			return proposal;
		},
		async complete(
			proposal: PreparedEffectCompletion,
		): Promise<EffectCompletionResult> {
			const prepared = proposals.get(proposal);
			if (!prepared)
				throw new EffectIdentityError(
					"Completion requires a proposal prepared by this effect store.",
				);
			for (let attempt = 0; attempt < 3; attempt++) {
				let committed;
				try {
					committed = await client.$transaction(
						async (transaction) => {
							const status = await storage.guardCompletion(
								transaction,
								prepared.effect,
							);
							if (status === "notRegistered")
								throw new EffectIdentityError(
									"The completion reference is not established.",
								);
							if (status === "alreadyCompleted") return { status };
							const staged = await publisher.stage(
								transaction,
								prepared.events,
							);
							return { status, notify: staged.notifyCommitted };
						},
						{ isolationLevel: "Serializable", ...parsed.data },
					);
				} catch (error) {
					if (classifyFailure(error, "commit") === "retry") {
						if (attempt < 2) continue;
						throw new EffectExecutionError("completionAborted", proposal);
					}
					// Conservative: no automatic replay after any unclassified outcome.
					throw new EffectExecutionError("completionUnknown", proposal);
				}
				// Only confirmed commits notify. The ingress publisher isolates failures.
				await committed.notify?.();
				return { status: committed.status };
			}
			throw new EffectExecutionError("completionAborted", proposal);
		},
	});
}
