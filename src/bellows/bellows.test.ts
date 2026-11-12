import { TaskFailure, TaskSuccess } from "@xjonathanlei/bellows";
import { expect, test, vi } from "vitest";
import type { DeliveryOptions, DeliverySource } from "../delivery/index.js";
import type { OutgoingEnvelope, OutgoingReference } from "../postgres/index.js";
import {
	createBellowsDelivery,
	type BellowsDeliveryOptions,
	type DeliveryTaskReceipt,
} from "./index.js";

const reference = {
	source: { streamName: "post", streamKey: '{"id":"p"}' },
	target: { streamName: "comment", streamKey: '{"number":1,"postId":"p"}' },
	channelSequence: "9007199254740993",
} satisfies OutgoingReference;
const envelope = {
	...reference,
	eventName: "comment.create",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"postId":"p","number":1,"body":"Private comment"}',
} satisfies OutgoingEnvelope;
const preparation = {
	source: { relation: "delivery_inputs", payloadColumn: "encoded_payload" },
	parameterOffset: 3,
};

function capabilities() {
	const capability: DeliverySource = {
		async read() {
			throw new Error("Unconfigured read");
		},
		async acknowledge() {
			throw new Error("Unconfigured acknowledgment");
		},
	};
	const source = {
		read: vi.spyOn(capability, "read").mockResolvedValue({
			envelope,
			acknowledged: false,
			hasPredecessor: false,
		}),
		acknowledge: vi.spyOn(capability, "acknowledge").mockResolvedValue(),
	};
	const deliver = vi.fn<DeliveryOptions["deliver"]>().mockResolvedValue({
		status: "processed",
	});
	return {
		source,
		deliver,
		options: {
			source: capability,
			deliver,
		} satisfies DeliveryOptions,
	};
}

test("codec preserves exact references and rejects event bodies in either direction", () => {
	const { task } = createBellowsDelivery({ schema: "tasks" });
	expect(task.codec.decode(task.codec.encode(reference))).toEqual(reference);
	const independent = { occurrenceId: "00000000-0000-4000-8000-000000000001" };
	expect(task.codec.decode(task.codec.encode(independent))).toEqual(
		independent,
	);
	expect(() => task.codec.encode(envelope)).toThrow(
		"Invalid Folven delivery task reference.",
	);
	for (const value of [
		"{",
		JSON.stringify({ ...reference, channelSequence: 1 }),
		JSON.stringify({ ...reference, ...independent }),
		JSON.stringify({ ...independent, encodedInput: "{}" }),
		JSON.stringify({ occurrenceId: "not-a-uuid" }),
	]) {
		expect(() => task.codec.decode(value)).toThrow(
			"Invalid Folven delivery task reference.",
		);
	}
	for (const value of [
		{ ...reference, ...independent },
		{ ...independent, target: reference.target },
	]) {
		expect(() => task.codec.encode(value)).toThrow(
			"Invalid Folven delivery task reference.",
		);
	}
	expect(
		task.callbackCodec.decode(task.callbackCodec.encode(undefined)),
	).toBeUndefined();
});

test.each([
	{ schema: "Tasks" },
	{ schema: "a".repeat(64) },
	{ schema: "tasks\n" },
	{ schema: "tasks", taskName: "not a task name" },
	{ schema: "tasks", retryDelayMs: 0 },
	{ schema: "tasks", retryDelayMs: 86_400_001 },
])("rejects invalid adapter configuration: %j", (options) => {
	expect(() => createBellowsDelivery(options)).toThrow(
		"Invalid Bellows delivery configuration.",
	);
});

test("pure preparation keeps each action's checked receipts separate", async () => {
	const committed =
		vi.fn<NonNullable<BellowsDeliveryOptions["onTasksCommitted"]>>();
	const delivery = createBellowsDelivery({
		schema: "tasks",
		taskName: "comments_delivery",
		onTasksCommitted: committed,
	});
	const fragment = delivery.publication.prepare(preparation);
	const discarded = await fragment.validate(
		[{ task_id: "9007199254740991" }],
		1,
	);
	const action = await fragment.validate(
		[{ task_id: "3" }, { task_id: "4" }],
		2,
	);
	void discarded;
	expect(fragment.parameters).toEqual(["comments_delivery"]);
	expect(committed).not.toHaveBeenCalled();
	await action();
	expect(committed).toHaveBeenCalledExactlyOnceWith([
		{ taskId: 3, taskName: "comments_delivery" },
		{ taskId: 4, taskName: "comments_delivery" },
	]);
	const receipts = committed.mock.calls[0]?.[0];
	expect(Object.isFrozen(receipts)).toBe(true);
	expect(Object.isFrozen(receipts?.[0])).toBe(true);
	await (
		await fragment.validate([], 0)
	)();
	expect(committed).toHaveBeenCalledTimes(1);
});

test.each([
	{ rows: [] },
	{ rows: [{ task_id: 1 }] },
	{ rows: [{ task_id: "private-invalid-id" }] },
	{ rows: [{ task_id: "0" }] },
	{ rows: [{ task_id: "01" }] },
	{ rows: [{ task_id: "9007199254740992" }] },
	{ rows: [{ task_id: "1" }, { task_id: "2" }] },
	{ rows: [{ task_id: "1" }, { task_id: "1" }], count: 2 },
])(
	"fails publication closed for invalid returned task identities: %j",
	({ rows, count = 1 }) => {
		const delivery = createBellowsDelivery({ schema: "tasks" });
		expect(() =>
			delivery.publication.prepare(preparation).validate(rows, count),
		).toThrow("Invalid Bellows publication identities or count.");
	},
);

test.each(["throw", "reject"] as const)(
	"isolates notification %s and diagnostics rejection without republishing",
	async (failure) => {
		const onDiagnostic = vi
			.fn<NonNullable<BellowsDeliveryOptions["onDiagnostic"]>>()
			.mockRejectedValue(new Error("Private observer failure"));
		const delivery = createBellowsDelivery({
			schema: "tasks",
			onTasksCommitted() {
				if (failure === "throw") throw new Error("Private synchronous failure");
				return Promise.reject(new Error("Private asynchronous failure"));
			},
			onDiagnostic,
		});
		const action = await delivery.publication
			.prepare(preparation)
			.validate([{ task_id: "1" }], 1);
		await expect(action()).resolves.toBeUndefined();
		expect(onDiagnostic).toHaveBeenCalledExactlyOnceWith({
			taskId: 1,
			taskName: "folven_delivery",
			stage: "notification",
			status: "failed",
		});
	},
);

test("notification stays owned until its returned promise settles", async () => {
	const released = Promise.withResolvers<void>();
	const receipts: DeliveryTaskReceipt[] = [];
	const delivery = createBellowsDelivery({
		schema: "tasks",
		async onTasksCommitted(committed) {
			receipts.push(...committed);
			await released.promise;
		},
	});
	const action = await delivery.publication
		.prepare(preparation)
		.validate([{ task_id: "1" }], 1);
	const finished = vi.fn();
	const pending = Promise.resolve(action()).then(finished);
	try {
		await Promise.resolve();
		expect(receipts).toEqual([{ taskId: 1, taskName: "folven_delivery" }]);
		expect(finished).not.toHaveBeenCalled();
	} finally {
		released.resolve();
		await pending;
	}
	expect(finished).toHaveBeenCalledOnce();
});

test.each(["processed", "acknowledged"] as const)(
	"maps %s completion to undefined-callback Bellows success",
	async (status) => {
		const options = capabilities();
		if (status === "acknowledged") {
			options.source.read.mockResolvedValue({
				envelope,
				acknowledged: true,
				hasPredecessor: false,
			});
		} else {
			options.deliver.mockResolvedValue({ status });
		}
		const worker = createBellowsDelivery({
			schema: "tasks",
		}).createWorkerFactory(options.options);
		const result = await worker.factory.build(1).process(7, reference);
		expect(result).toBeInstanceOf(TaskSuccess);
		expect(result).toEqual(TaskSuccess.done(undefined));
		expect(options.source.acknowledge).toHaveBeenCalledTimes(
			status === "acknowledged" ? 0 : 1,
		);
		await worker.drain();
	},
);

test.each([
	"predecessor",
	"gap",
	"read",
	"delivery",
	"response",
	"acknowledgment",
] as const)(
	"maps %s to a nonzero fixed retry without inline retries or leaked errors",
	async (outcome) => {
		const options = capabilities();
		const failure = new Error("Private payload/provider detail");
		if (outcome === "predecessor")
			options.source.read.mockResolvedValue({
				envelope,
				acknowledged: false,
				hasPredecessor: true,
			});
		if (outcome === "gap")
			options.deliver.mockResolvedValue({
				status: "gap",
				expectedSequence: "1",
			});
		if (outcome === "read") options.source.read.mockRejectedValue(failure);
		if (outcome === "delivery") options.deliver.mockRejectedValue(failure);
		if (outcome === "response")
			options.deliver.mockImplementation(async () => ({
				status: "processed",
				result: "Private handler result that must never escape",
			}));
		if (outcome === "acknowledgment")
			options.source.acknowledge.mockRejectedValue(failure);
		const onDiagnostic = vi
			.fn<NonNullable<BellowsDeliveryOptions["onDiagnostic"]>>()
			.mockRejectedValue(new Error("Observer failure"));
		const worker = createBellowsDelivery({
			schema: "tasks",
			now: () => 10_000,
			retryDelayMs: 123,
			onDiagnostic,
		}).createWorkerFactory(options.options);
		const result = await worker.factory.build(1).process(7, reference);
		expect(result).toBeInstanceOf(TaskFailure);
		expect(result).toEqual(TaskFailure.retryAt(10_123));
		expect(options.source.read).toHaveBeenCalledTimes(1);
		expect(options.deliver).toHaveBeenCalledTimes(
			outcome === "predecessor" || outcome === "read" ? 0 : 1,
		);
		expect(options.source.acknowledge).toHaveBeenCalledTimes(
			outcome === "acknowledgment" ? 1 : 0,
		);
		expect(onDiagnostic).toHaveBeenCalledExactlyOnceWith({
			taskId: 7,
			taskName: "folven_delivery",
			stage:
				outcome === "predecessor" || outcome === "gap" ? "processing" : outcome,
			status:
				outcome === "predecessor" || outcome === "gap" ? outcome : "failed",
		});
		await worker.drain();
	},
);

test("invalid clocks fail closed without acknowledgment and remain drainable", async () => {
	const options = capabilities();
	options.deliver.mockResolvedValue({ status: "gap", expectedSequence: "1" });
	const worker = createBellowsDelivery({
		schema: "tasks",
		now: () => Number.NaN,
	}).createWorkerFactory(options.options);
	await expect(worker.factory.build(1).process(7, reference)).rejects.toThrow(
		"Invalid Bellows delivery clock.",
	);
	expect(options.source.acknowledge).not.toHaveBeenCalled();
	await worker.drain();
});

test("drain retains concurrent business work through ingestion and acknowledgment even when the host stops waiting", async () => {
	const entered = Promise.withResolvers<void>();
	const deliver = Promise.withResolvers<void>();
	const acknowledging = Promise.withResolvers<void>();
	const acknowledge = Promise.withResolvers<void>();
	const options = capabilities();
	let started = 0;
	options.deliver.mockImplementation(async () => {
		if (++started === 2) entered.resolve();
		await deliver.promise;
		return { status: "processed" };
	});
	options.source.acknowledge.mockImplementation(async () => {
		acknowledging.resolve();
		await acknowledge.promise;
	});
	const worker = createBellowsDelivery({ schema: "tasks" }).createWorkerFactory(
		options.options,
	);
	// Lifecycle double: the invoker no longer awaits these business promises.
	// No claim is made here about Bellows lease timers or Cloudflare termination.
	const operations = [
		worker.factory.build(1).process(7, reference),
		worker.factory.build(2).process(7, reference),
	];
	const drained = vi.fn();
	const drain = worker.drain().then(drained);
	try {
		await entered.promise;
		expect(drained).not.toHaveBeenCalled();
		deliver.resolve();
		await acknowledging.promise;
		expect(drained).not.toHaveBeenCalled();
	} finally {
		deliver.resolve();
		acknowledge.resolve();
		await Promise.all(operations);
		await drain;
	}
	expect(drained).toHaveBeenCalledOnce();
	await worker.drain();
});
