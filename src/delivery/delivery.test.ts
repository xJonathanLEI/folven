import { expect, test, vi } from "vitest";
import type {
	IngestionResult,
	OutgoingEnvelope,
	OutgoingReference,
	PendingOccurrenceEnvelope,
} from "../postgres/types.js";
import {
	createDeliveryRunner,
	DeliveryError,
	type DeliveryOptions,
	type DeliverySource,
} from "./index.js";

const reference = {
	source: { streamName: "author", streamKey: '{"id":"a"}' },
	target: { streamName: "comment", streamKey: '{"postId":"p"}' },
	channelSequence: "9007199254740993",
} satisfies OutgoingReference;
const envelope = {
	...reference,
	eventName: "comment.add",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"postId":"p","body":"Retained content"}',
} satisfies OutgoingEnvelope;
const independent = {
	occurrenceId: "00000000-0000-4000-8000-000000000001",
	target: envelope.target,
	eventName: envelope.eventName,
	eventVersion: envelope.eventVersion,
	payloadFormat: envelope.payloadFormat,
	encodedInput: envelope.encodedInput,
} satisfies PendingOccurrenceEnvelope;

function fixture() {
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
	const deliver = vi
		.fn<DeliveryOptions["deliver"]>()
		.mockResolvedValue({ status: "processed" } satisfies IngestionResult);
	return {
		source,
		deliver,
		runner: createDeliveryRunner({
			source: capability,
			deliver,
		}),
	};
}

test.each(["processed", "alreadyProcessed"] as const)(
	"%s forwards retained bytes and completes only after acknowledgment",
	async (status) => {
		const { runner, source, deliver } = fixture();
		const ingestion = Promise.withResolvers<IngestionResult>();
		const acknowledgment = Promise.withResolvers<void>();
		const enteredDelivery = Promise.withResolvers<void>();
		const enteredAcknowledgment = Promise.withResolvers<void>();
		deliver.mockImplementation(() => {
			enteredDelivery.resolve();
			return ingestion.promise;
		});
		source.acknowledge.mockImplementation(() => {
			enteredAcknowledgment.resolve();
			return acknowledgment.promise;
		});
		const finished = vi.fn();
		const attempt = runner.attempt(reference).then((result) => {
			finished();
			return result;
		});
		try {
			await enteredDelivery.promise;
			expect(source.read).toHaveBeenCalledExactlyOnceWith(reference);
			expect(deliver).toHaveBeenCalledExactlyOnceWith(envelope);
			expect(source.acknowledge).not.toHaveBeenCalled();
			ingestion.resolve({ status });
			await enteredAcknowledgment.promise;
			expect(source.acknowledge).toHaveBeenCalledExactlyOnceWith(
				deliver.mock.calls[0]?.[0],
			);
			expect(finished).not.toHaveBeenCalled();
			acknowledgment.resolve();
			expect(await attempt).toEqual({ status: "completed" });
		} finally {
			ingestion.resolve({ status });
			acknowledgment.resolve();
			await attempt;
		}
	},
);

test("independent delivery has no predecessor state and rejects a channel gap without acknowledging", async () => {
	const { source, deliver, runner } = fixture();
	const reference = { occurrenceId: independent.occurrenceId };
	source.read.mockResolvedValue({ envelope: independent, acknowledged: false });
	deliver.mockResolvedValueOnce({ status: "gap", expectedSequence: "1" });
	await expect(runner.attempt(reference)).rejects.toEqual(
		new DeliveryError("response"),
	);
	expect(source.acknowledge).not.toHaveBeenCalled();
	expect(await runner.attempt(reference)).toEqual({ status: "completed" });
	expect(deliver).toHaveBeenLastCalledWith(independent);
	expect(source.acknowledge).toHaveBeenCalledExactlyOnceWith(independent);
	source.read.mockResolvedValue({ envelope: independent, acknowledged: true });
	expect(await runner.attempt(reference)).toEqual({ status: "completed" });
	expect(deliver).toHaveBeenCalledTimes(2);
});

test.each([
	{ envelope: independent, acknowledged: true, hasPredecessor: false },
	{
		envelope: {
			...independent,
			occurrenceId: "00000000-0000-4000-8000-000000000002",
		},
		acknowledged: true,
	},
	{ envelope, acknowledged: true, hasPredecessor: false },
])(
	"independent reads reject readiness metadata, another UUID or the channel alternative",
	async (read) => {
		const { source, deliver, runner } = fixture();
		source.read.mockResolvedValue(read);
		await expect(
			runner.attempt({ occurrenceId: independent.occurrenceId }),
		).rejects.toEqual(new DeliveryError("read"));
		expect(deliver).not.toHaveBeenCalled();
		expect(source.acknowledge).not.toHaveBeenCalled();
	},
);

test.each([true, false])(
	"acknowledged=%s short-circuits without ingestion or acknowledgment",
	async (acknowledged) => {
		const { runner, source, deliver } = fixture();
		source.read.mockResolvedValue({
			envelope,
			acknowledged,
			hasPredecessor: true,
		});
		expect(await runner.attempt(reference)).toEqual(
			acknowledged
				? { status: "completed" }
				: { status: "waiting", reason: "predecessor" },
		);
		expect(deliver).not.toHaveBeenCalled();
		expect(source.acknowledge).not.toHaveBeenCalled();
	},
);

test("a gap preserves its lossless expected sequence and never acknowledges", async () => {
	const { runner, source, deliver } = fixture();
	deliver.mockResolvedValue({
		status: "gap",
		expectedSequence: "9007199254740992",
	} satisfies IngestionResult);
	expect(await runner.attempt(reference)).toEqual({
		status: "waiting",
		reason: "gap",
		expectedSequence: "9007199254740992",
	});
	expect(deliver).toHaveBeenCalledTimes(1);
	expect(source.acknowledge).not.toHaveBeenCalled();
});

test.each([
	undefined,
	{ status: "success" },
	{ status: "processed", result: "private handler result" },
	{ status: "gap", expectedSequence: "0" },
])("invalid ingestion response %j cannot acknowledge", async (response) => {
	const { runner, source, deliver } = fixture();
	// @ts-expect-error Simulate a callback violating its declared ingestion contract.
	deliver.mockResolvedValue(response);
	await expect(runner.attempt(reference)).rejects.toEqual(
		new DeliveryError("response"),
	);
	expect(deliver).toHaveBeenCalledTimes(1);
	expect(source.acknowledge).not.toHaveBeenCalled();
});

test.each(["read", "delivery", "acknowledgment"] as const)(
	"%s failure is sanitized, does not retry inline, and allows a later attempt",
	async (stage) => {
		const { runner, source, deliver } = fixture();
		const failed =
			stage === "read"
				? source.read
				: stage === "delivery"
					? deliver
					: source.acknowledge;
		failed.mockRejectedValueOnce(
			new Error("Private payload, stream key or connection URL"),
		);
		const error = await runner
			.attempt(reference)
			.catch((error: unknown) => error);
		expect(error).toBeInstanceOf(DeliveryError);
		expect(error).toMatchObject({ stage });
		expect(error).toEqual(new DeliveryError(stage));
		expect(error).not.toHaveProperty("cause");
		expect(JSON.stringify(error)).not.toContain("Private");
		expect(source.read).toHaveBeenCalledTimes(1);
		expect(deliver).toHaveBeenCalledTimes(stage === "read" ? 0 : 1);
		expect(source.acknowledge).toHaveBeenCalledTimes(
			stage === "acknowledgment" ? 1 : 0,
		);
		expect(await runner.attempt(reference)).toEqual({ status: "completed" });
		expect(source.read).toHaveBeenLastCalledWith(reference);
		expect(source.acknowledge).toHaveBeenLastCalledWith(envelope);
	},
);

test.each([
	{ ...envelope, encodedInput: "invalid JSON" },
	{ ...envelope, channelSequence: "1" },
	{ ...envelope, source: { ...envelope.source, streamKey: '{"id":"other"}' } },
	{ ...envelope, target: { ...envelope.target, streamName: "other" } },
])(
	"invalid or mismatched retained output fails closed, even when acknowledged",
	async (retained) => {
		const { runner, source, deliver } = fixture();
		source.read.mockResolvedValue({
			envelope: retained,
			acknowledged: true,
			hasPredecessor: false,
		});
		await expect(runner.attempt(reference)).rejects.toEqual(
			new DeliveryError("read"),
		);
		expect(deliver).not.toHaveBeenCalled();
		expect(source.acknowledge).not.toHaveBeenCalled();
	},
);
