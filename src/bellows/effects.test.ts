import { TaskFailure, TaskSuccess } from "@xjonathanlei/bellows";
import { expect, test, vi } from "vitest";
import {
	EffectExecutionError,
	type createEffectRunner,
} from "../effects/index.js";
import type { EffectReference } from "../postgres/index.js";
import {
	createBellowsEffects,
	type BellowsEffectsOptions,
	type EffectFailurePolicy,
} from "./index.js";

const reference = {
	source: { streamName: "user", streamKey: '{"id":"u"}' },
	effectSequence: "9007199254740993",
} satisfies EffectReference;

test("effect task codecs retain precise coordinates and reject envelope metadata in either direction", () => {
	const { task } = createBellowsEffects({ schema: "tasks" });
	expect(task.codec.decode(task.codec.encode(reference))).toEqual(reference);
	for (const extra of [
		{ encodedInput: "{}" },
		{ occurrenceId: "00000000-0000-4000-8000-000000000001" },
		{ target: reference.source },
		{ effectSequence: 1 },
	]) {
		// @ts-expect-error Deliberately malformed wire reference with a numeric sequence.
		expect(() => task.codec.encode({ ...reference, ...extra })).toThrow(
			"Invalid Folven effect task reference.",
		);
		expect(() =>
			task.codec.decode(JSON.stringify({ ...reference, ...extra })),
		).toThrow("Invalid Folven effect task reference.");
	}
	expect(() => task.codec.decode("{")).toThrow(
		"Invalid Folven effect task reference.",
	);
});

test("all bounded execution stages reach the explicit policy without leaking arbitrary causes", async () => {
	for (const stage of [
		"read",
		"definition",
		"input",
		"handler",
		"receipt",
		"completionAborted",
		"completionUnknown",
	] as const) {
		const error = new EffectExecutionError(stage);
		const attempt = vi
			.fn<ReturnType<typeof createEffectRunner>["attempt"]>()
			.mockRejectedValue(error);
		const policy = vi
			.fn<EffectFailurePolicy>()
			.mockResolvedValue({ action: "retryAt", availableFromMs: 1234 });
		const diagnostic = vi
			.fn<NonNullable<BellowsEffectsOptions["onDiagnostic"]>>()
			.mockRejectedValue(new Error("Private observer detail"));
		const worker = createBellowsEffects({
			schema: "tasks",
			now: () => 1000,
			onDiagnostic: diagnostic,
		}).createWorkerFactory({
			runner: { attempt },
			onFailure: policy,
		});
		expect(await worker.factory.build(1).process(7, reference)).toEqual(
			TaskFailure.retryAt(1234),
		);
		expect(attempt).toHaveBeenCalledExactlyOnceWith(reference);
		expect(policy).toHaveBeenCalledExactlyOnceWith({
			taskId: 7,
			taskName: "folven_effect",
			reference,
			stage,
			externalWork: error.externalWork,
		});
		expect(diagnostic).toHaveBeenCalledExactlyOnceWith({
			taskId: 7,
			taskName: "folven_effect",
			stage,
			status: "retry",
		});
		await worker.drain();
	}
});

test("manual, invalid policies and invalid retry clocks finish scheduling without retrying or claiming effect completion", async () => {
	const attempt = vi
		.fn<ReturnType<typeof createEffectRunner>["attempt"]>()
		.mockRejectedValue(new Error("Private provider data"));
	for (const outcome of [
		"manual",
		"throw",
		"past",
		"invalid",
		"clock",
	] as const) {
		const policy = vi.fn<EffectFailurePolicy>();
		if (outcome === "throw")
			policy.mockRejectedValue(new Error("Private policy data"));
		else if (outcome === "invalid") {
			// @ts-expect-error Deliberately malformed external policy result.
			policy.mockResolvedValue({ action: "automatic" });
		} else
			policy.mockResolvedValue(
				outcome === "manual"
					? { action: "manual" }
					: { action: "retryAt", availableFromMs: 1000 },
			);
		const diagnostic =
			vi.fn<NonNullable<BellowsEffectsOptions["onDiagnostic"]>>();
		const worker = createBellowsEffects({
			schema: "tasks",
			now: () => (outcome === "clock" ? Number.NaN : 1000),
			onDiagnostic: diagnostic,
		}).createWorkerFactory({ runner: { attempt }, onFailure: policy });
		expect(await worker.factory.build(1).process(7, reference)).toEqual(
			TaskSuccess.done(undefined),
		);
		expect(policy).toHaveBeenCalledExactlyOnceWith({
			taskId: 7,
			taskName: "folven_effect",
			reference,
			stage: "processing",
			externalWork: "mayHaveRun",
		});
		expect(diagnostic).toHaveBeenCalledExactlyOnceWith({
			taskId: 7,
			taskName: "folven_effect",
			stage: outcome === "manual" ? "processing" : "policy",
			status: "manual",
		});
		await worker.drain();
	}
});

test("a missing failure policy fails at construction", () => {
	const runner = {
		attempt: vi.fn<ReturnType<typeof createEffectRunner>["attempt"]>(),
	};
	const adapter = createBellowsEffects({ schema: "tasks" });
	// @ts-expect-error JavaScript callers must supply an explicit failure policy.
	expect(() => adapter.createWorkerFactory({ runner })).toThrow(
		"failure policy is required",
	);
});
