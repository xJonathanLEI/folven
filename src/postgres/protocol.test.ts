import { expect, it } from "vitest";
import { parseDeliveryEnvelope, parseDeliveryReference } from "./protocol.js";
import type { OutgoingEnvelope, PendingOccurrenceEnvelope } from "./types.js";

const independent = {
	occurrenceId: "81bb4c98-920d-4b94-9e8f-e2c5d7f34510",
	target: { streamName: "comment", streamKey: '{"number":1,"postId":"p1"}' },
	eventName: "comment.add",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"postId":"p1","number":1,"body":"hello"}',
} satisfies PendingOccurrenceEnvelope;
const channel = {
	source: { streamName: "author", streamKey: '{"id":"u1"}' },
	target: independent.target,
	channelSequence: "1",
	eventName: independent.eventName,
	eventVersion: independent.eventVersion,
	payloadFormat: independent.payloadFormat,
	encodedInput: independent.encodedInput,
} satisfies OutgoingEnvelope;

it("parses two exact protocol variants without adding channel metadata", () => {
	const reference = { occurrenceId: independent.occurrenceId };
	expect(parseDeliveryReference(reference)).toEqual(reference);
	const parsed = parseDeliveryEnvelope(independent);
	expect(parsed).toEqual(independent);
	expect(Object.isFrozen(parsed)).toBe(true);
	expect(Object.isFrozen(parsed.target)).toBe(true);
	expect(parsed).not.toBe(independent);
	expect(parsed.target).not.toBe(independent.target);
	expect(parseDeliveryEnvelope(channel)).toEqual(channel);
	const { source, target, channelSequence } = channel;
	expect(parseDeliveryReference({ source, target, channelSequence })).toEqual({
		source,
		target,
		channelSequence,
	});
});

it("rejects mixed forms and noncanonical independent identities and wire facts", () => {
	for (const value of [
		null,
		{},
		{ ...independent, source: channel.source },
		{ ...independent, channelSequence: "1" },
		{ ...channel, occurrenceId: independent.occurrenceId },
		{ ...independent, unexpected: true },
		{ ...independent, occurrenceId: independent.occurrenceId.toUpperCase() },
		{ ...independent, occurrenceId: "not-a-uuid" },
		{ ...independent, occurrenceId: undefined },
		{ ...independent, target: undefined },
		{ ...independent, target: { ...independent.target, extra: true } },
		{
			...independent,
			target: {
				...independent.target,
				streamKey: '{"postId":"p1","number":1}',
			},
		},
		{ ...independent, eventName: "" },
		{ ...independent, eventVersion: 0 },
		{ ...independent, eventVersion: Number.MAX_SAFE_INTEGER + 1 },
		{ ...independent, payloadFormat: "json-v2" },
		{ ...independent, encodedInput: '{"number":1,"number":1}' },
	]) {
		expect(() => parseDeliveryEnvelope(value)).toThrow();
	}
	for (const value of [
		independent,
		{ occurrenceId: independent.occurrenceId, ...channel },
		{ occurrenceId: independent.occurrenceId, extra: true },
		{ occurrenceId: independent.occurrenceId.toUpperCase() },
		{ occurrenceId: "not-a-uuid" },
	]) {
		expect(() => parseDeliveryReference(value)).toThrow();
	}
});
