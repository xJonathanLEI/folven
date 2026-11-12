import { expect, it } from "vitest";
import {
	channelOccurrenceId,
	parseChannelSequence,
	parseOutgoingEnvelope,
	parseOutgoingReference,
	parseStreamIdentity,
} from "./channel.js";
import { ChannelEnvelopeError, PayloadEncodingError } from "./errors.js";
import type { OutgoingEnvelope } from "./index.js";

const envelope = {
	source: { streamName: "author", streamKey: '{"authorId":"u1"}' },
	target: { streamName: "comment", streamKey: '{"number":1,"postId":"p1"}' },
	channelSequence: "1",
	eventName: "comment.add",
	eventVersion: 1,
	payloadFormat: "json-v1",
	encodedInput: '{"postId":"p1","number":1,"body":"hello"}',
} satisfies OutgoingEnvelope;

it("freezes the domain-separated UUIDv8 protocol with independent compatibility vectors", () => {
	expect(channelOccurrenceId(envelope)).toBe(
		"8f8d7dd0-78cc-883c-8596-54c1b98b6641",
	);
	expect(
		channelOccurrenceId({ ...envelope, channelSequence: "9007199254740993" }),
	).toBe("eb62376c-32cf-89d0-8f48-4218809e6f91");
	expect(
		channelOccurrenceId({
			source: { streamName: "作者", streamKey: '{"id":"é"}' },
			target: { streamName: "singleton", streamKey: "{}" },
			channelSequence: "9223372036854775807",
		}),
	).toBe("1de0f287-d7ce-8ee4-99ed-832aa0e3be55");
	const changedFacts = {
		...envelope,
		eventName: "comment.updated",
		eventVersion: 2,
		encodedInput: "{}",
	};
	expect(channelOccurrenceId(changedFacts)).toBe(channelOccurrenceId(envelope));
	const variants = [
		envelope,
		{ ...envelope, channelSequence: "2" },
		{ ...envelope, source: { ...envelope.source, streamName: "other" } },
		{
			...envelope,
			source: { ...envelope.source, streamKey: '{"authorId":"u2"}' },
		},
		{ ...envelope, target: { ...envelope.target, streamName: "other" } },
		{
			...envelope,
			target: { ...envelope.target, streamKey: '{"number":2,"postId":"p1"}' },
		},
		{ ...envelope, source: envelope.target, target: envelope.source },
	];
	expect(new Set(variants.map(channelOccurrenceId)).size).toBe(variants.length);
});

it("parses strict identity-only references with the existing channel protocol", () => {
	const reference = {
		source: envelope.source,
		target: envelope.target,
		channelSequence: "9007199254740993",
	};
	const parsed = parseOutgoingReference(reference);
	expect(parsed).toEqual(reference);
	expect(Object.isFrozen(parsed)).toBe(true);
	expect(Object.isFrozen(parsed.source)).toBe(true);
	expect(Object.isFrozen(parsed.target)).toBe(true);
	for (const invalid of [
		envelope,
		{ ...reference, channelSequence: "01" },
		{ ...reference, channelSequence: "9223372036854775808" },
		{ ...reference, source: { ...reference.source, streamKey: '{ "id":1}' } },
		{ source: reference.source, target: reference.target },
	]) {
		expect(() => parseOutgoingReference(invalid)).toThrow(ChannelEnvelopeError);
	}
});

it("retains bigint positions losslessly and rejects noncanonical sequence forms", () => {
	for (const sequence of [
		"1",
		"9007199254740992",
		"9007199254740993",
		"9223372036854775807",
	]) {
		expect(parseChannelSequence(sequence).toString()).toBe(sequence);
		expect(
			parseOutgoingEnvelope({ ...envelope, channelSequence: sequence })
				.channelSequence,
		).toBe(sequence);
	}
	for (const sequence of [
		0,
		1,
		1n,
		"",
		"0",
		"-1",
		"+1",
		"01",
		"1.0",
		"1e2",
		" 1",
		"1\n",
		"9223372036854775808",
	]) {
		expect(() => parseChannelSequence(sequence)).toThrow(ChannelEnvelopeError);
	}
});

it("validates each canonical stream identity independently without local definitions", () => {
	const identity = {
		streamName: "a",
		streamKey: `{"id":"${"é".repeat(507)}"}`,
	};
	// 1-byte name + 9-byte JSON framing + 1,014 bytes of UTF-8 content.
	expect(parseStreamIdentity(identity)).toEqual(identity);
	expect(
		parseOutgoingEnvelope({ ...envelope, source: identity, target: identity }),
	).toMatchObject({
		source: identity,
		target: identity,
	});
	for (const invalid of [
		{ ...identity, streamName: "aa" },
		{ streamName: "", streamKey: "{}" },
		{ streamName: " author", streamKey: "{}" },
		{ streamName: "bad\0name", streamKey: "{}" },
		{ streamName: "\ud800", streamKey: "{}" },
		...[
			'{"":1}',
			'{" id":1}',
			'{"id ":1}',
			'{"b":1,"a":2}',
			'{"a":1,"a":1}',
			'{ "a":1}',
			'{"a":-0}',
			'{"a":null}',
			'{"a":true}',
			'{"a":""}',
			'{"a":1.5}',
			'{"a":9007199254740992}',
			'{"a":"\\ud800"}',
			'{"a":"\\u0000"}',
			"[]",
			"null",
			"private-not-json",
		].map((streamKey) => ({ streamName: "author", streamKey })),
		{ ...envelope.source, unexpected: true },
	]) {
		expect(() => parseStreamIdentity(invalid)).toThrow();
	}
	expect(
		parseStreamIdentity({
			streamName: "a",
			streamKey: '{"10":1,"2":2,"__proto__":"data"}',
		}).streamKey,
	).toBe('{"10":1,"2":2,"__proto__":"data"}');
});

it("returns immutable envelope snapshots only after checking their wire bytes", () => {
	expect(() =>
		parseOutgoingEnvelope({
			...envelope,
			encodedInput: '{"body":"private","body":"changed"}',
		}),
	).toThrow(PayloadEncodingError);
	const parsed = parseOutgoingEnvelope(envelope);
	expect(parsed).toEqual(envelope);
	expect(Object.isFrozen(parsed)).toBe(true);
	expect(Object.isFrozen(parsed.source)).toBe(true);
	expect(Object.isFrozen(parsed.target)).toBe(true);
});

it("rejects unsupported envelope facts without exposing caller data", () => {
	for (const invalid of [
		{ ...envelope, payloadFormat: "json-v2" },
		{ ...envelope, eventVersion: 0 },
		{ ...envelope, eventVersion: 1.5 },
		{ ...envelope, eventVersion: Number.MAX_SAFE_INTEGER + 1 },
		{ ...envelope, eventName: "" },
		{ ...envelope, eventName: "private\0" },
		{ ...envelope, sourceEventId: "private" },
		{ ...envelope, encodedInput: { private: "not bytes" } },
	]) {
		expect(() => parseOutgoingEnvelope(invalid)).toThrow();
	}
});
