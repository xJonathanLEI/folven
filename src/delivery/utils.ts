import { z } from "zod";
import {
	parseChannelSequence,
	parseOutgoingEnvelope,
} from "../postgres/channel.js";
import { parsePendingOccurrenceEnvelope } from "../postgres/protocol.js";
import type {
	DeliveryRead,
	DeliveryReference,
	IngestionResult,
} from "../postgres/types.js";
import { DeliveryError } from "./errors.js";

const readSchema = z.strictObject({
	envelope: z.unknown(),
	acknowledged: z.boolean(),
	hasPredecessor: z.boolean(),
});
const independentReadSchema = z.strictObject({
	envelope: z.unknown(),
	acknowledged: z.boolean(),
});
const resultSchema = z.discriminatedUnion("status", [
	z.strictObject({ status: z.literal("processed") }),
	z.strictObject({ status: z.literal("alreadyProcessed") }),
	z.strictObject({
		status: z.literal("gap"),
		expectedSequence: z.string(),
	}),
]);

export function parseDeliveryRead(
	value: unknown,
	reference: DeliveryReference,
): DeliveryRead {
	if ("occurrenceId" in reference) {
		const read = independentReadSchema.parse(value);
		const envelope = parsePendingOccurrenceEnvelope(read.envelope);
		if (envelope.occurrenceId !== reference.occurrenceId) {
			throw new DeliveryError("read");
		}
		return { ...read, envelope };
	}
	const read = readSchema.parse(value);
	const envelope = parseOutgoingEnvelope(read.envelope);
	if (
		envelope.source.streamName !== reference.source.streamName ||
		envelope.source.streamKey !== reference.source.streamKey ||
		envelope.target.streamName !== reference.target.streamName ||
		envelope.target.streamKey !== reference.target.streamKey ||
		envelope.channelSequence !== reference.channelSequence
	) {
		throw new DeliveryError("read");
	}
	return { ...read, envelope };
}

export function parseIngestionResult(value: unknown): IngestionResult {
	const result = resultSchema.parse(value);
	if (result.status === "gap") parseChannelSequence(result.expectedSequence);
	return result;
}
