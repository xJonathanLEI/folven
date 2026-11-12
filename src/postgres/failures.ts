import { z } from "zod";
import {
	incomingPositionConstraint,
	occurrenceConstraint,
	streamPositionConstraint,
} from "./schema.js";

const causeSchema = z.object({
	originalCode: z.string().optional(),
	kind: z.string(),
	constraint: z.object({ index: z.string() }).optional(),
});
const prismaError = z.object({ code: z.string() });
const rawError = z.object({
	code: z.literal("P2010"),
	meta: z.object({ driverAdapterError: z.object({ cause: causeSchema }) }),
});
const adapterError = z.object({
	name: z.literal("DriverAdapterError"),
	cause: causeSchema,
});

/** No message matching. Unknown/uncertain failures are never replayed. */
export function classifyFailure(
	error: unknown,
	phase: "begin" | "append" | "admission" | "handler" | "outputs" | "commit",
): "retry" | "duplicate" | "overflow" | "other" {
	const prisma = prismaError.safeParse(error);
	if (prisma.success && prisma.data.code === "P2034") return "retry";
	const raw = rawError.safeParse(error);
	const adapter = adapterError.safeParse(error);
	const cause = raw.success
		? raw.data.meta.driverAdapterError.cause
		: adapter.success
			? adapter.data.cause
			: undefined;
	if (!cause) return "other";
	if (
		cause.kind === "TransactionWriteConflict" &&
		(cause.originalCode === "40001" ||
			cause.originalCode === "40P01" ||
			(adapter.success && cause.originalCode === undefined))
	) {
		return "retry";
	}
	if (phase === "append" && cause.originalCode === "22003") return "overflow";
	if (phase !== "append") return "other";
	if (
		cause.originalCode === "23505" &&
		cause.kind === "UniqueConstraintViolation"
	) {
		if (
			cause.constraint?.index === occurrenceConstraint ||
			cause.constraint?.index === incomingPositionConstraint
		)
			return "duplicate";
		if (cause.constraint?.index === streamPositionConstraint) return "retry";
	}
	return "other";
}
