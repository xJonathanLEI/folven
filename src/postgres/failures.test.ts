import { expect, it } from "vitest";
import { classifyFailure } from "./failures.js";
import {
	incomingPositionConstraint,
	occurrenceConstraint,
	outgoingPositionConstraint,
	streamPositionConstraint,
} from "./schema.js";

it("recognizes only structured known-aborted transactions and append-local exact constraints", () => {
	expect(
		classifyFailure({ code: "P2034", meta: { modelName: "User" } }, "handler"),
	).toBe("retry");
	expect(
		classifyFailure(
			{
				name: "DriverAdapterError",
				cause: { kind: "TransactionWriteConflict" },
			},
			"commit",
		),
	).toBe("retry");
	for (const originalCode of ["40001", "40P01"]) {
		const cause = { originalCode, kind: "TransactionWriteConflict" };
		expect(
			classifyFailure(
				{ code: "P2010", meta: { driverAdapterError: { cause } } },
				"append",
			),
		).toBe("retry");
		expect(
			classifyFailure({ name: "DriverAdapterError", cause }, "commit"),
		).toBe("retry");
		expect(
			classifyFailure({ name: "DriverAdapterError", cause }, "outputs"),
		).toBe("retry");
	}
	const overflow = {
		code: "P2010",
		meta: {
			driverAdapterError: {
				cause: { originalCode: "22003", kind: "postgres" },
			},
		},
	};
	expect(classifyFailure(overflow, "append")).toBe("overflow");
	// Output and publication SQL share a statement; only allocation's explicit
	// exhaustion result can identify channel overflow.
	expect(classifyFailure(overflow, "outputs")).toBe("other");
	expect(classifyFailure(overflow, "handler")).toBe("other");
	for (const [index, expected] of [
		[occurrenceConstraint, "duplicate"],
		[incomingPositionConstraint, "duplicate"],
		[outgoingPositionConstraint, "other"],
		[streamPositionConstraint, "retry"],
		["users_email_key", "other"],
	] as const) {
		const error = {
			code: "P2010",
			meta: {
				driverAdapterError: {
					cause: {
						originalCode: "23505",
						kind: "UniqueConstraintViolation",
						constraint: { index },
					},
				},
			},
		};
		expect(classifyFailure(error, "append")).toBe(expected);
		for (const phase of [
			"begin",
			"admission",
			"handler",
			"outputs",
			"commit",
		] as const) {
			expect(classifyFailure(error, phase)).toBe("other");
		}
	}
	for (const error of [
		{ code: "P2002", meta: { target: ["email"] } },
		{ code: "P2028" },
		{ code: "ECONNRESET" },
		{ code: "23505", constraint: streamPositionConstraint },
		new Error("40001 TransactionWriteConflict"),
		{
			code: "P2010",
			meta: {
				driverAdapterError: {
					cause: { originalCode: "08006", kind: "ConnectionClosed" },
				},
			},
		},
	]) {
		expect(classifyFailure(error, "append")).toBe("other");
		expect(classifyFailure(error, "outputs")).toBe("other");
	}
});
