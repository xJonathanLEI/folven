import { z } from "zod";
import { DefinitionError } from "./errors.js";
import { validateName } from "./ownership.js";
import type { IsUnion } from "./prisma-types.js";
import type { ScopeStream } from "./scope.js";
import { validateStream } from "./stream.js";
import type { StreamDefinition, StreamKeyInput } from "./stream.js";

const eventDefinition = Symbol("eventDefinition");

/** Metadata-only view for heterogeneous registration; never invokes routing. */
export interface EventRegistration {
	readonly name: string;
	readonly version: number;
	readonly schema: z.ZodType;
	readonly stream: ScopeStream;
	readonly route: unknown;
	readonly [eventDefinition]: true;
}

export interface EventDefinition<
	Name extends string = string,
	Version extends number = number,
	Schema extends z.ZodType = z.ZodType,
	Stream extends ScopeStream = ScopeStream,
> extends EventRegistration {
	readonly name: Name;
	readonly version: Version;
	readonly schema: Schema;
	readonly stream: Stream;
	readonly route: (event: z.output<Schema>) => StreamKeyInput<Stream>;
}

export type PositiveIntegerLiteral<Version extends number> =
	true extends IsUnion<Version>
		? never
		: number extends Version
			? never
			: `${Version}` extends `${bigint}`
				? `${Version}` extends `-${string}` | "0"
					? never
					: Version
				: never;

export type EventInput<Event extends EventRegistration> = z.input<
	Event["schema"]
>;
export type EventPayload<Event extends EventRegistration> = z.output<
	Event["schema"]
>;

export function validateEvent(event: {
	readonly name: string;
	readonly version: number;
	readonly schema: z.ZodType;
	readonly stream: ScopeStream;
	readonly route: unknown;
}) {
	validateName(event.name, "Event name");
	if (!Number.isSafeInteger(event.version) || event.version < 1) {
		throw new DefinitionError(
			`Event "${event.name}" must have a positive safe-integer version.`,
		);
	}
	if (!(event.schema instanceof z.ZodType)) {
		throw new DefinitionError(
			`Event "${event.name}" must declare a Zod schema.`,
		);
	}
	validateStream(event.stream);
	if (typeof event.route !== "function") {
		throw new DefinitionError(
			`Event "${event.name}" version ${event.version}, stream "${event.stream.name}" must declare a callable route.`,
		);
	}
}

export function defineEvent<
	const Name extends string,
	const Version extends number,
	Schema extends z.ZodType,
	Stream extends ScopeStream,
	RouteResult extends StreamKeyInput<NoInfer<Stream>>,
>(input: {
	readonly name: Name;
	readonly version: PositiveIntegerLiteral<Version>;
	readonly schema: Schema;
	readonly stream: Stream &
		(Stream extends StreamDefinition<infer _Client> ? unknown : never);
	// An empty singleton key is structurally compatible with a Promise;
	// check the inferred return separately without widening the stream or schema.
	readonly route: (
		event: z.output<NoInfer<Schema>>,
	) => RouteResult &
		(Extract<RouteResult, PromiseLike<unknown>> extends never
			? unknown
			: never);
}): EventDefinition<Name, Version, Schema, Stream> {
	validateEvent(input);
	return Object.freeze({
		name: input.name,
		version: input.version,
		schema: input.schema,
		stream: input.stream,
		route: input.route,
		[eventDefinition]: true as const,
	});
}
