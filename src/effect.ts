import { z } from "zod";
import { DefinitionError } from "./errors.js";
import type { PositiveIntegerLiteral } from "./event.js";
import { validateName } from "./ownership.js";

const effectDefinition = Symbol("effectDefinition");

/** Metadata-only view for heterogeneous registration. Effects own no state. */
export interface EffectRegistration {
	readonly name: string;
	readonly version: number;
	readonly schema: z.ZodType;
	readonly [effectDefinition]: true;
}

export interface EffectDefinition<
	Name extends string = string,
	Version extends number = number,
	Schema extends z.ZodType = z.ZodType,
> extends EffectRegistration {
	readonly name: Name;
	readonly version: Version;
	readonly schema: Schema;
}

export type EffectInput<Effect extends EffectRegistration> = z.input<
	Effect["schema"]
>;
export type EffectPayload<Effect extends EffectRegistration> = z.output<
	Effect["schema"]
>;

/** Capture schema input only; the composition boundary owns registration. */
export interface EffectRequester {
	<Effect extends EffectRegistration>(
		effect: Effect,
		input: EffectInput<NoInfer<Effect>>,
	): void;
}

export function validateEffect(effect: {
	readonly name: string;
	readonly version: number;
	readonly schema: z.ZodType;
}): void {
	validateName(effect.name, "Effect name");
	if (!Number.isSafeInteger(effect.version) || effect.version < 1) {
		throw new DefinitionError(
			`Effect "${effect.name}" must have a positive safe-integer version.`,
		);
	}
	if (!(effect.schema instanceof z.ZodType)) {
		throw new DefinitionError(
			`Effect "${effect.name}" must declare a Zod schema.`,
		);
	}
	for (const field of ["stream", "route", "owns"]) {
		if (field in effect) {
			throw new DefinitionError(
				`Effect "${effect.name}" cannot declare "${field}".`,
			);
		}
	}
}

export function defineEffect<
	const Name extends string,
	const Version extends number,
	Schema extends z.ZodType,
>(input: {
	readonly name: Name;
	readonly version: PositiveIntegerLiteral<Version>;
	readonly schema: Schema;
	readonly stream?: never;
	readonly route?: never;
	readonly owns?: never;
}): EffectDefinition<Name, Version, Schema> {
	validateEffect(input);
	return Object.freeze({
		name: input.name,
		version: input.version,
		schema: input.schema,
		[effectDefinition]: true as const,
	});
}
