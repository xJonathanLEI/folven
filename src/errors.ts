/** A declaration error. Messages contain metadata, never event payloads. */
export class DefinitionError extends Error {
	override readonly name = "DefinitionError";
}

/** The composition boundary has not installed an output collector. */
export class EmissionUnavailableError extends Error {
	override readonly name = "EmissionUnavailableError";
}

/** The composition boundary has not installed an effect collector. */
export class EffectRequestUnavailableError extends Error {
	override readonly name = "EffectRequestUnavailableError";
}
