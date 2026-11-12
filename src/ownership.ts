import { DefinitionError } from "./errors.js";
import type {
	IsKeyComponent,
	IsUnion,
	ModelNames,
	Scalars,
	SelectorFields,
	UniqueSelectors,
} from "./prisma-types.js";

export type Ownership =
	| { readonly kind: "table" }
	| {
			readonly kind: "partition";
			readonly fields: Readonly<Record<string, string>>;
	  }
	| {
			readonly kind: "unique";
			readonly selector: string;
			readonly fields: Readonly<Record<string, string>>;
	  };

export type OwnershipMap = Readonly<Record<string, Ownership>>;

type ImmutableOwner<Owner extends Ownership> = {
	readonly [Property in keyof Owner]: Property extends "fields"
		? Readonly<Owner[Property]>
		: Owner[Property];
};

export type ImmutableOwnership<Owns extends OwnershipMap> = {
	readonly [Model in keyof Owns]: ImmutableOwner<Owns[Model]>;
};

type ValidFields<
	Delegate,
	Key,
	Fields extends Readonly<Record<string, string>>,
> = {
	readonly [Field in keyof Fields]: Field extends keyof Scalars<Delegate>
		? IsKeyComponent<Scalars<Delegate>[Field]> extends true
			? Fields[Field] extends keyof Key
				? IsKeyComponent<Key[Fields[Field]]> extends true
					? Key[Fields[Field]] extends Scalars<Delegate>[Field]
						? Fields[Field]
						: never
					: never
				: never
			: never
		: never;
};

type ValidateOwner<Delegate, Key, Owner extends Ownership> = [Owner] extends [
	{ readonly kind: "table" },
]
	? Record<Exclude<keyof Owner, "kind">, never>
	: [Owner] extends [
				{
					readonly fields: infer Fields extends Readonly<
						Record<string, string>
					>;
				},
		  ]
		? [Owner] extends [
				{
					readonly kind: "unique";
					readonly selector: infer Selector extends string;
				},
			]
			? true extends IsUnion<Selector>
				? never
				: Selector extends UniqueSelectors<Delegate>
					? [keyof Fields] extends [SelectorFields<Delegate, Selector>]
						? [SelectorFields<Delegate, Selector>] extends [keyof Fields]
							? {
									readonly fields: ValidFields<Delegate, Key, Fields>;
								} & Record<
									Exclude<keyof Owner, "kind" | "selector" | "fields">,
									never
								>
							: never
						: never
					: never
			: [Owner] extends [{ readonly kind: "partition" }]
				? { readonly fields: ValidFields<Delegate, Key, Fields> } & Record<
						Exclude<keyof Owner, "kind" | "fields">,
						never
					>
				: never
		: never;

export type ValidateOwnership<Client, Key, Owns extends OwnershipMap> = {
	readonly [Model in keyof Owns]: Model extends ModelNames<Client>
		? ValidateOwner<Client[Model], Key, Owns[Model]>
		: never;
};

function record(value: unknown): value is Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value))
		return false;
	const prototype: unknown = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
}

export function validateName(
	name: unknown,
	context: string,
): asserts name is string {
	if (typeof name !== "string" || name.length === 0 || name.trim() !== name) {
		throw new DefinitionError(
			`${context} must be a nonempty name without surrounding whitespace.`,
		);
	}
}

function exactProperties(
	value: Record<string, unknown>,
	allowed: readonly string[],
	context: string,
) {
	if (
		Reflect.ownKeys(value).some(
			(key) => typeof key !== "string" || !allowed.includes(key),
		)
	) {
		throw new DefinitionError(
			`${context} has unsupported declaration properties.`,
		);
	}
}

/**
 * Runtime shape checks complement (not replace) generated-client type checks.
 * No generated metadata or client instance is consulted here.
 */
export function validateOwnership(
	stream: string,
	owns: unknown,
	keyFields: readonly string[],
	requireCompleteKey: boolean,
) {
	if (!record(owns) || Reflect.ownKeys(owns).length === 0) {
		throw new DefinitionError(
			`Stream "${stream}" must declare at least one owned model.`,
		);
	}
	for (const model of Reflect.ownKeys(owns)) {
		validateName(model, `Stream "${stream}" model`);
		const owner = owns[model];
		const context = `Stream "${stream}", model "${model}"`;
		if (!record(owner))
			throw new DefinitionError(`${context} has invalid ownership.`);
		if (owner.kind === "table") {
			exactProperties(owner, ["kind"], context);
			if (requireCompleteKey && keyFields.length !== 0) {
				throw new DefinitionError(
					`${context} uses whole-table ownership with a varying stream key.`,
				);
			}
			continue;
		}
		if (owner.kind !== "partition" && owner.kind !== "unique") {
			throw new DefinitionError(
				`${context} has an unsupported ownership kind.`,
			);
		}
		if (owner.kind === "unique")
			validateName(owner.selector, `${context} selector`);
		const selectorContext =
			owner.kind === "unique"
				? `${context}, selector "${String(owner.selector)}"`
				: context;
		exactProperties(
			owner,
			owner.kind === "unique"
				? ["kind", "selector", "fields"]
				: ["kind", "fields"],
			selectorContext,
		);
		if (!record(owner.fields) || Reflect.ownKeys(owner.fields).length === 0) {
			throw new DefinitionError(
				`${selectorContext} must map at least one scalar field.`,
			);
		}
		const covered = new Set<string>();
		for (const field of Reflect.ownKeys(owner.fields)) {
			validateName(field, `${selectorContext} field`);
			const component = owner.fields[field];
			if (typeof component !== "string" || !keyFields.includes(component)) {
				throw new DefinitionError(
					`${selectorContext}, field "${field}" must reference a stream-key component.`,
				);
			}
			covered.add(component);
		}
		for (const component of keyFields) {
			if (requireCompleteKey && !covered.has(component)) {
				throw new DefinitionError(
					`${selectorContext} omits stream-key component "${component}".`,
				);
			}
		}
	}
}

/** Copy only metadata. Zod schemas are deliberately not deep-frozen. */
export function copyOwnership<const Owns extends OwnershipMap>(
	owns: Owns,
): ImmutableOwnership<Owns> {
	const copy = Object.fromEntries(
		Object.entries(owns).map(([model, owner]) => [
			model,
			Object.freeze(
				owner.kind === "table"
					? { ...owner }
					: { ...owner, fields: Object.freeze({ ...owner.fields }) },
			),
		]),
	);
	// Object.fromEntries erases the already-validated mapped keys, not their values.
	return Object.freeze(copy) as ImmutableOwnership<Owns>;
}
