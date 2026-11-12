import type { Types } from "@prisma/client/runtime/client";
import type { IsAny, ModelNames, Scalars } from "./prisma-types.js";
import type { BoundFields, ScopeStream, UniqueScope } from "./scope.js";
import type { StreamDefinition } from "./stream.js";

type Operation =
	| "findUnique"
	| "findUniqueOrThrow"
	| "findFirst"
	| "findFirstOrThrow"
	| "findMany"
	| "count"
	| "aggregate"
	| "create"
	| "createMany"
	| "update"
	| "updateMany"
	| "upsert"
	| "delete"
	| "deleteMany";

type UniqueOperation =
	| "findUnique"
	| "findUniqueOrThrow"
	| "update"
	| "upsert"
	| "delete";

type Relations<Delegate> =
	Types.Public.Payload<Delegate> extends { objects: infer Objects }
		? keyof Objects
		: never;

type Forbid<Keys extends PropertyKey> = { [Key in Keys]?: never };

/** Recurse only at Prisma's model-filter boolean operators, never scalar data. */
type Filter<Value, Delegate> = Value extends (infer Item)[]
	? Filter<Item, Delegate>[]
	: Value extends object
		? {
				[Key in keyof Value]: Key extends Relations<Delegate>
					? never
					: Key extends "AND" | "OR" | "NOT"
						? Filter<Value[Key], Delegate>
						: Value[Key];
			} & Forbid<Relations<Delegate>>
		: Value;

type ScalarSelection<
	Value,
	Delegate,
	Extra extends PropertyKey = never,
> = Value extends object
	? {
			[Key in keyof Value]: Key extends keyof Scalars<Delegate> | Extra
				? Value[Key]
				: never;
		} & Forbid<Relations<Delegate> | "_count">
	: Value;

type ScalarOrder<Value, Delegate> = Value extends (infer Item)[]
	? ScalarOrder<Item, Delegate>[]
	: Value extends object
		? {
				[Key in keyof Value]: Key extends keyof Scalars<Delegate>
					? Value[Key]
					: never;
			} & Forbid<Relations<Delegate>>
		: Value;

/** Distributes over Prisma's checked/unchecked XOR without losing requirements. */
type WriteData<
	Value,
	Delegate,
	Fields,
	Creating extends boolean,
> = Value extends (infer Item)[]
	? WriteData<Item, Delegate, Fields, Creating>[]
	: Value extends object
		? {
				[Key in keyof Value]: Key extends Relations<Delegate>
					? never
					: Creating extends false
						? Key extends keyof Fields
							? never
							: Value[Key]
						: Value[Key];
			} & Forbid<Relations<Delegate>> &
				(Creating extends true ? Fields : Forbid<keyof Fields>)
		: Value;

type AllowedArgument =
	| "where"
	| "select"
	| "data"
	| "create"
	| "update"
	| "orderBy"
	| "distinct"
	| "take"
	| "skip"
	| "limit"
	| "skipDuplicates"
	| "_count"
	| "_avg"
	| "_sum"
	| "_min"
	| "_max";

type RestrictArgs<Args, Delegate, Op, Fields, Where> = {
	[Key in keyof Args]: Key extends AllowedArgument
		? Key extends "where"
			? Filter<Args[Key], Delegate>
			: Key extends "select"
				? Op extends "count"
					? ScalarSelection<Args[Key], Delegate, "_all">
					: ScalarSelection<Args[Key], Delegate>
				: Key extends "_count" | "_avg" | "_sum" | "_min" | "_max"
					? ScalarSelection<Args[Key], Delegate, "_all">
					: Key extends "orderBy"
						? ScalarOrder<Args[Key], Delegate>
						: Key extends "data" | "create" | "update"
							? WriteData<
									Args[Key],
									Delegate,
									Fields,
									Key extends "create"
										? true
										: Op extends "create" | "createMany"
											? true
											: false
								>
							: Args[Key]
		: never;
} & (Op extends "create" | "createMany" ? unknown : { where: Where }) &
	Forbid<"include" | "cursor" | "omit" | "relationLoadStrategy">;

type Arguments<
	Delegate,
	Op extends Operation,
	Stream extends ScopeStream,
	Model extends keyof Stream["owns"],
	Invocation,
> = Op extends keyof Delegate
	? Types.Public.Args<Delegate, Op> extends infer Args
		? IsAny<Args> extends true
			? never
			: Args &
					RestrictArgs<
						Args,
						Delegate,
						Op,
						BoundFields<Stream, Model, Invocation>,
						Op extends UniqueOperation
							? UniqueScope<Stream, Model, Invocation>
							: BoundFields<Stream, Model, Invocation>
					>
		: never
	: never;

type DelegateFence<
	Delegate,
	Stream extends ScopeStream,
	Model extends keyof Stream["owns"],
	Invocation,
> = {
	readonly [Op in Operation]: <
		Args extends Arguments<Delegate, Op, Stream, Model, Invocation>,
	>(
		args: Args &
			// Check the inferred finite filter too: recursive generated filter
			// intersections alone can trigger TypeScript's recursive-type shortcut.
			{
				[Key in keyof NoInfer<Args>]: Key extends "where"
					? Filter<Args[Key], Delegate>
					: Args[Key];
			} & Record<
				Exclude<
					keyof Args,
					keyof Arguments<Delegate, Op, Stream, Model, Invocation>
				>,
				never
			>,
	) => Promise<Types.Public.Result<Delegate, Args, Op>>;
};

/** A type view only. No delegate, query, or result is wrapped at runtime. */
export type FencedPrisma<
	Client,
	Stream extends StreamDefinition<Client>,
	Invocation,
> =
	IsAny<Client> extends true
		? never
		: Exclude<keyof Stream["owns"], ModelNames<Client>> extends never
			? {
					readonly [Model in keyof Stream["owns"]]: Model extends keyof Client
						? DelegateFence<Client[Model], Stream, Model, Invocation>
						: never;
				}
			: never;
