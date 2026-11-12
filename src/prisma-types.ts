import type { Types } from "@prisma/client/runtime/client";

export type IsAny<Value> = 0 extends 1 & Value ? true : false;
type IsNever<Value> = [Value] extends [never] ? true : false;

export type IsUnion<Value, Whole = Value> = Value extends Whole
	? [Whole] extends [Value]
		? false
		: true
	: never;

type Payload<Delegate> = Types.Public.Payload<Delegate>;
type UniqueWhere<Delegate> =
	Types.Public.Args<Delegate, "findUnique"> extends { where: infer Where }
		? Where
		: never;

/** Fail closed when Prisma's public utilities cannot recognize a delegate. */
type IsDelegate<Delegate> =
	IsAny<Delegate> extends true
		? false
		: IsAny<Payload<Delegate>> extends true
			? false
			: IsNever<Payload<Delegate>> extends true
				? false
				: Payload<Delegate> extends {
							name: string;
							scalars: object;
							objects: object;
					  }
					? IsAny<UniqueWhere<Delegate>> extends true
						? false
						: IsNever<UniqueWhere<Delegate>> extends true
							? false
							: IsAny<Scalars<Delegate>> extends true
								? false
								: IsAny<Payload<Delegate>["objects"]> extends true
									? false
									: true
					: false;

export type ModelNames<Client> =
	IsAny<Client> extends true
		? never
		: {
				[Model in keyof Client]: IsDelegate<Client[Model]> extends true
					? Model
					: never;
			}[keyof Client] &
				string;

export type Scalars<Delegate> =
	Payload<Delegate> extends { scalars: infer Fields } ? Fields : never;

/** Nullable, optional, list, JSON, date, boolean and other keys are not v1 keys. */
export type IsKeyComponent<Value> =
	IsAny<Value> extends true
		? false
		: IsNever<Value> extends true
			? false
			: [Value] extends [string]
				? true
				: [Value] extends [number]
					? true
					: false;

type SelectorValue<
	Delegate,
	Selector extends PropertyKey,
> = Selector extends keyof Scalars<Delegate>
	? Scalars<Delegate>[Selector]
	: Selector extends keyof UniqueWhere<Delegate>
		? NonNullable<UniqueWhere<Delegate>[Selector]>
		: never;

type UniqueSelector<Delegate, Selector extends keyof UniqueWhere<Delegate>> =
	IsNever<SelectorValue<Delegate, Selector>> extends true
		? never
		: {
					[Field in Selector]-?: SelectorValue<Delegate, Selector>;
			  } extends UniqueWhere<Delegate>
			? Selector
			: never;

/** An isolated selector must satisfy Prisma's own AtLeast unique requirement. */
export type UniqueSelectors<Delegate> = {
	[Selector in keyof UniqueWhere<Delegate>]: UniqueSelector<Delegate, Selector>;
}[keyof UniqueWhere<Delegate>] &
	string;

export type SelectorFields<
	Delegate,
	Selector extends string,
> = Selector extends keyof Scalars<Delegate>
	? Selector
	: keyof SelectorValue<Delegate, Selector> & string;
