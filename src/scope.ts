import type { z } from "zod";
import type { OwnershipMap } from "./ownership.js";
import type { StreamKey } from "./stream.js";

declare const boundComponent: unique symbol;

/** Structural metadata used by the type fence, not a second definition API. */
export interface ScopeStream {
	readonly name: string;
	readonly key: z.ZodObject;
	readonly owns: OwnershipMap;
}

type Component<Value, Role, Invocation> = Value & {
	readonly [boundComponent]: {
		readonly role: Role;
		// Function property (not a method): both parameter and result are checked.
		readonly invocation: (token: Invocation) => Invocation;
	};
};

export type BoundFields<
	Stream extends ScopeStream,
	Model extends keyof Stream["owns"],
	Invocation,
> = Stream["owns"][Model] extends {
	readonly fields: infer Fields extends Readonly<Record<string, string>>;
}
	? {
			readonly [Field in keyof Fields]: Component<
				StreamKey<Stream>[Fields[Field] & keyof StreamKey<Stream>],
				readonly [Stream["name"], Model, Field, Fields[Field]],
				Invocation
			>;
		}
	: Readonly<Record<never, never>>;

type Unique<
	Stream extends ScopeStream,
	Model extends keyof Stream["owns"],
	Invocation,
> = Stream["owns"][Model] extends {
	readonly kind: "unique";
	readonly selector: infer Selector extends string;
}
	? {
			readonly [Key in Selector]: Key extends keyof BoundFields<
				Stream,
				Model,
				Invocation
			>
				? BoundFields<Stream, Model, Invocation>[Key]
				: BoundFields<Stream, Model, Invocation>;
		}
	: never;

/** Plain Prisma-compatible values; brands exist only in the TypeScript view. */
export type BoundScope<Stream extends ScopeStream, Invocation> = {
	readonly [Model in keyof Stream["owns"]]: {
		readonly where: BoundFields<Stream, Model, Invocation>;
		readonly fields: BoundFields<Stream, Model, Invocation>;
	} & (Stream["owns"][Model] extends { readonly kind: "unique" }
		? { readonly unique: Unique<Stream, Model, Invocation> }
		: Record<never, never>);
};

export type UniqueScope<
	Stream extends ScopeStream,
	Model extends keyof Stream["owns"],
	Invocation,
> = Stream["owns"][Model] extends { readonly kind: "unique" }
	? Unique<Stream, Model, Invocation>
	: BoundFields<Stream, Model, Invocation>;
