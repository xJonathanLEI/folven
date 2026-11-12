import { z } from "zod";
import { DefinitionError } from "./errors.js";
import { copyOwnership, validateName, validateOwnership } from "./ownership.js";
import type {
	ImmutableOwnership,
	OwnershipMap,
	ValidateOwnership,
} from "./ownership.js";
import type { IsKeyComponent } from "./prisma-types.js";

const clientType = Symbol("streamDefinition");

export interface StreamDefinition<
	Client,
	Name extends string = string,
	Key extends z.ZodObject = z.ZodObject,
	Owns extends OwnershipMap = OwnershipMap,
> {
	readonly name: Name;
	readonly key: Key;
	readonly owns: ImmutableOwnership<Owns>;
	/** Type-only invariant client association; no client is retained at runtime. */
	readonly [clientType]: ((client: Client) => Client) | undefined;
}

export type StreamKey<Stream extends { readonly key: z.ZodObject }> = z.output<
	Stream["key"]
>;
export type StreamKeyInput<Stream extends { readonly key: z.ZodObject }> =
	z.input<Stream["key"]>;

type ValidateKey<Key extends z.ZodObject> = {
	[Field in keyof Key["shape"]]: IsKeyComponent<
		z.output<Key["shape"][Field]>
	> extends true
		? never
		: Field;
}[keyof Key["shape"]] extends never
	? unknown
	: never;

/** Check the parsed domain without replacing or transforming the caller's schema. */
export function parseStreamKey<
	Stream extends { readonly name: string; readonly key: z.ZodObject },
>(stream: Stream, input: unknown): StreamKey<Stream> {
	const parsed = stream.key.parse(input);
	const fields = Object.keys(stream.key.shape);
	if (
		Object.keys(parsed).length !== fields.length ||
		fields.some((field) => {
			const value: unknown = parsed[field];
			return !(
				(typeof value === "string" && value.length > 0) ||
				(typeof value === "number" &&
					Number.isSafeInteger(value) &&
					!Object.is(value, -0))
			);
		})
	) {
		throw new DefinitionError(
			`Stream "${stream.name}" keys must contain only nonempty strings or safe integers (excluding negative zero).`,
		);
	}
	// ZodObject's broad signature erases its shape here; parsing retains the exact output.
	return parsed as StreamKey<Stream>;
}

export function validateStream(
	stream: {
		readonly name: string;
		readonly key: z.ZodObject;
		readonly owns: OwnershipMap;
	},
	requireCompleteKey = true,
) {
	validateName(stream.name, "Stream name");
	if (
		!(stream.key instanceof z.ZodObject) ||
		(stream.key.def.catchall !== undefined &&
			!(stream.key.def.catchall instanceof z.ZodNever))
	) {
		throw new DefinitionError(
			`Stream "${stream.name}" requires a fixed object key schema.`,
		);
	}
	for (const field of Object.keys(stream.key.shape)) {
		validateName(field, `Stream "${stream.name}" key component`);
	}
	validateOwnership(
		stream.name,
		stream.owns,
		Object.keys(stream.key.shape),
		requireCompleteKey,
	);
}

export function createStream<
	Client,
	const Name extends string,
	Key extends z.ZodObject,
	const Owns extends OwnershipMap,
>(input: {
	readonly name: Name;
	readonly key: Key & ValidateKey<NoInfer<Key>>;
	readonly owns: Owns &
		ValidateOwnership<Client, z.output<NoInfer<Key>>, NoInfer<Owns>>;
}): StreamDefinition<Client, Name, Key, Owns> {
	// Shape and coverage validation also runs at registry construction.
	validateStream(input, false);
	return Object.freeze({
		name: input.name,
		key: input.key,
		owns: copyOwnership(input.owns),
		[clientType]: undefined,
	});
}

export type StreamDeclaration<
	Client,
	Name extends string,
	Key extends z.ZodObject,
	Owns extends OwnershipMap,
> = Parameters<typeof createStream<Client, Name, Key, Owns>>[0];
