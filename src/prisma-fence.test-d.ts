import type { Types } from "@prisma/client/runtime/client";
import { z } from "zod";
import type { Prisma, Post } from "../tests/fixtures/generated/client.js";
import type { BoundScope, FencedPrisma } from "./index.js";
import { createDefinitions } from "./index.js";
import type { author, post, comment, singleton } from "./registry.test-d.js";

type Client = Prisma.TransactionClient;
declare const invocation: unique symbol;
type Invocation = typeof invocation;
declare const db: FencedPrisma<Client, typeof author, Invocation>;
declare const scope: BoundScope<typeof author, Invocation>;
declare const postDb: FencedPrisma<Client, typeof post, Invocation>;
declare const postScope: BoundScope<typeof post, Invocation>;
declare const commentDb: FencedPrisma<Client, typeof comment, Invocation>;
declare const commentScope: BoundScope<typeof comment, Invocation>;
declare const tableDb: FencedPrisma<Client, typeof singleton, Invocation>;
declare const tableScope: BoundScope<typeof singleton, Invocation>;
const commentPartition = createDefinitions<Client>().stream({
	name: "comments-by-post-and-author",
	key: z.strictObject({ postId: z.string(), authorId: z.string() }),
	owns: {
		comment: {
			kind: "partition",
			fields: { postId: "postId", authorId: "authorId" },
		},
	},
});
declare const partitionDb: FencedPrisma<
	Client,
	typeof commentPartition,
	Invocation
>;
declare const partitionScope: BoundScope<typeof commentPartition, Invocation>;

type Equal<Left, Right> =
	(<Value>() => Value extends Left ? 1 : 2) extends <
		Value,
	>() => Value extends Right ? 1 : 2
		? true
		: false;
type Expect<Value extends true> = Value;

const read = {
	where: { ...scope.post.where, title: { startsWith: "Hello" } },
	select: { id: true, title: true },
	orderBy: [{ views: "desc" }, { createdAt: "asc" }],
	distinct: ["title"],
	take: 10,
	skip: 1,
} satisfies Prisma.PostFindManyArgs;
const rows = db.post.findMany(read);
export type Selected = Expect<
	Equal<Awaited<typeof rows>, { id: string; title: string }[]>
>;
const nullable = db.post.findUnique({
	where: { ...scope.post.where, id: "p1" },
	select: { title: true },
});
export type Nullable = Expect<
	Equal<Awaited<typeof nullable>, { title: string } | null>
>;
const required = db.post.findUniqueOrThrow({
	where: { ...scope.post.where, authorId_slug: { authorId: "u1", slug: "a" } },
	select: { title: true },
});
export type Required = Expect<
	Equal<Awaited<typeof required>, { title: string }>
>;
const first = db.post.findFirst({ where: scope.post.where });
export type First = Expect<Equal<Awaited<typeof first>, Post | null>>;
const firstRequired = db.post.findFirstOrThrow({ where: scope.post.where });
export type FirstRequired = Expect<Equal<Awaited<typeof firstRequired>, Post>>;
void db.user.findUnique({ where: scope.user.unique });
void db.user.findMany({ where: scope.user.where });
void postDb.post.findUnique({ where: postScope.post.unique });
void postDb.post.findMany({ where: postScope.post.where });
void commentDb.comment.findUnique({ where: commentScope.comment.unique });
void tableDb.user.findMany({ where: tableScope.user.where });
void partitionDb.comment.findUnique({
	where: {
		...partitionScope.comment.where,
		postId_number: { postId: partitionScope.comment.fields.postId, number: 1 },
	},
});
void partitionDb.comment.createMany({
	data: [{ ...partitionScope.comment.fields, number: 1, body: "text" }],
});

const data = {
	id: "p1",
	...scope.post.fields,
	slug: "hello",
	title: "Hello",
	published: false,
	views: 0,
	createdAt: new Date(),
	tags: ["one"],
	metadata: { author: { comments: ["ordinary JSON"] }, AND: { id: "data" } },
	score: "1.25",
} satisfies Prisma.PostUncheckedCreateInput;
const create = db.post.create({ data, select: { id: true } });
export type Create = Expect<Equal<Awaited<typeof create>, { id: string }>>;
const many = db.post.createMany({ data: [data, { ...data, id: "p2" }] });
void db.post.createMany({ data, skipDuplicates: true });
export type Batch = Expect<Equal<Awaited<typeof many>, { count: number }>>;
void commentDb.comment.create({
	data: { ...commentScope.comment.fields, authorId: "u1", body: "hello" },
});
const updateArgs = {
	where: { ...scope.post.where, id: "p1" },
	data: {
		views: { increment: 1 },
		title: { set: "Updated" },
		tags: { push: "two" },
	},
	select: { id: true, views: true },
} satisfies Prisma.PostUpdateArgs;
const updated = db.post.update(updateArgs);
export type Updated = Expect<
	Equal<Awaited<typeof updated>, { id: string; views: number }>
>;
void db.post.updateMany({
	where: { ...scope.post.where, views: { gt: 5 } },
	data: { views: { multiply: 2 } },
	limit: 5,
});
void db.post.upsert({
	where: { ...scope.post.where, id: "p1" },
	create: data,
	update: { views: { decrement: 1 } },
});
void db.post.delete({ where: { ...scope.post.where, id: "p1" } });
void db.post.deleteMany({ where: scope.post.where });
const count = db.post.count({ where: scope.post.where });
export type Count = Expect<Equal<Awaited<typeof count>, number>>;
const selectedCount = db.post.count({
	where: scope.post.where,
	select: { title: true, _all: true },
});
export type SelectedCount = Expect<
	Equal<Awaited<typeof selectedCount>, { title: number; _all: number }>
>;
const aggregate = db.post.aggregate({
	where: scope.post.where,
	_count: { _all: true },
	_sum: { views: true },
	_avg: { rating: true },
	_min: { createdAt: true },
	_max: { title: true },
});
export type Aggregate = Expect<
	Equal<
		Awaited<typeof aggregate>,
		{
			_count: { _all: number };
			_sum: { views: number | null };
			_avg: { rating: number | null };
			_min: { createdAt: Date | null };
			_max: { title: string | null };
		}
	>
>;
void db.post.findMany({
	where: {
		...scope.post.where,
		AND: [{ OR: [{ title: { contains: "a", mode: "insensitive" } }] }],
		NOT: { views: { lt: 0 } },
		metadata: {
			path: ["author", "comments"],
			equals: { author: "ordinary JSON" },
		},
		createdAt: { gte: new Date() },
		score: { gt: "1.1" },
		tags: { hasSome: ["one"] },
	},
	orderBy: { rating: { sort: "asc", nulls: "last" } },
});

// The fence must remain a subset of Prisma's actual argument contracts.
declare const uniqueInput: Parameters<typeof postDb.post.findUnique>[0];
export const originalUnique: Prisma.PostFindUniqueArgs = uniqueInput;
type OriginalArguments<Op extends keyof typeof db.post> = Types.Public.Args<
	Client["post"],
	Op
>;
export type AllArgumentsNarrow = Expect<
	Equal<
		{
			[Op in keyof typeof db.post]: Parameters<
				(typeof db.post)[Op]
			>[0] extends OriginalArguments<Op>
				? never
				: Op;
		}[keyof typeof db.post],
		never
	>
>;

// @ts-expect-error Optional Prisma args become mandatory scoped args.
void db.post.findMany();
// @ts-expect-error No implicit partition injection.
void db.post.findMany({});
// @ts-expect-error A bound key only under OR is not a top-level predicate.
void db.post.findMany({ where: { OR: [scope.post.where] } });
// @ts-expect-error Nor is a bound key under NOT.
void db.post.count({ where: { NOT: scope.post.where } });
// @ts-expect-error Aggregate calls also require explicit scope.
void db.post.aggregate({ _count: true });
// @ts-expect-error Arbitrary event strings are not capabilities.
void db.post.findMany({ where: { authorId: "u1" } });
// @ts-expect-error Scalar brands cannot be preserved by an overriding spread.
void db.post.findMany({ where: { ...scope.post.where, authorId: "other" } });
// @ts-expect-error Same primitive type, different model/column role.
void db.post.findMany({ where: { authorId: scope.user.fields.id } });
// @ts-expect-error A unique selector is still required for partition single-row operations.
void db.post.update({ where: scope.post.where, data: { title: "new" } });
// @ts-expect-error Flat compound fields are not a Prisma unique selector.
void postDb.post.findUnique({ where: postScope.post.where });
// @ts-expect-error Compound unique selectors are not flat model filters.
void postDb.post.findMany({ where: postScope.post.unique });
void postDb.post.findUnique({
	where: {
		authorId_slug: {
			// @ts-expect-error Same-typed compound components cannot swap roles.
			authorId: postScope.post.fields.slug,
			// @ts-expect-error The other component is independently branded too.
			slug: postScope.post.fields.authorId,
		},
	},
});
void commentDb.comment.findUnique({
	// @ts-expect-error The compound primary key requires its numeric component.
	where: { postId_number: { postId: commentScope.comment.fields.postId } },
});
void db.post.update({
	where: { ...scope.post.where, id: "p1" },
	// @ts-expect-error Ownership fields cannot be reassigned, even to the bound value.
	data: { authorId: scope.post.fields.authorId },
});
void db.post.updateMany({
	where: scope.post.where,
	// @ts-expect-error Operator objects cannot reassign ownership.
	data: { authorId: { set: "other" } },
});
void db.post.upsert({
	where: { ...scope.post.where, id: "p1" },
	create: data,
	// @ts-expect-error upsert.update preserves ownership.
	update: { authorId: "other" },
});
void db.post.upsert({
	where: { ...scope.post.where, id: "p1" },
	// @ts-expect-error upsert.create requires explicitly bound ownership.
	create: { ...data, authorId: "other" },
	update: {},
});
// @ts-expect-error Every createMany row needs the bound fields.
void db.post.createMany({ data: [data, { ...data, authorId: "other" }] });
// @ts-expect-error Original required scalar fields remain required.
void db.post.create({ data: { ...scope.post.fields, id: "p1" } });
void db.post.update({
	where: { ...scope.post.where, id: "p1" },
	// @ts-expect-error Scalar inputs are not widened.
	data: { views: "many" },
});
// @ts-expect-error Missing original upsert branch.
void db.post.upsert({ where: { ...scope.post.where, id: "p1" }, create: data });
// @ts-expect-error No undeclared delegate.
void db.comment;
// @ts-expect-error No raw SQL.
void db.$queryRaw;
// @ts-expect-error No transaction management.
void db.$transaction;
// @ts-expect-error No lifecycle methods.
void db.$disconnect;
// @ts-expect-error No extension methods.
void db.$extends;
// @ts-expect-error groupBy has no contract yet.
void db.post.groupBy;
// @ts-expect-error Return-many writes have no contract yet.
void db.post.createManyAndReturn;
// @ts-expect-error Return-many writes have no contract yet.
void db.post.updateManyAndReturn;
// @ts-expect-error Fluent relation methods are not in the Promise view.
void db.user.findUnique({ where: scope.user.unique }).posts;
// @ts-expect-error Returned row IDs do not mint bound capabilities.
void db.post.findMany({ where: { authorId: (await create).id } });
// @ts-expect-error Readonly scope metadata.
scope.post.fields.authorId = data.authorId;
// @ts-expect-error Readonly nested unique components.
postScope.post.unique.authorId_slug.slug = postScope.post.fields.slug;

const include = { where: scope.post.where, include: { author: true } };
// @ts-expect-error Visible forbidden properties in variables are rejected.
void db.post.findMany(include);
const selection = {
	where: scope.post.where,
	select: { id: true, author: true },
};
// @ts-expect-error Even relations to another owned model are excluded.
void db.post.findMany(selection);
const relationCount = {
	where: scope.post.where,
	select: { id: true, _count: { select: { comments: true } } },
};
// @ts-expect-error Relation counts are relation access.
void db.post.findMany(relationCount);
const relationFilter = {
	where: {
		...scope.post.where,
		AND: [{ NOT: { OR: [{ author: { name: "N" } }] } }],
	},
};
// @ts-expect-error Recursively forbidden model relation filters, including variables.
void db.post.findMany(relationFilter);
const relationOrder = {
	where: scope.post.where,
	orderBy: { author: { name: "asc" }, title: "asc" },
} as const;
// @ts-expect-error Relations cannot be used for ordering.
void db.post.findMany(relationOrder);
const nestedWrite = {
	data: {
		...data,
		comments: { create: { number: 1, authorId: "u1", body: "text" } },
	},
};
// @ts-expect-error Nested writes remain forbidden in unchecked scalar data.
void db.post.create(nestedWrite);
const cursor = { where: scope.post.where, cursor: { id: "outside" } };
// @ts-expect-error Cursors are explicitly unsupported in v1.
void db.post.findMany(cursor);
const unknownArgument = { where: scope.post.where, somethingNew: true };
// @ts-expect-error Unknown top-level arguments in variables cannot slip through inference.
void db.post.findMany(unknownArgument);
const omitted = { where: scope.post.where, omit: { title: true } };
// @ts-expect-error New argument surfaces must be deliberately supported.
void db.post.findMany(omitted);

declare const otherInvocation: unique symbol;
declare const otherScope: BoundScope<typeof author, typeof otherInvocation>;
// @ts-expect-error Scope values from a different invocation cannot be reused.
void db.post.findMany({ where: otherScope.post.where });
declare const erasedScope: BoundScope<typeof author, unknown>;
// @ts-expect-error An erased invocation cannot be specialized again.
void db.post.findMany({ where: erasedScope.post.where });
// @ts-expect-error Invariance also prevents erasing a concrete invocation.
export const erased: BoundScope<typeof author, unknown> = scope;
// @ts-expect-error Different stream-family role, even for the same model and field.
void db.post.findMany({ where: postScope.post.where });

export type AnyClientFailsClosed = Expect<
	Equal<
		FencedPrisma<ReturnType<typeof JSON.parse>, typeof author, Invocation>,
		never
	>
>;

const countRelations = {
	where: scope.post.where,
	select: { _all: true, comments: true },
};
// @ts-expect-error Count selections cannot silently accept visible relation fields.
void db.post.count(countRelations);
const aggregateRelations = {
	where: scope.post.where,
	_count: { _all: true, comments: true },
};
// @ts-expect-error Aggregate selections cannot silently accept visible relation fields.
void db.post.aggregate(aggregateRelations);
const recursiveList = {
	where: {
		...scope.post.where,
		OR: [{ title: "hello" }, { comments: { some: { body: "text" } } }],
	},
};
// @ts-expect-error Relation filters are forbidden in mixed boolean branches.
void db.post.findMany(recursiveList);
const nestedUpdate = {
	where: { ...scope.post.where, id: "p1" },
	data: { title: "new", comments: { deleteMany: {} } },
};
// @ts-expect-error Nested writes are excluded from update as well as create.
void db.post.update(nestedUpdate);
// @ts-expect-error Prisma's own unique selector cannot be present with an undefined value.
void db.post.findUnique({ where: { ...scope.post.where, id: undefined } });
// @ts-expect-error All ordinary reads require a scope, even singleton reads.
void tableDb.user.findFirst({});
// @ts-expect-error All counts require a scope.
void db.post.count();
// @ts-expect-error Bulk deletes require a scope.
void db.post.deleteMany({});
// @ts-expect-error Table fields are not a capability for a partition.
void db.post.findMany({ where: tableScope.user.fields });
void commentDb.comment.update({
	where: commentScope.comment.unique,
	// @ts-expect-error Mutable ownership operators are excluded in compound scopes too.
	data: { number: { increment: 1 } },
});
// @ts-expect-error A unique-owner single row operation must use its declared selector.
void postDb.post.delete({ where: { id: "p1", ...postScope.post.where } });
void db.post.findMany({
	// @ts-expect-error An ownership field filter is not a bound scalar.
	where: { authorId: { equals: scope.post.fields.authorId } },
});
// @ts-expect-error The scalar unique selector must also use its bound component.
void db.user.findUnique({ where: { id: "u1" } });
void partitionDb.comment.findMany({
	// @ts-expect-error Composite partitions must bind every mapped dimension.
	where: { postId: partitionScope.comment.fields.postId },
});

export function independentInvocations<Current, Previous>(
	current: FencedPrisma<Client, typeof author, Current>,
	previous: BoundScope<typeof author, Previous>,
) {
	// @ts-expect-error Unrelated generic invocation tokens cannot be unified.
	void current.post.findMany({ where: previous.post.where });
}
