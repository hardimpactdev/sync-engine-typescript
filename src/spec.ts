import type { Row, SqlDatabase, SqlValue } from "./database.ts";

// What an app tells the store about each synced entity. The store enforces it
// on every write, and the sync functions use it to rebase and remap.

/** A total that a named operation changes by a delta, such as stock. */
export type CounterSpec = {
    /** The column that holds the total. */
    field: string;
    /** The operation argument that holds the delta. */
    delta: string;
    /** The entity of the row that holds the total. Default: the operation's entity. */
    entity?: string;
    /** The argument that holds that row's sync_id. Default: the operation's own row. */
    row?: string;
    /**
     * The argument that holds what the delta is counted in, such as its unit. The entity's
     * `convert` then lands the delta in what the row holds.
     */
    basis?: string;
};

export type OperationSpec = {
    counters?: readonly CounterSpec[];
    /** Arguments that hold another row's sync_id, with the entity they point to. */
    references?: Readonly<Record<string, string>>;
};

export type EntitySpec = {
    /** Fields an upsert may carry. Default: every column that is not a row convention. */
    writable?: readonly string[];
    /** Fields only the server sets. The store refuses to write them. */
    serverOwned?: readonly string[];
    /** Fields that hold another row's sync_id, with the entity they point to. */
    references?: Readonly<Record<string, string>>;
    /** Named operations, such as `adjust`. */
    operations?: Readonly<Record<string, OperationSpec>>;
    /** The scope's sync_id for a row. Default: the row's scope column (`scope_id`). */
    scope?: (row: Row, db: Pick<SqlDatabase, "get" | "all">) => string | null;
    /**
     * Converts a counter delta counted in `basis`, such as grams, to what the row holds now,
     * such as kilograms. The store lands every delta that names a basis through it: a
     * queued operation's, and a provisional result's, also when the row changed its unit
     * on the server or the delta moved to another row.
     */
    convert?: (field: string, delta: SqlValue, basis: string, row: Row) => SqlValue;
};
