import type { Row, SqlDatabase } from "./database";

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
};
