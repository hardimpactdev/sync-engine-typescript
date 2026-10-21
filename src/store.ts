import type { Row, SqlDatabase, SqlValue } from "./database.ts";
import { addValues, negate } from "./decimal.ts";
import type { CounterSpec, EntitySpec } from "./spec.ts";

// Every write to synced data goes through the store. In one transaction it
// changes the row and writes or merges its outbox entry (protocol §6). After
// commit it tells the screens that read those tables. A listener that throws
// cannot undo a commit, so its error goes to onListenerError, and the write and
// the other listeners go on.

/** Engine tables a screen may subscribe to, such as a count of unsent changes. */
export type EngineTable = "sync_outbox" | "sync_rejected" | "sync_state";

export type StoreOptions<T extends string> = {
    /** The synced tables. Each one is also the entity name on the wire. */
    tables: readonly T[];
    entities?: Partial<Record<T, EntitySpec>>;
    /** The column of a synced row that holds its scope's sync_id. Default `scope_id`. */
    scopeColumn?: string;
    newId: () => string;
    now?: () => string;
    /** Gets an error a listener threw after a commit. By default it is thrown again in a microtask. */
    onListenerError?: (error: unknown) => void;
};

export type WriteOptions = {
    /** Local file ids that must upload before this change is sent. */
    dependsOn?: readonly string[];
    /**
     * Writes the local, provisional result of an action that the server runs
     * itself, such as the stock a cook uses. These writes never reach the
     * outbox. The store keeps them on top of pulled rows until the server's
     * own result has arrived, then removes them (see the README).
     */
    provisional?: (local: ProvisionalWriter) => void;
    /**
     * Sends the upsert as an account merge (protocol §5.4): the server lands it on a
     * matching row and fills in only what that row lacks. The flag stays with this
     * change through every resend. Later writes to the row queue as normal changes;
     * they never merge into it. Needs `syncEngineSchemaV2`.
     */
    merge?: boolean;
};

/** Writes the provisional result of a change. Needs `syncEngineSchemaV2`. */
export type ProvisionalWriter = {
    get<R>(sql: string, params?: SqlValue[]): R | null;
    all<R>(sql: string, params?: SqlValue[]): R[];
    /**
     * Adds a row that only this device has, and returns its sync_id. Only other provisional
     * rows may point at it; they follow it when it goes, as through a foreign key.
     */
    insert(table: string, values: Row): string;
    /** Adds a delta to a counter: a field that an operation's counters change, such as stock. */
    add(table: string, syncId: string, field: string, delta: SqlValue): void;
};

/** Writes rows for an operation's local effect. These writes get no outbox entry of their own. */
export type LocalWriter = {
    get<R>(sql: string, params?: SqlValue[]): R | null;
    all<R>(sql: string, params?: SqlValue[]): R[];
    /** Inserts the row, or changes the given columns of it. */
    put(table: string, syncId: string, values: Row): void;
};

export interface LocalStore<T extends string = string> {
    all<R>(sql: string, params?: SqlValue[]): R[];
    get<R>(sql: string, params?: SqlValue[]): R | null;
    newId(): string;
    transaction<R>(work: () => R): R;
    /** Adds a row and returns its sync_id. */
    insert(table: T, values: Row, options?: WriteOptions): string;
    update(table: T, syncId: string, patch: Row, options?: WriteOptions): void;
    /** Deletes a row. A row the server knows stays hidden until the delete is acked. */
    remove(table: T, syncId: string): void;
    /**
     * Records a named operation, such as `adjust {delta}`. The store first runs
     * `applyLocally`, then adds the deltas of the spec's counters, then queues
     * the operation. Operations never merge.
     */
    operate(
        entity: T,
        rowId: string,
        op: string,
        args: Row,
        applyLocally?: (local: LocalWriter) => void,
        options?: WriteOptions,
    ): void;
    /** Whether a row is provisional: made only here, until the server's result replaces it. */
    isProvisional(table: T, syncId: string): boolean;
    subscribe(tables: readonly (T | EngineTable)[], listener: () => void): () => void;
    /** Changes whenever one of the tables changes, for useSyncExternalStore. */
    version(tables: readonly (T | EngineTable)[]): string;
}

export type OutboxEntry = {
    position: number;
    change_id: string;
    scope_id: string;
    entity: string;
    row_id: string;
    op: string;
    payload: string;
    base_seq: number | null;
    depends_on: string | null;
    state: "pending" | "sending";
    attempts: number;
    created_at: string;
    /** 1 for an account merge (syncEngineSchemaV2); absent before it. */
    merge?: number;
};

export class MissingRowError extends Error {
    constructor(table: string, syncId: string) {
        super(`There is no ${table} row ${syncId}, or it was deleted.`);
        this.name = "MissingRowError";
    }
}

/** A write that would change, or point at, a row the server has not made yet. */
export class ProvisionalRowError extends Error {
    constructor(table: string, syncId: string) {
        super(`The ${table} row ${syncId} is provisional. It waits for the server's own row.`);
        this.name = "ProvisionalRowError";
    }
}

/** One provisional write: a row made only here (field null), or a delta on a field. */
type ProvisionalEffect = {
    id: number;
    change_id: string;
    scope_id: string;
    entity: string;
    row_id: string;
    field: string | null;
    delta: SqlValue;
    acked: number;
};

/** Columns every synced row has, with the scope column. The store and sync set them; a caller never does. */
const rowConventions = [
    "sync_id",
    "server_id",
    "server_seq",
    "created_at",
    "updated_at",
    "deleted_at",
];
const plainName = /^[a-z][a-z0-9_]*$/;

type Spec = {
    name: string;
    writable: Set<string> | null;
    serverOwned: Set<string>;
    references: Map<string, string>;
    operations: Map<string, { counters: readonly CounterSpec[]; references: Map<string, string> }>;
    counterFields: Set<string>;
    scope: NonNullable<EntitySpec["scope"]>;
};

/** A counter of one queued operation that lands on a row. */
type CounterHit = { entry: OutboxEntry; entity: string; row: string; field: string; delta: SqlValue };

/** The store's inside, for the sync functions in sync.ts. */
export type Engine = ReturnType<typeof createEngine>;

const engines = new WeakMap<object, Engine>();

export function engineOf(store: LocalStore<string>): Engine {
    const engine = engines.get(store);
    if (!engine) throw new Error("This is not a store made by createLocalStore.");
    return engine;
}

export function createLocalStore<T extends string>(
    db: SqlDatabase,
    options: StoreOptions<T>,
): LocalStore<T> {
    const engine = createEngine(db, options);
    const { transaction, touch, now, spec } = engine;

    const store: LocalStore<T> = {
        all: (sql, params = []) => db.all(sql, params),
        get: (sql, params = []) => db.get(sql, params),
        newId: options.newId,
        transaction,

        insert(table, values, { dependsOn, provisional, merge } = {}) {
            const entity = spec(table);
            const synced = engine.checkWrite(entity, values, "insert");
            const syncId = options.newId();
            const stamp = now();
            const names = ["sync_id", ...Object.keys(values), "created_at", "updated_at"];
            transaction(() => {
                engine.guardReferences(entity.references, synced);
                db.run(
                    `INSERT INTO ${table} (${names.join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
                    [syncId, ...Object.values(values), stamp, stamp],
                );
                const row = engine.row(table, syncId)!;
                // After the write, so a resolver that reads the stored row sees it.
                engine.guardScope(entity.scope(row, db));
                const entry = engine.append(entity, row, "upsert", { ...synced }, dependsOn, merge);
                if (provisional) engine.writeProvisional(entry, provisional);
                touch(table);
            });
            return syncId;
        },

        update(table, syncId, patch, { dependsOn, provisional, merge } = {}) {
            const entity = spec(table);
            const synced = engine.checkWrite(entity, patch, "update");
            const columns = Object.keys(patch);
            if (provisional && Object.keys(synced).length === 0) {
                throw new Error("A provisional result needs a change that the server gets.");
            }
            if (columns.length === 0) return;
            transaction(() => {
                engine.guardRow(table, syncId);
                engine.guardReferences(entity.references, synced);
                const { changes } = db.run(
                    `UPDATE ${table} SET ${columns.map((column) => `${column} = ?`).join(", ")}, updated_at = ? WHERE sync_id = ? AND deleted_at IS NULL`,
                    [...Object.values(patch), now(), syncId],
                );
                if (changes === 0) throw new MissingRowError(table, syncId);
                engine.guardScope(entity.scope(engine.row(table, syncId)!, db));
                if (Object.keys(synced).length > 0) {
                    const entry = merge
                        ? engine.append(entity, engine.row(table, syncId)!, "upsert", synced, dependsOn, true)
                        : engine.queueUpsert(entity, engine.row(table, syncId)!, synced, dependsOn);
                    if (provisional) engine.writeProvisional(entry, provisional);
                }
                touch(table);
            });
        },

        remove(table, syncId) {
            const entity = spec(table);
            transaction(() => {
                const row = engine.row(table, syncId);
                if (!row || row.deleted_at !== null) throw new MissingRowError(table, syncId);
                engine.guardRow(table, syncId);
                const entries = engine.entriesFor(table, syncId);
                if (!engine.knownToServer(row, entries)) {
                    // An unsent insert and its delete cancel out.
                    engine.cancel(table, syncId);
                    return;
                }
                engine.drop(entries.filter((entry) => engine.unsent(entry)));
                engine.append(entity, row, "delete", {});
                const stamp = now();
                db.run(`UPDATE ${table} SET deleted_at = ?, updated_at = ? WHERE sync_id = ?`, [
                    stamp,
                    stamp,
                    syncId,
                ]);
                touch(table);
            });
        },

        operate(table, rowId, op, args, applyLocally, { dependsOn, provisional } = {}) {
            const entity = spec(table);
            const operation = entity.operations.get(op);
            if (!operation) throw new Error(`${table} has no operation ${op}.`);
            transaction(() => {
                engine.guardReferences(operation.references, args);
                applyLocally?.(engine.writer);
                const row = engine.row(table, rowId);
                if (!row || row.deleted_at !== null) throw new MissingRowError(table, rowId);
                engine.guardRow(table, rowId);
                const entry = engine.append(entity, row, op, args, dependsOn);
                engine.shiftCounters(entry, 1);
                if (provisional) engine.writeProvisional(entry, provisional);
                touch(table);
            });
        },

        isProvisional: (table, syncId) => engine.isProvisionalRow(table, syncId),

        subscribe(tables, listener) {
            for (const table of tables) {
                if (!engine.listeners.has(table)) engine.listeners.set(table, new Set());
                engine.listeners.get(table)!.add(listener);
            }
            return () => {
                for (const table of tables) engine.listeners.get(table)?.delete(listener);
            };
        },

        version: (tables) => tables.map((table) => engine.versions.get(table) ?? 0).join(":"),
    };

    engines.set(store, engine);
    return store;
}

function createEngine<T extends string>(
    db: SqlDatabase,
    {
        tables,
        entities = {},
        scopeColumn = "scope_id",
        newId,
        now = () => new Date().toISOString(),
        onListenerError = rethrowLater,
    }: StoreOptions<T>,
) {
    if (!plainName.test(scopeColumn)) throw new Error(`${scopeColumn} is not a column name.`);
    const conventions = new Set([...rowConventions, scopeColumn]);
    let depth = 0;
    const touched = new Set<string>();
    const versions = new Map<string, number>();
    const listeners = new Map<string, Set<() => void>>();
    const columnCache = new Map<string, Set<string>>();
    const nullableCache = new Map<string, Set<string>>();

    const specs = resolveSpecs(tables, entities as Partial<Record<string, EntitySpec>>, scopeColumn);
    // For each entity, the queued operations whose counters land on its rows.
    const countersOn = new Map<string, { source: string; op: string; counter: CounterSpec }[]>();
    for (const source of specs.values()) {
        for (const [op, operation] of source.operations) {
            for (const counter of operation.counters) {
                const target = counter.entity ?? source.name;
                if (!countersOn.has(target)) countersOn.set(target, []);
                countersOn.get(target)!.push({ source: source.name, op, counter });
            }
        }
    }

    function transaction<R>(work: () => R): R {
        const outer = depth === 0;
        const savepoint = `level_${depth}`;
        db.exec(outer ? "BEGIN IMMEDIATE" : `SAVEPOINT ${savepoint}`);
        depth++;
        let result: R;
        try {
            result = work();
            // COMMIT can fail too, such as on a deferred foreign key, so it stays inside the try.
            db.exec(outer ? "COMMIT" : `RELEASE ${savepoint}`);
        } catch (failure) {
            if (outer) {
                db.exec("ROLLBACK");
                touched.clear();
            } else {
                db.exec(`ROLLBACK TO ${savepoint}`);
                db.exec(`RELEASE ${savepoint}`);
            }
            throw failure;
        } finally {
            depth--;
        }
        if (outer) notify();
        return result;
    }

    function notify() {
        const changed = [...touched];
        touched.clear();
        for (const table of changed) versions.set(table, (versions.get(table) ?? 0) + 1);
        const called = new Set<() => void>();
        for (const table of changed) {
            for (const listener of listeners.get(table) ?? []) {
                if (called.has(listener)) continue;
                called.add(listener);
                try {
                    listener();
                } catch (error) {
                    onListenerError(error);
                }
            }
        }
    }

    function touch(table: string) {
        touched.add(table);
    }

    function spec(table: string): Spec {
        const found = specs.get(table);
        if (!found) throw new Error(`${table} is not a synced table.`);
        return found;
    }

    function columns(table: string): Set<string> {
        let found = columnCache.get(table);
        if (!found) {
            found = new Set(
                db.all<{ name: string }>(`PRAGMA table_info(${table})`).map((column) => column.name),
            );
            columnCache.set(table, found);
        }
        return found;
    }

    function row(table: string, syncId: string): Row | null {
        return db.get<Row>(`SELECT * FROM ${table} WHERE sync_id = ?`, [syncId]);
    }

    function deleteRow(table: string, syncId: string) {
        db.run(`DELETE FROM ${table} WHERE sync_id = ?`, [syncId]);
        touch(table);
    }

    /** Checks the columns of a write and returns the fields that go to the server. */
    function checkWrite(entity: Spec, values: Row, mode: "insert" | "update"): Row {
        const synced: Row = {};
        for (const [column, value] of Object.entries(values)) {
            if (!plainName.test(column)) throw new Error(`${column} is not a column name.`);
            if (column === scopeColumn && mode === "insert") continue;
            if (conventions.has(column)) throw new Error(`The store sets ${column} itself.`);
            if (column.startsWith("local_")) continue;
            if (entity.serverOwned.has(column)) {
                throw new Error(`The server sets ${entity.name}.${column}.`);
            }
            if (entity.counterFields.has(column)) {
                throw new Error(
                    `${entity.name}.${column} is a counter. Change it through an operation.`,
                );
            }
            if (entity.writable && !entity.writable.has(column)) {
                throw new Error(`${entity.name}.${column} is not a writable field.`);
            }
            synced[column] = value;
        }
        return synced;
    }

    function scopeOf(entity: Spec, values: Row): string {
        const scope = entity.scope(values, db);
        if (!scope) {
            throw new Error(`The ${entity.name} row ${values.sync_id} has no scope.`);
        }
        return scope;
    }

    function entriesFor(entity: string, rowId: string): OutboxEntry[] {
        return db.all<OutboxEntry>(
            "SELECT * FROM sync_outbox WHERE entity = ? AND row_id = ? ORDER BY position",
            [entity, rowId],
        );
    }

    /** True while the server may have seen the entry, so its payload must not change. */
    function unsent(entry: OutboxEntry): boolean {
        return entry.state === "pending" && entry.attempts === 0;
    }

    function knownToServer(values: Row, entries: OutboxEntry[]): boolean {
        return values.server_seq !== null || entries.some((entry) => !unsent(entry));
    }

    function append(
        entity: Spec,
        values: Row,
        op: string,
        payload: Row,
        dependsOn?: readonly string[],
        merge = false,
    ): OutboxEntry {
        const changeId = newId();
        if (merge && !columns("sync_outbox").has("merge")) {
            throw new Error("A merge change needs syncEngineSchemaV2 in the app's migrations.");
        }
        db.run(
            `INSERT INTO sync_outbox (change_id, scope_id, entity, row_id, op, payload, base_seq, depends_on, created_at${merge ? ", merge" : ""}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?${merge ? ", 1" : ""})`,
            [
                changeId,
                scopeOf(entity, values),
                entity.name,
                String(values.sync_id),
                op,
                JSON.stringify(payload),
                (values.server_seq as number | null | undefined) ?? null,
                dependsOn ? JSON.stringify([...dependsOn]) : null,
                now(),
            ],
        );
        touch("sync_outbox");
        return db.get<OutboxEntry>("SELECT * FROM sync_outbox WHERE change_id = ?", [changeId])!;
    }

    /**
     * Merges an upsert into the row's last upsert while that one is unsent.
     * Later values win. A merge moves values earlier in the send order, so it
     * does not happen when a new reference points at a row queued after it.
     */
    function queueUpsert(
        entity: Spec,
        values: Row,
        fields: Row,
        dependsOn?: readonly string[],
    ): OutboxEntry {
        const entries = entriesFor(entity.name, String(values.sync_id));
        const last = entries.findLast((entry) => entry.op === "upsert");
        const mergeable =
            last !== undefined &&
            unsent(last) &&
            !last.merge &&
            !entries.some((entry) => entry.position > last.position && entry.op === "delete") &&
            !referencesQueuedAfter(entity, fields, last.position);
        if (!mergeable) return append(entity, values, "upsert", fields, dependsOn);
        const files = new Set<string>([...parseList(last.depends_on), ...(dependsOn ?? [])]);
        db.run("UPDATE sync_outbox SET payload = ?, depends_on = ? WHERE position = ?", [
            JSON.stringify({ ...JSON.parse(last.payload), ...fields }),
            last.depends_on === null && !dependsOn ? null : JSON.stringify([...files]),
            last.position,
        ]);
        touch("sync_outbox");
        return last;
    }

    function referencesQueuedAfter(entity: Spec, fields: Row, position: number): boolean {
        for (const [field, target] of entity.references) {
            const value = fields[field];
            if (typeof value !== "string") continue;
            const later = db.get(
                "SELECT 1 FROM sync_outbox WHERE entity = ? AND row_id = ? AND position > ?",
                [target, value, position],
            );
            if (later) return true;
        }
        return false;
    }

    /** The rows a queued change points at, as `entity:sync_id`. */
    function referencesOf(entry: OutboxEntry): string[] {
        const entity = spec(entry.entity);
        const payload = JSON.parse(entry.payload) as Row;
        const references =
            entry.op === "upsert"
                ? entity.references
                : (entity.operations.get(entry.op)?.references ?? new Map<string, string>());
        const keys: string[] = [];
        for (const [field, target] of references) {
            const value = payload[field];
            if (typeof value === "string") keys.push(`${target}:${value}`);
        }
        return keys;
    }

    /** The counters of a queued operation, with the row each one lands on. */
    function countersOf(entry: OutboxEntry): CounterHit[] {
        const operation = specs.get(entry.entity)?.operations.get(entry.op);
        if (!operation) return [];
        const args = JSON.parse(entry.payload) as Row;
        const hits: CounterHit[] = [];
        for (const counter of operation.counters) {
            const target = counter.row ? args[counter.row] : entry.row_id;
            if (typeof target !== "string") continue;
            hits.push({
                entry,
                entity: counter.entity ?? entry.entity,
                row: target,
                field: counter.field,
                delta: args[counter.delta] ?? null,
            });
        }
        return hits;
    }

    /** The queued operations whose counters land on one row. */
    function countersLandingOn(entity: string, rowId: string): CounterHit[] {
        const hits: CounterHit[] = [];
        for (const { source, op, counter } of countersOn.get(entity) ?? []) {
            const entries = counter.row
                ? db.all<OutboxEntry>(
                      "SELECT * FROM sync_outbox WHERE entity = ? AND op = ? AND json_extract(payload, ?) = ? ORDER BY position",
                      [source, op, `$.${counter.row}`, rowId],
                  )
                : db.all<OutboxEntry>(
                      "SELECT * FROM sync_outbox WHERE entity = ? AND op = ? AND row_id = ? ORDER BY position",
                      [source, op, rowId],
                  );
            for (const entry of entries) {
                const args = JSON.parse(entry.payload) as Row;
                hits.push({
                    entry,
                    entity,
                    row: rowId,
                    field: counter.field,
                    delta: args[counter.delta] ?? null,
                });
            }
        }
        return hits;
    }

    /** Adds (1) or takes back (-1) an operation's deltas, except on one row. */
    function shiftCounters(
        entry: OutboxEntry,
        sign: 1 | -1,
        except?: { entity: string; row: string },
    ) {
        for (const hit of countersOf(entry)) {
            if (except && hit.entity === except.entity && hit.row === except.row) continue;
            if (!columns(hit.entity).has(hit.field)) continue;
            const current = db.get<Row>(`SELECT ${hit.field} FROM ${hit.entity} WHERE sync_id = ?`, [
                hit.row,
            ]);
            if (!current) continue;
            db.run(`UPDATE ${hit.entity} SET ${hit.field} = ? WHERE sync_id = ?`, [
                addValues(current[hit.field], sign === 1 ? hit.delta : negate(hit.delta)),
                hit.row,
            ]);
            touch(hit.entity);
        }
    }

    /** Removes entries from the outbox and takes back their deltas and provisional results. */
    function drop(entries: OutboxEntry[]) {
        for (const entry of entries) {
            shiftCounters(entry, -1);
            settleProvisional(entry.change_id);
            db.run("DELETE FROM sync_outbox WHERE position = ?", [entry.position]);
        }
        if (entries.length > 0) touch("sync_outbox");
    }

    // Provisional results (syncEngineSchemaV2). A change that the server turns into
    // more rows, such as a cook that uses stock, can carry the result this device
    // expects. Those writes have no outbox entry. Pulled rows get the provisional
    // deltas on top, as they get queued counters. The result goes when the server
    // has answered: at once for a rejected or dropped change, and for an applied
    // change at the end of the next drained pull, which holds the server's rows.

    let provisionalReady: boolean | null = null;
    function hasProvisional(): boolean {
        provisionalReady ??=
            db.get("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_provisional'") !== null;
        return provisionalReady;
    }

    function writeProvisional(entry: OutboxEntry, work: (local: ProvisionalWriter) => void) {
        if (!hasProvisional()) {
            throw new Error("Provisional results need syncEngineSchemaV2 in the app's migrations.");
        }
        const record = (entity: string, rowId: string, field: string | null, delta: SqlValue) =>
            db.run(
                "INSERT INTO sync_provisional (change_id, scope_id, entity, row_id, field, delta) VALUES (?, ?, ?, ?, ?, ?)",
                [entry.change_id, entry.scope_id, entity, rowId, field, delta],
            );
        work({
            get: (sql, params = []) => db.get(sql, params),
            all: (sql, params = []) => db.all(sql, params),
            insert(table, values) {
                spec(table);
                for (const column of Object.keys(values)) {
                    if (!plainName.test(column)) throw new Error(`${column} is not a column name.`);
                    if (conventions.has(column) && column !== scopeColumn) {
                        throw new Error(`The store sets ${column} itself.`);
                    }
                }
                const syncId = newId();
                const stamp = now();
                const names = ["sync_id", ...Object.keys(values), "created_at", "updated_at"];
                db.run(
                    `INSERT INTO ${table} (${names.join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
                    [syncId, ...Object.values(values), stamp, stamp],
                );
                record(table, syncId, null, null);
                touch(table);
                return syncId;
            },
            add(table, syncId, field, delta) {
                spec(table);
                // Only a counter: no write may set it, so the delta can always be taken back.
                if (!spec(table).counterFields.has(field)) {
                    throw new Error(`${table}.${field} is not a counter of an operation.`);
                }
                const current = row(table, syncId);
                if (!current || current.deleted_at !== null) throw new MissingRowError(table, syncId);
                guardRow(table, syncId);
                db.run(`UPDATE ${table} SET ${field} = ? WHERE sync_id = ?`, [
                    addValues(current[field], delta),
                    syncId,
                ]);
                record(table, syncId, field, delta);
                touch(table);
            },
        });
    }

    function provisionalOn(entity: string, rowId: string): ProvisionalEffect[] {
        if (!hasProvisional()) return [];
        return db.all<ProvisionalEffect>(
            "SELECT * FROM sync_provisional WHERE entity = ? AND row_id = ? AND field IS NOT NULL ORDER BY id",
            [entity, rowId],
        );
    }

    function isProvisionalRow(entity: string, rowId: string): boolean {
        if (!hasProvisional()) return false;
        return (
            db.get(
                "SELECT 1 FROM sync_provisional WHERE entity = ? AND row_id = ? AND field IS NULL",
                [entity, rowId],
            ) !== null
        );
    }

    /** Refuses a write to a provisional row: the server's own row replaces it. */
    function guardRow(entity: string, rowId: string) {
        if (isProvisionalRow(entity, rowId)) throw new ProvisionalRowError(entity, rowId);
    }

    /** Refuses a reference to a provisional row: the server would not know it. */
    function guardReferences(references: Map<string, string>, values: Row) {
        for (const [field, target] of references) {
            const value = values[field];
            if (typeof value === "string") guardRow(target, value);
        }
    }

    /** Refuses a row in a provisional scope. */
    function guardScope(scopeId: unknown) {
        if (typeof scopeId !== "string" || !hasProvisional()) return;
        const found = db.get<{ entity: string }>(
            "SELECT entity FROM sync_provisional WHERE row_id = ? AND field IS NULL",
            [scopeId],
        );
        if (found) throw new ProvisionalRowError(found.entity, scopeId);
    }

    /** Removes provisional results: of one change, or of the changes the server applied in a scope. */
    function settleProvisional(changeId: string): void;
    function settleProvisional(changeId: null, scopeId: string): void;
    function settleProvisional(changeId: string | null, scopeId?: string) {
        if (!hasProvisional()) return;
        const effects =
            changeId !== null
                ? db.all<ProvisionalEffect>("SELECT * FROM sync_provisional WHERE change_id = ? ORDER BY id DESC", [
                      changeId,
                  ])
                : db.all<ProvisionalEffect>(
                      "SELECT * FROM sync_provisional WHERE scope_id IN (?, ?) AND acked = 1 ORDER BY id DESC",
                      [scopeId!, scopeAlias(scopeId!)],
                  );
        for (const effect of effects) {
            db.run("DELETE FROM sync_provisional WHERE id = ?", [effect.id]);
            const current = row(effect.entity, effect.row_id);
            if (!current) continue;
            if (effect.field === null) {
                deleteWithProvisional(effect.entity, effect.row_id);
                continue;
            }
            db.run(`UPDATE ${effect.entity} SET ${effect.field} = ? WHERE sync_id = ?`, [
                addValues(current[effect.field], negate(effect.delta)),
                effect.row_id,
            ]);
            touch(effect.entity);
        }
    }

    /**
     * Takes back every provisional result of a scope, applied or not, for a purge: the
     * scope's changes will never be answered. Rows made in other scopes go too.
     */
    function discardProvisional(scopeId: string) {
        if (!hasProvisional()) return;
        const changes = db.all<{ change_id: string }>(
            "SELECT DISTINCT change_id FROM sync_provisional WHERE scope_id IN (?, ?)",
            [scopeId, scopeAlias(scopeId)],
        );
        for (const { change_id } of changes) settleProvisional(change_id);
    }

    /** The scope a remapped scope became, so a client that still uses the old id settles its results. */
    function scopeAlias(scopeId: string): string {
        return (
            db.get<{ value: string }>("SELECT value FROM local_meta WHERE key = ?", [
                `sync.provisional_scope.${scopeId}`,
            ])?.value ?? scopeId
        );
    }

    /** The server applied the change: its result arrives with the next drained pull. */
    function ackProvisional(changeId: string) {
        if (hasProvisional()) db.run("UPDATE sync_provisional SET acked = 1 WHERE change_id = ?", [changeId]);
    }

    /**
     * Removes a row the server never saw, with its queued changes. What points
     * at it follows the reference's column, as a database's foreign key would:
     *
     * - An optional link (a nullable column) is cleared, in the rows and in
     *   unsent changes, and the row stays (`ON DELETE SET NULL`).
     * - A required link (`NOT NULL`) takes along a new row that it made: the
     *   change that inserted that row cancels it in turn (`ON DELETE CASCADE`).
     *   A later upsert of a row that stays is kept; the server rejects it with
     *   the row's current values, and the device restores the row from them.
     * - An operation follows the same rule for its arguments, by the column of
     *   the same name in its own table: an optional link is cleared and the
     *   operation stays; otherwise it is dropped with its deltas, and one that
     *   made its own row cancels that row.
     */
    function cancel(entity: string, rowId: string) {
        drop(entriesFor(entity, rowId));
        const dependents = provisionalDependents(entity, rowId);
        deleteRow(entity, rowId);
        clearLinks(entity, rowId);
        releaseProvisional(entity, rowId, dependents);
        const key = `${entity}:${rowId}`;
        const mentions = db.all<OutboxEntry>(
            "SELECT * FROM sync_outbox WHERE state = 'pending' AND attempts = 0 AND instr(payload, ?) > 0 ORDER BY position",
            [rowId],
        );
        for (const mention of mentions) {
            if (!referencesOf(mention).includes(key)) continue;
            // A cancel earlier in this loop may have removed it already.
            const entry = db.get<OutboxEntry>("SELECT * FROM sync_outbox WHERE position = ?", [
                mention.position,
            ]);
            if (!entry) continue;
            if (!clearOptionalFields(entry, entity, rowId)) continue;
            const child = row(entry.entity, entry.row_id);
            const entries = entriesFor(entry.entity, entry.row_id);
            if (child && !knownToServer(child, entries) && entries[0].position === entry.position) {
                cancel(entry.entity, entry.row_id);
                continue;
            }
            if (entry.op !== "upsert") drop([entry]);
        }
    }

    /**
     * What happens to provisional rows when a row they point at goes: a cancelled row, or
     * a provisional row whose change settled. As a foreign key would, an optional link is
     * cleared and a required link takes the provisional row along; a row in a scope that
     * goes, goes too. Deltas on the row that went have nothing left to land on.
     *
     * The dependents are found before the row goes, since an entity's scope resolver may
     * read that row.
     */
    type Dependent = { effect: ProvisionalEffect; clear: string[]; goes: boolean };

    function provisionalDependents(entity: string, rowId: string): Dependent[] {
        if (!hasProvisional()) return [];
        const dependents: Dependent[] = [];
        for (const effect of db.all<ProvisionalEffect>(
            "SELECT * FROM sync_provisional WHERE field IS NULL ORDER BY id",
        )) {
            const source = specs.get(effect.entity);
            const child = row(effect.entity, effect.row_id);
            if (!source || !child || (effect.entity === entity && effect.row_id === rowId)) continue;
            const dependent: Dependent = { effect, clear: [], goes: source.scope(child, db) === rowId };
            for (const [field, pointsAt] of source.references) {
                if (dependent.goes || pointsAt !== entity || child[field] !== rowId) continue;
                if (optional(effect.entity, field)) dependent.clear.push(field);
                else dependent.goes = true;
            }
            if (dependent.goes || dependent.clear.length) dependents.push(dependent);
        }
        return dependents;
    }

    function releaseProvisional(entity: string, rowId: string, dependents: Dependent[]) {
        if (!hasProvisional()) return;
        db.run("DELETE FROM sync_provisional WHERE entity = ? AND row_id = ? AND field IS NOT NULL", [
            entity,
            rowId,
        ]);
        for (const { effect, clear, goes } of dependents) {
            if (!row(effect.entity, effect.row_id)) continue;
            if (goes) {
                db.run("DELETE FROM sync_provisional WHERE id = ?", [effect.id]);
                deleteWithProvisional(effect.entity, effect.row_id);
                continue;
            }
            for (const field of clear) {
                db.run(`UPDATE ${effect.entity} SET ${field} = NULL WHERE sync_id = ?`, [effect.row_id]);
            }
            touch(effect.entity);
        }
    }

    /** Deletes a row, and the provisional rows that need it follow. */
    function deleteWithProvisional(entity: string, rowId: string) {
        const dependents = provisionalDependents(entity, rowId);
        deleteRow(entity, rowId);
        releaseProvisional(entity, rowId, dependents);
    }

    /** Whether a reference column may hold null. A field that is no column counts as required. */
    function optional(table: string, field: string): boolean {
        let found = nullableCache.get(table);
        if (!found) {
            found = new Set(
                db
                    .all<{ name: string; notnull: number }>(`PRAGMA table_info(${table})`)
                    .filter((column) => column.notnull === 0)
                    .map((column) => column.name),
            );
            nullableCache.set(table, found);
        }
        return found.has(field);
    }

    /** Sets optional links to a removed row to null in every synced table. */
    function clearLinks(entity: string, rowId: string) {
        for (const source of specs.values()) {
            for (const [field, pointsAt] of source.references) {
                if (pointsAt !== entity || !optional(source.name, field)) continue;
                const { changes } = db.run(
                    `UPDATE ${source.name} SET ${field} = NULL WHERE ${field} = ?`,
                    [rowId],
                );
                if (changes > 0) touch(source.name);
            }
        }
    }

    /**
     * Clears the optional links of an unsent change to a removed row, in its
     * payload and in its own row. Returns whether a required link is left.
     */
    function clearOptionalFields(entry: OutboxEntry, entity: string, rowId: string): boolean {
        const source = spec(entry.entity);
        const references =
            entry.op === "upsert"
                ? source.references
                : (source.operations.get(entry.op)?.references ?? new Map<string, string>());
        const payload = JSON.parse(entry.payload) as Row;
        let cleared = false;
        let required = false;
        for (const [field, pointsAt] of references) {
            if (pointsAt !== entity || payload[field] !== rowId) continue;
            if (optional(entry.entity, field)) {
                payload[field] = null;
                db.run(
                    `UPDATE ${entry.entity} SET ${field} = NULL WHERE sync_id = ? AND ${field} = ?`,
                    [entry.row_id, rowId],
                );
                cleared = true;
            } else {
                required = true;
            }
        }
        if (cleared) {
            db.run("UPDATE sync_outbox SET payload = ? WHERE position = ?", [
                JSON.stringify(payload),
                entry.position,
            ]);
            touch("sync_outbox");
            touch(entry.entity);
        }
        return required;
    }

    /**
     * Writes a server version of a row with the queued changes on top: fields of
     * queued upserts keep their queued values, counters get their queued deltas,
     * and a queued delete keeps the row hidden.
     */
    function rebaseRow(
        entity: string,
        syncId: string,
        server: Row,
        { seq, scopeId }: { seq?: number; scopeId: string },
    ) {
        const known = columns(entity);
        const values: Row = {};
        for (const [column, value] of Object.entries(server)) {
            if (!known.has(column) || column.startsWith("local_")) continue;
            if (["sync_id", scopeColumn, "server_seq", "deleted_at"].includes(column)) continue;
            values[column] = value;
        }
        let hidden = false;
        for (const entry of entriesFor(entity, syncId)) {
            if (entry.op === "delete") hidden = true;
            if (entry.op !== "upsert") continue;
            for (const [column, value] of Object.entries(JSON.parse(entry.payload) as Row)) {
                if (known.has(column)) values[column] = value;
            }
        }
        for (const hit of countersLandingOn(entity, syncId)) {
            if (hit.field in values) values[hit.field] = addValues(values[hit.field], hit.delta);
        }
        for (const effect of provisionalOn(entity, syncId)) {
            if (effect.field! in values) values[effect.field!] = addValues(values[effect.field!], effect.delta);
        }
        if (seq !== undefined) values.server_seq = seq;
        const stamp = now();
        values.updated_at ??= stamp;
        const existing = row(entity, syncId);
        if (existing) {
            values.deleted_at = hidden ? (existing.deleted_at ?? stamp) : null;
            const names = Object.keys(values);
            db.run(
                `UPDATE ${entity} SET ${names.map((name) => `${name} = ?`).join(", ")} WHERE sync_id = ?`,
                [...Object.values(values), syncId],
            );
        } else {
            values.created_at ??= stamp;
            values.deleted_at = hidden ? stamp : null;
            if (known.has(scopeColumn)) values[scopeColumn] = scopeId;
            const names = ["sync_id", ...Object.keys(values)];
            db.run(
                `INSERT INTO ${entity} (${names.join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
                [syncId, ...Object.values(values)],
            );
        }
        touch(entity);
    }

    /**
     * The server matched a device row to one of its own. Every reference to the
     * device's sync_id, in the tables and in unsent outbox entries, moves to the
     * server row's sync_id.
     * Entries the server may have seen keep their payload, so a resend stays the same.
     */
    function remap(entity: string, from: string, to: string) {
        if (from === to) return;
        const target = row(entity, to);
        if (target) {
            // The row this one becomes gets its provisional deltas, which move with the records below.
            for (const effect of provisionalOn(entity, from)) {
                const current = row(entity, to)!;
                db.run(`UPDATE ${entity} SET ${effect.field} = ? WHERE sync_id = ?`, [
                    addValues(current[effect.field!], effect.delta),
                    to,
                ]);
            }
            for (const column of columns(entity)) {
                if (!column.startsWith("local_")) continue;
                db.run(
                    `UPDATE ${entity} SET ${column} = COALESCE(${column}, (SELECT ${column} FROM ${entity} WHERE sync_id = ?)) WHERE sync_id = ?`,
                    [from, to],
                );
            }
            db.run(`DELETE FROM ${entity} WHERE sync_id = ?`, [from]);
        } else {
            db.run(`UPDATE ${entity} SET sync_id = ? WHERE sync_id = ?`, [to, from]);
        }
        touch(entity);

        for (const source of specs.values()) {
            for (const [field, pointsAt] of source.references) {
                if (pointsAt !== entity) continue;
                const { changes } = db.run(
                    `UPDATE ${source.name} SET ${field} = ? WHERE ${field} = ?`,
                    [to, from],
                );
                if (changes > 0) touch(source.name);
            }
            // Ids are unique, so a scope id in any table can only be this scope.
            if (columns(source.name).has(scopeColumn)) {
                const { changes } = db.run(
                    `UPDATE ${source.name} SET ${scopeColumn} = ? WHERE ${scopeColumn} = ?`,
                    [to, from],
                );
                if (changes > 0) touch(source.name);
            }
        }
        for (const table of ["sync_outbox", "sync_rejected", "sync_state"]) {
            db.run(`UPDATE OR IGNORE ${table} SET scope_id = ? WHERE scope_id = ?`, [to, from]);
        }
        if (hasProvisional()) {
            const { changes } = db.run("UPDATE sync_provisional SET scope_id = ? WHERE scope_id = ?", [to, from]);
            if (changes > 0) {
                db.run(
                    "INSERT INTO local_meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    [`sync.provisional_scope.${from}`, to],
                );
            }
            db.run("UPDATE sync_provisional SET row_id = ? WHERE entity = ? AND row_id = ?", [to, entity, from]);
        }
        db.run("UPDATE sync_rejected SET row_id = ? WHERE entity = ? AND row_id = ?", [
            to,
            entity,
            from,
        ]);
        db.run(
            "UPDATE sync_outbox SET row_id = ? WHERE entity = ? AND row_id = ? AND state = 'pending' AND attempts = 0",
            [to, entity, from],
        );
        const mentions = db.all<OutboxEntry>(
            "SELECT * FROM sync_outbox WHERE state = 'pending' AND attempts = 0 AND instr(payload, ?) > 0",
            [from],
        );
        for (const entry of mentions) {
            const source = spec(entry.entity);
            const references = new Map(source.references);
            for (const [arg, pointsAt] of source.operations.get(entry.op)?.references ?? []) {
                references.set(arg, pointsAt);
            }
            const payload = JSON.parse(entry.payload) as Row;
            for (const [field, pointsAt] of references) {
                if (pointsAt === entity && payload[field] === from) payload[field] = to;
            }
            db.run("UPDATE sync_outbox SET payload = ? WHERE position = ?", [
                JSON.stringify(payload),
                entry.position,
            ]);
        }
        touch("sync_outbox");
    }

    const writer: LocalWriter = {
        get: (sql, params = []) => db.get(sql, params),
        all: (sql, params = []) => db.all(sql, params),
        put(table, syncId, values) {
            const entity = spec(table);
            guardRow(table, syncId);
            guardReferences(entity.references, values);
            for (const column of Object.keys(values)) {
                if (!plainName.test(column)) throw new Error(`${column} is not a column name.`);
                if (conventions.has(column) && column !== scopeColumn) {
                    throw new Error(`The store sets ${column} itself.`);
                }
            }
            const stamp = now();
            if (row(table, syncId)) {
                // A counter of an existing row changes only by deltas, so provisional ones can be taken back.
                for (const column of Object.keys(values)) {
                    if (entity.counterFields.has(column)) {
                        throw new Error(`${table}.${column} is a counter. Change it through an operation.`);
                    }
                }
                const names = Object.keys(values);
                db.run(
                    `UPDATE ${table} SET ${[...names, "updated_at"].map((name) => `${name} = ?`).join(", ")} WHERE sync_id = ?`,
                    [...Object.values(values), stamp, syncId],
                );
            } else {
                const names = ["sync_id", ...Object.keys(values), "created_at", "updated_at"];
                db.run(
                    `INSERT INTO ${table} (${names.join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
                    [syncId, ...Object.values(values), stamp, stamp],
                );
            }
            // After the write, inside the operation's transaction, so a refusal undoes it.
            guardScope(entity.scope(row(table, syncId)!, db));
            touch(table);
        },
    };

    return {
        db,
        now,
        specs,
        scopeColumn,
        versions,
        listeners,
        writer,
        transaction,
        touch,
        spec,
        columns,
        row,
        deleteRow,
        deleteWithProvisional,
        checkWrite,
        entriesFor,
        unsent,
        knownToServer,
        append,
        queueUpsert,
        referencesOf,
        shiftCounters,
        drop,
        cancel,
        rebaseRow,
        remap,
        hasProvisional,
        writeProvisional,
        isProvisionalRow,
        guardRow,
        guardReferences,
        guardScope,
        settleProvisional,
        discardProvisional,
        ackProvisional,
    };
}

function resolveSpecs(
    tables: readonly string[],
    entities: Partial<Record<string, EntitySpec>>,
    scopeColumn: string,
): Map<string, Spec> {
    const specs = new Map<string, Spec>();
    for (const table of tables) {
        if (!plainName.test(table)) throw new Error(`${table} is not a table name.`);
        const entity = entities[table] ?? {};
        const operations: Spec["operations"] = new Map();
        for (const [op, operation] of Object.entries(entity.operations ?? {})) {
            if (op === "upsert" || op === "delete" || !plainName.test(op)) {
                throw new Error(`${op} cannot be the name of an operation.`);
            }
            const references = new Map(Object.entries(operation.references ?? {}));
            for (const counter of operation.counters ?? []) {
                for (const name of [counter.field, counter.delta, counter.row ?? "x"]) {
                    if (!plainName.test(name)) throw new Error(`${name} is not a column name.`);
                }
                if (counter.row) references.set(counter.row, counter.entity ?? table);
            }
            operations.set(op, { counters: operation.counters ?? [], references });
        }
        specs.set(table, {
            name: table,
            writable: entity.writable ? new Set(entity.writable) : null,
            serverOwned: new Set(entity.serverOwned ?? []),
            references: new Map(Object.entries(entity.references ?? {})),
            operations,
            counterFields: new Set(),
            scope:
                entity.scope ??
                ((values) => {
                    const scope = values[scopeColumn];
                    return typeof scope === "string" ? scope : null;
                }),
        });
    }
    for (const [table, entity] of specs) {
        for (const operation of entity.operations.values()) {
            for (const counter of operation.counters) {
                const target = specs.get(counter.entity ?? table);
                if (!target) throw new Error(`${counter.entity} is not a synced table.`);
                target.counterFields.add(counter.field);
            }
        }
    }
    return specs;
}

/** Throws the error outside the write, where the app's global error handler sees it. */
function rethrowLater(error: unknown) {
    queueMicrotask(() => {
        throw error;
    });
}

function parseList(json: string | null): string[] {
    return json === null ? [] : (JSON.parse(json) as string[]);
}
