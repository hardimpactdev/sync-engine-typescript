import type { Row, SqlDatabase, SqlValue } from "./database";
import { addValues, negate } from "./decimal";
import type { CounterSpec, EntitySpec } from "./spec";

// Every write to synced data goes through the store. In one transaction it
// changes the row and writes or merges its outbox entry (protocol §6). After
// commit it tells the screens that read those tables.

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
};

export type WriteOptions = {
    /** Local file ids that must upload before this change is sent. */
    dependsOn?: readonly string[];
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
};

export class MissingRowError extends Error {
    constructor(table: string, syncId: string) {
        super(`There is no ${table} row ${syncId}, or it was deleted.`);
        this.name = "MissingRowError";
    }
}

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

        insert(table, values, { dependsOn } = {}) {
            const entity = spec(table);
            const synced = engine.checkWrite(entity, values, "insert");
            const syncId = options.newId();
            const stamp = now();
            const names = ["sync_id", ...Object.keys(values), "created_at", "updated_at"];
            transaction(() => {
                db.run(
                    `INSERT INTO ${table} (${names.join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
                    [syncId, ...Object.values(values), stamp, stamp],
                );
                const row = engine.row(table, syncId)!;
                engine.append(entity, row, "upsert", { ...synced }, dependsOn);
                touch(table);
            });
            return syncId;
        },

        update(table, syncId, patch, { dependsOn } = {}) {
            const entity = spec(table);
            const synced = engine.checkWrite(entity, patch, "update");
            const columns = Object.keys(patch);
            if (columns.length === 0) return;
            transaction(() => {
                const { changes } = db.run(
                    `UPDATE ${table} SET ${columns.map((column) => `${column} = ?`).join(", ")}, updated_at = ? WHERE sync_id = ? AND deleted_at IS NULL`,
                    [...Object.values(patch), now(), syncId],
                );
                if (changes === 0) throw new MissingRowError(table, syncId);
                if (Object.keys(synced).length > 0) {
                    engine.queueUpsert(entity, engine.row(table, syncId)!, synced, dependsOn);
                }
                touch(table);
            });
        },

        remove(table, syncId) {
            const entity = spec(table);
            transaction(() => {
                const row = engine.row(table, syncId);
                if (!row || row.deleted_at !== null) throw new MissingRowError(table, syncId);
                const entries = engine.entriesFor(table, syncId);
                if (!engine.knownToServer(row, entries)) {
                    // An unsent insert and its delete cancel out.
                    engine.drop(entries);
                    engine.deleteRow(table, syncId);
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

        operate(table, rowId, op, args, applyLocally, { dependsOn } = {}) {
            const entity = spec(table);
            if (!entity.operations.has(op)) throw new Error(`${table} has no operation ${op}.`);
            transaction(() => {
                applyLocally?.(engine.writer);
                const row = engine.row(table, rowId);
                if (!row || row.deleted_at !== null) throw new MissingRowError(table, rowId);
                const entry = engine.append(entity, row, op, args, dependsOn);
                engine.shiftCounters(entry, 1);
                touch(table);
            });
        },

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
    }: StoreOptions<T>,
) {
    if (!plainName.test(scopeColumn)) throw new Error(`${scopeColumn} is not a column name.`);
    const conventions = new Set([...rowConventions, scopeColumn]);
    let depth = 0;
    const touched = new Set<string>();
    const versions = new Map<string, number>();
    const listeners = new Map<string, Set<() => void>>();
    const columnCache = new Map<string, Set<string>>();

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
        try {
            const result = work();
            db.exec(outer ? "COMMIT" : `RELEASE ${savepoint}`);
            depth--;
            if (outer) notify();
            return result;
        } catch (failure) {
            depth--;
            if (outer) {
                db.exec("ROLLBACK");
                touched.clear();
            } else {
                db.exec(`ROLLBACK TO ${savepoint}`);
                db.exec(`RELEASE ${savepoint}`);
            }
            throw failure;
        }
    }

    function notify() {
        const changed = [...touched];
        touched.clear();
        const called = new Set<() => void>();
        for (const table of changed) {
            versions.set(table, (versions.get(table) ?? 0) + 1);
            for (const listener of listeners.get(table) ?? []) {
                if (called.has(listener)) continue;
                called.add(listener);
                listener();
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
    ): OutboxEntry {
        const changeId = newId();
        db.run(
            "INSERT INTO sync_outbox (change_id, scope_id, entity, row_id, op, payload, base_seq, depends_on, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    function queueUpsert(entity: Spec, values: Row, fields: Row, dependsOn?: readonly string[]) {
        const entries = entriesFor(entity.name, String(values.sync_id));
        const last = entries.findLast((entry) => entry.op === "upsert");
        const mergeable =
            last !== undefined &&
            unsent(last) &&
            !entries.some((entry) => entry.position > last.position && entry.op === "delete") &&
            !referencesQueuedAfter(entity, fields, last.position);
        if (!mergeable) {
            append(entity, values, "upsert", fields, dependsOn);
            return;
        }
        const files = new Set<string>([...parseList(last.depends_on), ...(dependsOn ?? [])]);
        db.run("UPDATE sync_outbox SET payload = ?, depends_on = ? WHERE position = ?", [
            JSON.stringify({ ...JSON.parse(last.payload), ...fields }),
            last.depends_on === null && !dependsOn ? null : JSON.stringify([...files]),
            last.position,
        ]);
        touch("sync_outbox");
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

    /** Removes entries from the outbox and takes back their deltas. */
    function drop(entries: OutboxEntry[]) {
        for (const entry of entries) {
            shiftCounters(entry, -1);
            db.run("DELETE FROM sync_outbox WHERE position = ?", [entry.position]);
        }
        if (entries.length > 0) touch("sync_outbox");
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
            spec(table);
            for (const column of Object.keys(values)) {
                if (!plainName.test(column)) throw new Error(`${column} is not a column name.`);
                if (conventions.has(column) && column !== scopeColumn) {
                    throw new Error(`The store sets ${column} itself.`);
                }
            }
            const stamp = now();
            if (row(table, syncId)) {
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
        checkWrite,
        entriesFor,
        unsent,
        knownToServer,
        append,
        queueUpsert,
        referencesOf,
        shiftCounters,
        drop,
        rebaseRow,
        remap,
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

function parseList(json: string | null): string[] {
    return json === null ? [] : (JSON.parse(json) as string[]);
}
