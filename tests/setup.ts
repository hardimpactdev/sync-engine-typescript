import { readFileSync } from "node:fs";
import { migrate, syncEngineSchemaV1, syncEngineSchemaV2 } from "../src/migrate.ts";
import { createLocalStore } from "../src/store.ts";
import { applyPullPage } from "../src/sync.ts";
import type { Row } from "../src/database.ts";
import { memoryDatabase } from "./memory-database.ts";

// A neutral schema like the fixtures': rooms, items in rooms, counters with an
// `adjust` delta, and stock that movements change through `record`.

const row = `
    sync_id TEXT PRIMARY KEY NOT NULL,
    scope_id TEXT NOT NULL,
    server_id INTEGER UNIQUE,
    server_seq INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT`;

export const testMigrations = [
    syncEngineSchemaV1,
    `
    CREATE TABLE scopes (
        sync_id TEXT PRIMARY KEY NOT NULL,
        server_id INTEGER UNIQUE,
        server_seq INTEGER,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        deleted_at TEXT,
        name TEXT NOT NULL
    );
    CREATE TABLE rooms (${row}, name TEXT NOT NULL);
    CREATE TABLE items (${row},
        room_id TEXT,
        name TEXT NOT NULL,
        notes TEXT,
        illustration TEXT,
        local_photo_path TEXT
    );
    CREATE TABLE counters (${row}, name TEXT NOT NULL, quantity INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE stock (${row}, name TEXT NOT NULL, quantity TEXT NOT NULL DEFAULT '0.000');
    CREATE TABLE movements (${row}, stock_id TEXT, delta TEXT NOT NULL);
    CREATE TABLE parts (${row}, item_id TEXT NOT NULL, name TEXT NOT NULL);
    `,
    syncEngineSchemaV2,
];

export const tables = ["scopes", "rooms", "items", "counters", "stock", "movements", "parts"] as const;
export type Table = (typeof tables)[number];

export const scope = "01J9XX0A1B2C3D4E5F6G7H8J9K";

/** A store on a fresh database. `ids` come first from newId; `at` fixes the clock. */
export function setup({
    ids = [],
    onListenerError,
}: { ids?: string[]; onListenerError?: (error: unknown) => void } = {}) {
    const db = memoryDatabase();
    migrate(db, testMigrations);
    let id = 0;
    let clock = 0;
    let fixed: string | null = null;
    const queue = [...ids];
    const store = createLocalStore(db, {
        tables,
        entities: {
            scopes: { scope: (values) => values.sync_id as string },
            // items.room_id is nullable: an optional link. parts.item_id is NOT NULL: a required one.
            items: { references: { room_id: "rooms" }, serverOwned: ["illustration"] },
            parts: { references: { item_id: "items" } },
            counters: { operations: { adjust: { counters: [{ field: "quantity", delta: "delta" }] } } },
            movements: {
                writable: [],
                operations: {
                    record: {
                        counters: [{ entity: "stock", row: "stock_id", field: "quantity", delta: "delta" }],
                    },
                },
            },
        },
        newId: () => queue.shift() ?? `ID${String(++id).padStart(4, "0")}`,
        now: () => fixed ?? new Date(Date.UTC(2026, 9, 20, 9, 0, clock++)).toISOString(),
        onListenerError,
    });
    db.run(
        "INSERT INTO scopes (sync_id, name, server_seq, created_at, updated_at) VALUES (?, 'Home', 1, 'x', 'x')",
        [scope],
    );
    const at = (time: string) => {
        fixed = time;
    };
    return { db, store, at };
}

export type Store = ReturnType<typeof setup>["store"];

export type Entry = {
    entity: string;
    row_id: string;
    op: string;
    payload: Row;
    state: string;
    attempts: number;
    base_seq: number | null;
};

export function outbox(store: Store): Entry[] {
    return store
        .all<Entry & { payload: string }>(
            "SELECT entity, row_id, op, payload, state, attempts, base_seq FROM sync_outbox ORDER BY position",
        )
        .map((entry) => ({ ...entry, payload: JSON.parse(entry.payload) }));
}

export function fixture<T>(name: string): T {
    return JSON.parse(readFileSync(new URL(`../fixtures/v1/${name}`, import.meta.url), "utf8")) as T;
}

/** Writes a row as a pull brings it. */
export function pulled(store: Store, entity: Table, id: string, row: Row, seq: number) {
    applyPullPage(store, scope, {
        changes: [{ entity, id, seq, op: "upsert", row }],
        cursor: `cursor-${seq}`,
        has_more: false,
    });
}
