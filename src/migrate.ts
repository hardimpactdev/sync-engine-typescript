import type { SqlDatabase } from "./database.ts";

/** A SQL script, or a function for a step that needs code, such as moving data. */
export type Migration = string | ((db: SqlDatabase) => void);

/**
 * Brings the database to the latest version, counted in `PRAGMA user_version`.
 * Each step commits on its own. Never edit a shipped step; add a new one.
 *
 * Steps run with foreign keys off, so a step can rebuild a table. Before each
 * commit the runner checks that no foreign key is broken.
 */
export function migrate(db: SqlDatabase, migrations: readonly Migration[]) {
    const version = db.get<{ user_version: number }>("PRAGMA user_version")?.user_version ?? 0;
    if (version > migrations.length) {
        throw new Error(
            `The local database is version ${version}, newer than this app (${migrations.length}).`,
        );
    }
    db.exec("PRAGMA foreign_keys = OFF");
    try {
        for (let next = version; next < migrations.length; next++) {
            db.exec("BEGIN IMMEDIATE");
            try {
                const step = migrations[next];
                if (typeof step === "string") db.exec(step);
                else step(db);
                const broken = db.all("PRAGMA foreign_key_check");
                if (broken.length > 0) {
                    throw new Error(`Migration ${next + 1} breaks ${broken.length} foreign keys.`);
                }
                db.exec(`PRAGMA user_version = ${next + 1}`);
                db.exec("COMMIT");
            } catch (failure) {
                db.exec("ROLLBACK");
                throw failure;
            }
        }
    } finally {
        db.exec("PRAGMA foreign_keys = ON");
    }
}

/**
 * The engine's own tables, version 1. Add it as one step of the app's
 * migrations. It only creates what is missing, so an app that already has
 * `local_meta` can add it too. A later engine version adds its own script.
 */
export const syncEngineSchemaV1 = `
    CREATE TABLE IF NOT EXISTS local_meta (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_state (
        scope_id TEXT PRIMARY KEY NOT NULL,
        cursor TEXT,
        snapshot_complete INTEGER NOT NULL DEFAULT 0,
        last_pull_at TEXT,
        last_push_at TEXT,
        last_error TEXT,
        backoff_until TEXT
    );

    CREATE TABLE IF NOT EXISTS sync_outbox (
        position INTEGER PRIMARY KEY AUTOINCREMENT,
        change_id TEXT NOT NULL UNIQUE,
        scope_id TEXT NOT NULL,
        entity TEXT NOT NULL,
        row_id TEXT NOT NULL,
        op TEXT NOT NULL,
        payload TEXT NOT NULL,
        base_seq INTEGER,
        depends_on TEXT,
        state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'sending')),
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS sync_outbox_row ON sync_outbox (entity, row_id);
    CREATE INDEX IF NOT EXISTS sync_outbox_scope ON sync_outbox (scope_id, state, position);

    CREATE TABLE IF NOT EXISTS sync_rejected (
        change_id TEXT PRIMARY KEY NOT NULL,
        scope_id TEXT NOT NULL,
        entity TEXT NOT NULL,
        row_id TEXT NOT NULL,
        op TEXT NOT NULL,
        payload TEXT NOT NULL,
        base_seq INTEGER,
        error_code TEXT NOT NULL,
        error_message TEXT,
        error_fields TEXT,
        current TEXT,
        created_at TEXT NOT NULL,
        rejected_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS sync_rejected_row ON sync_rejected (entity, row_id);
`;

/**
 * The engine's changes of 0.1.9. Add it as a later step than `syncEngineSchemaV1`.
 * It only adds what is missing, so it can run on any database that has version 1.
 *
 * - `sync_provisional` holds what a change wrote only on this device: a row
 *   (`field` null) or a delta on a field. `acked` is 1 once the server applied the
 *   change. An app may read it, for example to leave provisional rows out of a
 *   picker; only the store writes it.
 * - `sync_outbox.merge` marks an account merge change (protocol §5.4).
 */
export const syncEngineSchemaV2: Migration = (db) => {
    db.exec(`
        CREATE TABLE IF NOT EXISTS sync_provisional (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            change_id TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            entity TEXT NOT NULL,
            row_id TEXT NOT NULL,
            field TEXT,
            delta TEXT,
            acked INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS sync_provisional_change ON sync_provisional (change_id);
        CREATE INDEX IF NOT EXISTS sync_provisional_row ON sync_provisional (entity, row_id);
        CREATE INDEX IF NOT EXISTS sync_provisional_scope ON sync_provisional (scope_id, acked);
    `);
    const outbox = db.all<{ name: string }>("PRAGMA table_info(sync_outbox)");
    if (!outbox.some((column) => column.name === "merge")) {
        db.exec("ALTER TABLE sync_outbox ADD COLUMN merge INTEGER NOT NULL DEFAULT 0");
    }
};

/**
 * Proposed rows (protocol §5.5): the ids an outbox entry proposes, and the key of each
 * provisional row whose id it proposes. Reads the schema first, so it runs on any earlier one.
 */
export const syncEngineSchemaV3: Migration = (db) => {
    const has = (table: string, column: string) =>
        db.all<{ name: string }>(`PRAGMA table_info(${table})`).some((found) => found.name === column);
    if (!has("sync_outbox", "proposals")) db.exec("ALTER TABLE sync_outbox ADD COLUMN proposals TEXT");
    if (!has("sync_provisional", "proposal")) db.exec("ALTER TABLE sync_provisional ADD COLUMN proposal TEXT");
};
