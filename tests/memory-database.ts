import { DatabaseSync } from "node:sqlite";
import type { SqlDatabase, SqlValue } from "../src/database.ts";

/** An in-memory node:sqlite database behind the same interface expo-sqlite fills on the device. */
export function memoryDatabase(): SqlDatabase {
    const db = new DatabaseSync(":memory:");
    return {
        exec: (sql) => db.exec(sql),
        run: (sql, params = []) => ({ changes: Number(db.prepare(sql).run(...params).changes) }),
        all: <T>(sql: string, params: SqlValue[] = []) => db.prepare(sql).all(...params) as T[],
        get: <T>(sql: string, params: SqlValue[] = []) =>
            (db.prepare(sql).get(...params) as T | undefined) ?? null,
    };
}
