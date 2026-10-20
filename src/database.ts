// The SQL surface the store needs. Any synchronous SQLite binding backs it, such
// as expo-sqlite on a phone and node:sqlite in tests, so the store runs the same
// everywhere.

export type SqlValue = string | number | null;

export interface SqlDatabase {
    exec(sql: string): void;
    run(sql: string, params?: SqlValue[]): { changes: number };
    all<T>(sql: string, params?: SqlValue[]): T[];
    get<T>(sql: string, params?: SqlValue[]): T | null;
}

export type Row = Record<string, SqlValue>;
