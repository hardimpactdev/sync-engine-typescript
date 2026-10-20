import assert from "node:assert/strict";
import test from "node:test";
import { migrate, syncEngineSchemaV1 } from "../src/migrate.ts";
import { memoryDatabase } from "./memory-database.ts";
import { testMigrations } from "./setup.ts";

const version = (db: ReturnType<typeof memoryDatabase>) =>
    db.get<{ user_version: number }>("PRAGMA user_version")?.user_version;

test("migrations run once and refuse a database from a newer app", () => {
    const db = memoryDatabase();
    migrate(db, testMigrations);
    migrate(db, testMigrations);
    assert.equal(version(db), testMigrations.length);
    db.exec(`PRAGMA user_version = ${testMigrations.length + 1}`);
    assert.throws(() => migrate(db, testMigrations), /newer than this app/);
});

test("a failed step keeps nothing and leaves the version", () => {
    const db = memoryDatabase();
    assert.throws(() =>
        migrate(db, ["CREATE TABLE a (id INTEGER)", "CREATE TABLE b (id INTEGER); SELECT nope FROM a"]),
    );
    assert.equal(version(db), 1);
    assert.equal(db.all("SELECT name FROM sqlite_master WHERE name = 'b'").length, 0);
});

test("a step may be code, and may rebuild a table that others point at", () => {
    const db = memoryDatabase();
    migrate(db, [
        `CREATE TABLE parents (id TEXT PRIMARY KEY);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents (id));
         INSERT INTO parents VALUES ('P');
         INSERT INTO children VALUES ('C', 'P');`,
        (step) => {
            step.exec(`CREATE TABLE new_parents (id TEXT PRIMARY KEY, name TEXT);
                INSERT INTO new_parents (id) SELECT id FROM parents;
                DROP TABLE parents;
                ALTER TABLE new_parents RENAME TO parents;`);
            step.run("UPDATE parents SET name = ?", ["Rebuilt"]);
        },
    ]);
    assert.deepEqual(
        db.all<object>("SELECT id, name FROM parents").map((row) => ({ ...row })),
        [{ id: "P", name: "Rebuilt" }],
    );
    assert.equal(db.get<{ foreign_keys: number }>("PRAGMA foreign_keys")?.foreign_keys, 1);
});

test("a step that breaks a foreign key is rolled back", () => {
    const db = memoryDatabase();
    assert.throws(
        () =>
            migrate(db, [
                `CREATE TABLE parents (id TEXT PRIMARY KEY);
                 CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents (id));
                 INSERT INTO children VALUES ('C', 'missing');`,
            ]),
        /breaks 1 foreign keys/,
    );
    assert.equal(version(db), 0);
});

test("the engine schema can join a database that already has local_meta", () => {
    const db = memoryDatabase();
    migrate(db, ["CREATE TABLE local_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)", syncEngineSchemaV1]);
    const names = db
        .all<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .map((table) => table.name);
    for (const table of ["local_meta", "sync_outbox", "sync_rejected", "sync_state"]) {
        assert.ok(names.includes(table), table);
    }
});
