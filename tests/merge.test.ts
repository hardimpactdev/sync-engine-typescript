import assert from "node:assert/strict";
import test from "node:test";
import { migrate, syncEngineSchemaV1 } from "../src/migrate.ts";
import { createLocalStore } from "../src/store.ts";
import { returnToPending, takeBatch } from "../src/sync.ts";
import { memoryDatabase } from "./memory-database.ts";
import { scope, setup } from "./setup.ts";

// An account merge copies a row into the account's scope (protocol §5.4). The change
// carries `merge: true` on every send, and edits after it stay normal changes.

test("a merge change keeps its flag through a resend, and a later edit is a normal change", () => {
    const { store } = setup();
    const rice = store.insert("items", { scope_id: scope, name: "Rice", notes: "Guest notes" }, { merge: true });
    store.update("items", rice, { notes: "Edited after the copy" });

    const first = takeBatch(store, scope);
    returnToPending(store, scope);
    const again = takeBatch(store, scope);

    assert.deepEqual(
        first.map((change) => [change.fields, change.merge ?? false]),
        [
            [{ name: "Rice", notes: "Guest notes" }, true],
            [{ notes: "Edited after the copy" }, false],
        ],
    );
    assert.deepEqual(again, first);
});

test("an update can be a merge change of its own", () => {
    const { store } = setup();
    const rice = store.insert("items", { scope_id: scope, name: "Rice" });
    store.update("items", rice, { notes: "Copied" }, { merge: true });
    store.update("items", rice, { notes: "Mine" });

    assert.deepEqual(
        takeBatch(store, scope).map((change) => [change.fields, change.merge ?? false]),
        [
            [{ name: "Rice" }, false],
            [{ notes: "Copied" }, true],
            [{ notes: "Mine" }, false],
        ],
    );
});

test("a merge change needs the second engine schema", () => {
    const db = memoryDatabase();
    migrate(db, [
        syncEngineSchemaV1,
        `CREATE TABLE items (sync_id TEXT PRIMARY KEY NOT NULL, scope_id TEXT NOT NULL, server_id INTEGER, server_seq INTEGER,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT, name TEXT NOT NULL)`,
    ]);
    const store = createLocalStore(db, { tables: ["items"], newId: () => "01JBC59ZQ0B2D4F6H8K0M2P4T1" });

    assert.throws(() => store.insert("items", { scope_id: scope, name: "Rice" }, { merge: true }), /syncEngineSchemaV2/);
});
