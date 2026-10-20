import assert from "node:assert/strict";
import test from "node:test";
import { createSyncClient } from "../src/client.ts";
import { migrate, syncEngineSchemaV1 } from "../src/migrate.ts";
import { createLocalStore, MissingRowError } from "../src/store.ts";
import { applyPullPage, returnToPending, takeBatch } from "../src/sync.ts";
import { memoryDatabase } from "./memory-database.ts";
import { scope, outbox, pulled, setup, type Store } from "./setup.ts";

function room(store: Store, name = "Kitchen") {
    return store.insert("rooms", { scope_id: scope, name });
}

function stock(store: Store, id: string) {
    return store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [id])
        ?.quantity;
}

test("a write changes the row and queues one merged upsert", () => {
    const { store } = setup();
    const kitchen = room(store);
    const item = store.insert("items", {
        scope_id: scope,
        name: "Dishwasher",
        local_photo_path: "photos/a.jpg",
    });
    store.update("items", item, { notes: "Under the sink", local_photo_path: null });
    store.update("items", item, { local_photo_path: "photos/b.jpg" });
    store.update("items", item, { room_id: kitchen, notes: "Left of the sink" });

    const row = store.get<{ notes: string; local_photo_path: string }>(
        "SELECT notes, local_photo_path FROM items WHERE sync_id = ?",
        [item],
    );
    assert.equal(row?.notes, "Left of the sink");
    assert.equal(row?.local_photo_path, "photos/b.jpg");

    // Device-only columns and scope_id never reach the payload. Later values win.
    assert.deepEqual(outbox(store), [
        {
            entity: "rooms",
            row_id: kitchen,
            op: "upsert",
            payload: { name: "Kitchen" },
            state: "pending",
            attempts: 0,
            base_seq: null,
        },
        {
            entity: "items",
            row_id: item,
            op: "upsert",
            payload: { name: "Dishwasher", notes: "Left of the sink", room_id: kitchen },
            state: "pending",
            attempts: 0,
            base_seq: null,
        },
    ]);
    const entry = store.get<{ scope_id: string }>("SELECT scope_id FROM sync_outbox");
    assert.equal(entry?.scope_id, scope);
});

test("a merge never moves a reference ahead of the row it points at", () => {
    const { store } = setup();
    const item = store.insert("items", { scope_id: scope, name: "Lamp" });
    const attic = room(store, "Attic");
    store.update("items", item, { room_id: attic });
    store.update("items", item, { notes: "Broken" });

    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.payload]),
        [
            ["items", { name: "Lamp" }],
            ["rooms", { name: "Attic" }],
            ["items", { room_id: attic, notes: "Broken" }],
        ],
    );
});

test("an unsent insert and its delete cancel out", () => {
    const { store } = setup();
    const item = store.insert("items", { scope_id: scope, name: "Kettle" });
    store.update("items", item, { notes: "Old" });
    store.remove("items", item);
    assert.deepEqual(outbox(store), []);
    assert.equal(store.all("SELECT * FROM items").length, 0);
});

test("a delete of a row the server knows waits for its ack with the row hidden", () => {
    const { store } = setup();
    pulled(store, "items", "01JBC59ZQ0B2D4F6H8K0M2P4R6", { name: "Dishwasher" }, 4182);
    store.update("items", "01JBC59ZQ0B2D4F6H8K0M2P4R6", { notes: "Noisy" });
    store.remove("items", "01JBC59ZQ0B2D4F6H8K0M2P4R6");

    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.payload, entry.base_seq]),
        [["delete", {}, 4182]],
    );
    const row = store.get<{ deleted_at: string | null }>("SELECT deleted_at FROM items");
    assert.ok(row?.deleted_at);
});

test("a deleted or unknown row cannot change again", () => {
    const { store } = setup();
    pulled(store, "items", "ITEM", { name: "Milk" }, 10);
    store.remove("items", "ITEM");
    assert.throws(() => store.update("items", "ITEM", { name: "Oat milk" }), MissingRowError);
    assert.throws(() => store.remove("items", "ITEM"), MissingRowError);
    assert.throws(() => store.remove("items", "NOPE"), MissingRowError);
    assert.throws(() => store.operate("counters", "NOPE", "adjust", { delta: 1 }), MissingRowError);
});

test("a sent change is frozen, so later edits queue a new entry", () => {
    const { store } = setup();
    const item = store.insert("items", { scope_id: scope, name: "Fridge" });
    takeBatch(store, scope);
    store.update("items", item, { notes: "Humming" });
    assert.deepEqual(
        outbox(store).map((entry) => [entry.state, entry.payload]),
        [
            ["sending", { name: "Fridge" }],
            ["pending", { notes: "Humming" }],
        ],
    );

    // A failed send goes back to pending, but the server may have applied it,
    // so it keeps its payload and a delete still reaches the server.
    returnToPending(store, scope);
    store.update("items", item, { notes: "Quiet" });
    store.remove("items", item);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.attempts, entry.payload]),
        [
            ["upsert", 1, { name: "Fridge" }],
            ["delete", 0, {}],
        ],
    );
});

test("the store sets keys and timestamps itself and refuses fields it may not write", () => {
    const { store } = setup();
    assert.throws(
        () => store.insert("rooms", { scope_id: scope, name: "Hall", sync_id: "X" }),
        /sets sync_id itself/,
    );
    assert.throws(
        () => store.insert("rooms", { scope_id: scope, "name = 1; --": "Hall" }),
        /not a column name/,
    );
    assert.throws(() => store.insert("users" as never, { name: "Nick" }), /not a synced table/);
    assert.throws(
        () => store.insert("items", { scope_id: scope, name: "Oven", illustration: "x" }),
        /server sets items.illustration/,
    );
    assert.throws(
        () => store.insert("counters", { scope_id: scope, name: "Milk", quantity: 3 }),
        /is a counter/,
    );
    assert.throws(
        () => store.insert("movements", { scope_id: scope, delta: "1" }),
        /not a writable field/,
    );
    const hall = room(store, "Hall");
    assert.throws(() => store.update("rooms", hall, { scope_id: "OTHER" }), /sets scope_id/);
    assert.throws(() => store.operate("counters", hall, "reset", {}), /no operation reset/);
});

test("synced references have no foreign keys, so a child may arrive first", () => {
    const { store } = setup();
    store.insert("items", { scope_id: scope, name: "Sofa", room_id: "NOT-YET-PULLED" });
    assert.equal(store.all("SELECT * FROM items").length, 1);
});

test("an operation changes its counters and is never merged", () => {
    const { store } = setup();
    const milk = store.newId();
    const create = () =>
        store.operate("counters", milk, "adjust", { name: "Milk", delta: 1 }, (local) => {
            if (!local.get("SELECT 1 FROM counters WHERE sync_id = ?", [milk])) {
                local.put("counters", milk, { scope_id: scope, name: "Milk" });
            }
        });
    create();
    create();
    store.operate("counters", milk, "adjust", { name: "Milk", delta: -1 });

    assert.equal(
        store.get<{ quantity: number }>("SELECT quantity FROM counters")?.quantity,
        1,
    );
    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.payload]),
        [
            ["adjust", { name: "Milk", delta: 1 }],
            ["adjust", { name: "Milk", delta: 1 }],
            ["adjust", { name: "Milk", delta: -1 }],
        ],
    );
});

test("an operation may change a total on another row, and its delete takes it back", () => {
    const { store } = setup();
    const rice = store.insert("stock", { scope_id: scope, name: "Rice" });
    const record = (delta: string) => {
        const movement = store.newId();
        store.operate("movements", movement, "record", { stock_id: rice, delta }, (local) =>
            local.put("movements", movement, { scope_id: scope, stock_id: rice, delta }),
        );
        return movement;
    };
    record("1.500");
    const second = record("0.250");
    assert.equal(stock(store, rice), "1.750");

    // An unsent movement that is deleted leaves no trace.
    store.remove("movements", second);
    assert.equal(stock(store, rice), "1.500");
    assert.deepEqual(
        outbox(store).map((entry) => entry.op),
        ["upsert", "record"],
    );
});

test("a failed transaction keeps nothing and tells no one", () => {
    const { store } = setup();
    let calls = 0;
    store.subscribe(["rooms", "sync_outbox"], () => calls++);
    assert.throws(() =>
        store.transaction(() => {
            room(store, "Garage");
            throw new Error("stop");
        }),
    );
    assert.equal(store.all("SELECT * FROM rooms").length, 0);
    assert.equal(store.all("SELECT * FROM sync_outbox").length, 0);
    assert.equal(calls, 0);
});

test("a failed inner step rolls back only itself", () => {
    const { store } = setup();
    store.transaction(() => {
        room(store, "Garage");
        assert.throws(() =>
            store.transaction(() => {
                room(store, "Shed");
                throw new Error("stop");
            }),
        );
    });
    assert.deepEqual(
        store.all<{ name: string }>("SELECT name FROM rooms").map((row) => row.name),
        ["Garage"],
    );
    assert.equal(outbox(store).length, 1);
});

test("listeners hear about their tables once per commit", () => {
    const { store } = setup();
    const heard: string[] = [];
    const stop = store.subscribe(["rooms", "items"], () => heard.push("inventory"));
    store.subscribe(["counters"], () => heard.push("counters"));
    store.subscribe(["sync_outbox"], () => heard.push("outbox"));
    const before = store.version(["rooms"]);

    store.transaction(() => {
        room(store, "Garage");
        store.insert("items", { scope_id: scope, name: "Bike" });
    });
    assert.deepEqual(heard.sort(), ["inventory", "outbox"]);
    assert.notEqual(store.version(["rooms"]), before);

    stop();
    room(store, "Shed");
    const milk = store.newId();
    store.operate("counters", milk, "adjust", { delta: 1 }, (local) =>
        local.put("counters", milk, { scope_id: scope, name: "Milk" }),
    );
    assert.deepEqual(heard.sort(), ["counters", "inventory", "outbox", "outbox", "outbox"]);
});

test("an app may keep the scope in a column of its own", async () => {
    const db = memoryDatabase();
    migrate(db, [
        syncEngineSchemaV1,
        `CREATE TABLE notes (
            sync_id TEXT PRIMARY KEY NOT NULL,
            team_id TEXT NOT NULL,
            server_id INTEGER UNIQUE,
            server_seq INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            deleted_at TEXT,
            title TEXT NOT NULL
        )`,
    ]);
    let id = 0;
    const store = createLocalStore(db, {
        tables: ["notes"],
        scopeColumn: "team_id",
        newId: () => `ID${String(++id).padStart(4, "0")}`,
    });

    const note = store.insert("notes", { team_id: scope, title: "Plan" });
    assert.deepEqual(JSON.parse(store.get<{ payload: string }>("SELECT payload FROM sync_outbox")!.payload), {
        title: "Plan",
    });
    assert.equal(store.get<{ scope_id: string }>("SELECT scope_id FROM sync_outbox")?.scope_id, scope);
    assert.throws(() => store.update("notes", note, { team_id: "OTHER" }), /The store sets team_id itself/);

    applyPullPage(store, scope, {
        changes: [{ entity: "notes", id: "PULLED", seq: 3, op: "upsert", row: { title: "From the web" } }],
        cursor: "c1",
        has_more: false,
    });
    assert.equal(store.get<{ team_id: string }>("SELECT team_id FROM notes WHERE sync_id = 'PULLED'")?.team_id, scope);

    const client = createSyncClient({
        store,
        scopeId: scope,
        clientId: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        entities: ["notes"],
        transport: async () => ({ status: 404, body: null }),
    });
    assert.deepEqual(await client.sync(), { status: "gone", lostChanges: 1 });
    assert.equal(store.get<{ count: number }>("SELECT COUNT(*) AS count FROM notes")?.count, 0);
});
