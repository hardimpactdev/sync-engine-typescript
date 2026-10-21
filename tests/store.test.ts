import assert from "node:assert/strict";
import test from "node:test";
import { createSyncClient } from "../src/client.ts";
import { migrate, syncEngineSchemaV1 } from "../src/migrate.ts";
import { createLocalStore, MissingRowError } from "../src/store.ts";
import { applyPullPage, returnToPending, takeBatch } from "../src/sync.ts";
import { memoryDatabase } from "./memory-database.ts";
import { scope, outbox, pulled, setup, tables, testMigrations, type Store } from "./setup.ts";

function field(store: Store, table: string, id: string, column: string) {
    return store.get<Record<string, unknown>>(`SELECT ${column} FROM ${table} WHERE sync_id = ?`, [id])?.[column];
}

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

test("a cancelled insert takes new rows with a required link to it along", () => {
    const { store } = setup();
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    store.insert("parts", { scope_id: scope, item_id: lamp, name: "Shade" });
    store.remove("items", lamp);
    assert.deepEqual(outbox(store), []);
    assert.equal(store.all("SELECT * FROM parts").length, 0);
});

test("a cancelled insert clears an optional link to it and keeps the row", () => {
    const { store } = setup();
    const attic = room(store, "Attic");
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp", room_id: attic });
    store.remove("rooms", attic);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.row_id, entry.payload]),
        [["items", lamp, { name: "Lamp", room_id: null }]],
    );
    assert.equal(field(store, "items", lamp, "room_id"), null);
});

test("an operation keeps its row and loses an optional link to a cancelled insert", () => {
    const { store } = setup();
    const rice = store.insert("stock", { scope_id: scope, name: "Rice" });
    const movement = store.newId();
    store.operate("movements", movement, "record", { stock_id: rice, delta: "1.500" }, (local) =>
        local.put("movements", movement, { scope_id: scope, stock_id: rice, delta: "1.500" }),
    );
    store.remove("stock", rice);
    // movements.stock_id is nullable, so the movement stays, as with ON DELETE SET NULL.
    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.payload]),
        [["record", { stock_id: null, delta: "1.500" }]],
    );
    assert.equal(field(store, "movements", movement, "stock_id"), null);
});

test("a cancelled insert keeps a change the server may have seen", () => {
    const { store, db } = setup();
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    const shade = store.insert("parts", { scope_id: scope, item_id: lamp, name: "Shade" });
    db.run("UPDATE sync_outbox SET state = 'sending', attempts = 1 WHERE entity = 'parts'");
    store.insert("parts", { scope_id: scope, item_id: lamp, name: "Bulb" });
    store.remove("items", lamp);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.row_id, entry.state, entry.payload]),
        [[shade, "sending", { item_id: lamp, name: "Shade" }]],
    );
    assert.deepEqual(
        store.all<{ sync_id: string }>("SELECT sync_id FROM parts").map((part) => part.sync_id),
        [shade],
    );
});

test("a cleared optional link leaves a change the server may have seen as it was sent", () => {
    const { store, db } = setup();
    const attic = room(store, "Attic");
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp", room_id: attic });
    db.run("UPDATE sync_outbox SET state = 'sending', attempts = 1 WHERE entity = 'items'");
    const chair = store.insert("items", { scope_id: scope, name: "Chair", room_id: attic });
    store.remove("rooms", attic);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.row_id, entry.state, entry.payload]),
        [
            [lamp, "sending", { name: "Lamp", room_id: attic }],
            [chair, "pending", { name: "Chair", room_id: null }],
        ],
    );
    assert.equal(store.all("SELECT * FROM items WHERE room_id IS NOT NULL").length, 0);
});

test("a delete of a row the server knows leaves the changes in it alone", () => {
    const { store } = setup();
    pulled(store, "rooms", "ATTIC", { name: "Attic" }, 12);
    pulled(store, "stock", "RICE", { name: "Rice", quantity: "2.000" }, 13);
    store.insert("items", { scope_id: scope, name: "Lamp", room_id: "ATTIC" });
    const movement = store.newId();
    store.operate("movements", movement, "record", { stock_id: "RICE", delta: "1.500" }, (local) =>
        local.put("movements", movement, { scope_id: scope, stock_id: "RICE", delta: "1.500" }),
    );
    store.remove("rooms", "ATTIC");
    store.remove("stock", "RICE");
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.op]),
        [
            ["items", "upsert"],
            ["movements", "record"],
            ["rooms", "delete"],
            ["stock", "delete"],
        ],
    );
    assert.equal(store.all("SELECT * FROM items").length, 1);
});

test("a cancelled insert clears an optional link in later upserts of rows that stay", () => {
    const { store } = setup();
    pulled(store, "items", "SOFA", { name: "Sofa" }, 20);
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    const attic = room(store, "Attic");
    store.update("items", "SOFA", { room_id: attic, notes: "Torn" });
    store.update("items", lamp, { room_id: attic });
    store.remove("rooms", attic);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.row_id, entry.payload]),
        [
            [lamp, { name: "Lamp" }],
            ["SOFA", { room_id: null, notes: "Torn" }],
            [lamp, { room_id: null }],
        ],
    );
    assert.equal(store.all("SELECT * FROM items WHERE room_id IS NULL").length, 2);
});

test("a cancelled insert leaves a later upsert with a required link, for the server to reject", () => {
    const { store } = setup();
    pulled(store, "parts", "SHADE", { item_id: "SOFA", name: "Shade" }, 21);
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    store.update("parts", "SHADE", { item_id: lamp });
    store.remove("items", lamp);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.row_id, entry.payload]),
        [["SHADE", { item_id: lamp }]],
    );
    assert.equal(field(store, "parts", "SHADE", "item_id"), lamp);
});

test("a cancel follows a chain of new rows and takes back deltas on rows that stay", () => {
    const db = memoryDatabase();
    migrate(db, testMigrations);
    let id = 0;
    const store = createLocalStore(db, {
        tables,
        entities: {
            items: { references: { room_id: "rooms" } },
            parts: { references: { item_id: "items" } },
            counters: {
                operations: {
                    adjust: {
                        counters: [{ field: "quantity", delta: "delta" }],
                        references: { part_id: "parts" },
                    },
                },
            },
        },
        newId: () => `ID${String(++id).padStart(4, "0")}`,
    });
    const quantity = () =>
        store.get<{ quantity: number }>("SELECT quantity FROM counters WHERE sync_id = 'BULBS'")
            ?.quantity;
    pulled(store, "counters", "BULBS", { name: "Bulbs", quantity: 2 }, 7);
    // Required links: the shade needs the lamp, and the adjust names the shade.
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    const shade = store.insert("parts", { scope_id: scope, item_id: lamp, name: "Shade" });
    store.operate("counters", "BULBS", "adjust", { part_id: shade, delta: 3 });
    store.operate("counters", "BULBS", "adjust", { delta: 1 });
    assert.equal(quantity(), 6);

    store.remove("items", lamp);
    assert.equal(quantity(), 3);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.payload]),
        [["adjust", { delta: 1 }]],
    );
    assert.equal(store.all("SELECT * FROM parts").length, 0);
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

test("a listener that throws does not undo the commit or break later writes", () => {
    const reported: unknown[] = [];
    const { store } = setup({ onListenerError: (error) => reported.push(error) });
    const heard: string[] = [];
    const stop = store.subscribe(["rooms"], () => {
        throw new Error("broken screen");
    });
    store.subscribe(["rooms", "items"], () => heard.push("other"));
    const rooms = store.version(["rooms"]);
    const items = store.version(["items"]);

    // The write committed, so it returns as usual; the listener's error is reported apart.
    store.transaction(() => {
        room(store, "Garage");
        store.insert("items", { scope_id: scope, name: "Bike" });
    });
    assert.deepEqual(
        reported.map((error) => (error as Error).message),
        ["broken screen"],
    );
    assert.deepEqual(heard, ["other"]);
    assert.notEqual(store.version(["rooms"]), rooms);
    assert.notEqual(store.version(["items"]), items);

    stop();
    room(store, "Shed");
    assert.deepEqual(
        store.all<{ name: string }>("SELECT name FROM rooms ORDER BY name").map((row) => row.name),
        ["Garage", "Shed"],
    );
    assert.equal(outbox(store).filter((entry) => entry.entity === "rooms").length, 2);
    assert.deepEqual(heard, ["other", "other"]);
});

test("by default a listener error is thrown again in a microtask", () => {
    const { store } = setup();
    const later: (() => void)[] = [];
    const queue = globalThis.queueMicrotask;
    globalThis.queueMicrotask = (callback) => later.push(callback);
    try {
        store.subscribe(["rooms"], () => {
            throw new Error("broken screen");
        });
        room(store, "Garage");
    } finally {
        globalThis.queueMicrotask = queue;
    }
    assert.equal(later.length, 1);
    assert.throws(later[0]!, /broken screen/);
    assert.equal(outbox(store).length, 1);
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
