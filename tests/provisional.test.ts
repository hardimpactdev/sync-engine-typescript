import assert from "node:assert/strict";
import test from "node:test";
import { createSyncClient, type SyncTransport } from "../src/client.ts";
import { testMigrations } from "./setup.ts";
import { migrate, syncEngineSchemaV1 } from "../src/migrate.ts";
import { createLocalStore, ProvisionalRowError, type ProvisionalWriter } from "../src/store.ts";
import { applyAcks, applyPullPage, takeBatch } from "../src/sync.ts";
import { memoryDatabase } from "./memory-database.ts";
import { outbox, pulled, scope, setup, type Store } from "./setup.ts";

// An action the server runs itself: "use" an item, which takes stock and writes a
// movement on the server. The device sends only the item's change, and shows the
// result it expects as provisional writes until the server's rows arrive.

const flour = "01JBC59ZQ0B2D4F6H8K0M2P4R6";
const cake = "01JBC59ZQ0B2D4F6H8K0M2P4R7";
const serverMovement = "01JBC59ZQ0B2D4F6H8K0M2P4R8";

function kitchen(options: Parameters<typeof setup>[0] = {}) {
    const { store, db } = setup(options);
    pulled(store, "stock", flour, { name: "Flour", quantity: "5.000" }, 1);
    pulled(store, "items", cake, { name: "Cake", notes: null }, 2);
    return { store, db };
}

/** Uses the cake: the change says so, and the stock and movement are provisional. */
function bake(store: Store) {
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                local.insert("movements", { scope_id: scope, stock_id: flour, delta: "-2.000" });
                local.add("stock", flour, "quantity", "-2.000");
            },
        },
    );
}

function quantity(store: Store) {
    return store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [flour])?.quantity;
}

function movements(store: Store) {
    return store.all<{ sync_id: string; delta: string }>("SELECT sync_id, delta FROM movements ORDER BY delta");
}

function provisional(store: Store) {
    return store.get<{ count: number }>("SELECT COUNT(*) AS count FROM sync_provisional")?.count;
}

/** The server applied the batch; its own movement and stock come in the same answer. */
function serverBakes(store: Store, { hasMore = false } = {}) {
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: change.change_id, status: "applied", seq: 3 }]);
    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: cake, seq: 3, op: "upsert", row: { name: "Cake", notes: "Baked" } },
            { entity: "movements", id: serverMovement, seq: 4, op: "upsert", row: { stock_id: flour, delta: "-2.000" } },
            { entity: "stock", id: flour, seq: 5, op: "upsert", row: { name: "Flour", quantity: "3.000" } },
        ],
        cursor: "c5",
        has_more: hasMore,
    });
    return change;
}

test("a provisional result shows at once and never reaches the outbox", () => {
    const { store } = kitchen();

    bake(store);

    assert.equal(quantity(store), "3.000");
    assert.equal(movements(store).length, 1);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.op, entry.payload]),
        [["items", "upsert", { notes: "Baked" }]],
    );
    const [movement] = movements(store);
    assert.equal(store.isProvisional("movements", movement.sync_id), true);
    assert.equal(store.isProvisional("items", cake), false);
});

test("the server's rows replace the provisional result after a drained pull", () => {
    const { store } = kitchen();
    bake(store);

    serverBakes(store);

    assert.equal(quantity(store), "3.000");
    assert.deepEqual(
        movements(store).map((movement) => movement.sync_id),
        [serverMovement],
    );
    assert.equal(provisional(store), 0);
});

test("a pulled row keeps the provisional delta until the pull is drained", () => {
    const { store } = kitchen();
    bake(store);

    // Another device added flour before this device's change reached the server.
    pulled(store, "stock", flour, { name: "Flour", quantity: "6.000" }, 3);
    assert.equal(quantity(store), "4.000");

    // The answer to the change has more pages: the result stays until the last one.
    serverBakes(store, { hasMore: true });
    assert.equal(quantity(store), "1.000");
    assert.equal(movements(store).length, 2);

    applyPullPage(store, scope, { changes: [], cursor: "c6", has_more: false });
    assert.equal(quantity(store), "3.000");
    assert.equal(movements(store).length, 1);
});

test("an action the server already ran counts once", () => {
    const { store } = kitchen();
    bake(store);

    // The web baked the cake first. Its rows arrive before this device's change.
    pulled(store, "stock", flour, { name: "Flour", quantity: "3.000" }, 3);
    serverBakes(store);

    assert.equal(quantity(store), "3.000");
    assert.equal(movements(store).length, 1);
});

test("a rejected change takes its provisional result back", () => {
    const { store } = kitchen();
    bake(store);

    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [
        {
            change_id: change.change_id,
            status: "rejected",
            error: { code: "validation", message: "Not today." },
            current: { name: "Cake", notes: null },
        },
    ]);

    assert.equal(quantity(store), "5.000");
    assert.equal(movements(store).length, 0);
    assert.equal(provisional(store), 0);
    assert.equal(store.get<{ notes: string | null }>("SELECT notes FROM items")?.notes, null);
});

test("removing a row keeps its unsent action, which goes before the delete", () => {
    const { store } = kitchen();
    bake(store);

    store.remove("items", cake);

    // The action stays: the server runs it once (or finds it done), then deletes the row.
    assert.equal(quantity(store), "3.000");
    assert.equal(movements(store).length, 1);
    assert.deepEqual(
        takeBatch(store, scope).map((change) => [change.entity, change.op, change.fields ?? null]),
        [
            ["items", "upsert", { notes: "Baked" }],
            ["items", "delete", null],
        ],
    );
    assert.equal(store.get("SELECT deleted_at FROM items WHERE sync_id = ?", [cake]) !== null, true);
});

test("removing a row the server never saw keeps its action and the insert it needs", () => {
    const { store } = kitchen();
    const pie = store.insert(
        "items",
        { scope_id: scope, name: "Pie" },
        { provisional: (local) => local.add("stock", flour, "quantity", "-1.000") },
    );

    store.remove("items", pie);

    assert.equal(quantity(store), "4.000");
    assert.deepEqual(
        takeBatch(store, scope).map((change) => [change.op, change.fields ?? null]),
        [
            ["upsert", { name: "Pie" }],
            ["delete", null],
        ],
    );
});

test("removing a row without an action still drops its unsent changes", () => {
    const { store } = kitchen();
    store.update("items", cake, { notes: "Iced" });
    const pie = store.insert("items", { scope_id: scope, name: "Pie" });

    store.remove("items", cake);
    store.remove("items", pie);

    assert.deepEqual(
        takeBatch(store, scope).map((change) => [change.id, change.op]),
        [[cake, "delete"]],
    );
});

test("an action kept by a remove settles after the server answers both", () => {
    const { store } = kitchen();
    bake(store);
    store.remove("items", cake);
    const batch = takeBatch(store, scope);

    applyAcks(
        store,
        scope,
        batch.map((change, index) => ({ change_id: change.change_id, status: "applied" as const, seq: 3 + index })),
    );
    applyPullPage(store, scope, {
        changes: [
            { entity: "movements", id: serverMovement, seq: 5, op: "upsert", row: { stock_id: flour, delta: "-2.000" } },
            { entity: "stock", id: flour, seq: 6, op: "upsert", row: { name: "Flour", quantity: "3.000" } },
            { entity: "items", id: cake, seq: 7, op: "delete" },
        ],
        cursor: "c7",
        has_more: false,
    });

    assert.equal(quantity(store), "3.000");
    assert.deepEqual(
        movements(store).map((movement) => movement.sync_id),
        [serverMovement],
    );
    assert.equal(provisional(store), 0);
    assert.equal(store.get("SELECT 1 FROM items WHERE sync_id = ?", [cake]), null);
});

test("later edits merged into the change keep its provisional result", () => {
    const { store } = kitchen();
    bake(store);
    store.update("items", cake, { name: "Lemon cake" });

    assert.equal(outbox(store).length, 1);
    serverBakes(store);

    assert.equal(quantity(store), "3.000");
    assert.equal(provisional(store), 0);
});

test("a provisional row cannot change or be pointed at", () => {
    const { store } = kitchen();
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                local.insert("rooms", { scope_id: scope, name: "Pantry" });
            },
        },
    );
    const room = store.get<{ sync_id: string }>("SELECT sync_id FROM rooms")!.sync_id;

    assert.throws(() => store.update("rooms", room, { name: "Larder" }), ProvisionalRowError);
    assert.throws(() => store.remove("rooms", room), ProvisionalRowError);
    assert.throws(() => store.insert("items", { scope_id: scope, name: "Jar", room_id: room }), ProvisionalRowError);
    assert.throws(() => store.update("items", cake, { room_id: room }), ProvisionalRowError);
    assert.equal(outbox(store).length, 1);
});

test("a provisional result needs a change the server gets", () => {
    const { store } = kitchen();

    assert.throws(
        () => store.update("items", cake, { local_photo_path: "a.jpg" }, { provisional: () => {} }),
        /needs a change/,
    );
});

test("a remap moves the provisional delta to the server's row", () => {
    const { store } = kitchen();
    const jar = store.insert("stock", { scope_id: scope, name: "Jar" });
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        { provisional: (local) => local.add("stock", jar, "quantity", "-1.000") },
    );
    const [first, second] = takeBatch(store, scope);
    const serverJar = "01JBC59ZQ0B2D4F6H8K0M2P4S0";
    applyAcks(store, scope, [
        { change_id: first.change_id, status: "applied", seq: 3, remap: { from: jar, to: serverJar } },
        { change_id: second.change_id, status: "applied", seq: 4 },
    ]);
    const jarStock = () =>
        store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [serverJar])?.quantity;

    applyPullPage(store, scope, {
        changes: [{ entity: "stock", id: serverJar, seq: 5, op: "upsert", row: { name: "Jar", quantity: "4.000" } }],
        cursor: "c5",
        has_more: true,
    });
    assert.equal(jarStock(), "3.000");

    applyPullPage(store, scope, { changes: [], cursor: "c6", has_more: false });
    assert.equal(jarStock(), "4.000");
    assert.equal(provisional(store), 0);
});

test("a sync run settles the result, and a resync keeps a provisional row it has not answered", async () => {
    const { store } = kitchen();
    bake(store);
    let answered = false;
    const transport: SyncTransport = async (body) => {
        if (body.changes.length === 0) {
            return {
                status: 200,
                body: { protocol: 1, acks: [], changes: [], cursor: "c9", has_more: false, server_time: "x" },
            };
        }
        answered = true;
        return {
            status: 200,
            body: {
                protocol: 1,
                acks: [{ change_id: body.changes[0].change_id, status: "applied", seq: 3 }],
                changes: [
                    { entity: "movements", id: serverMovement, seq: 4, op: "upsert", row: { stock_id: flour, delta: "-2.000" } },
                    { entity: "stock", id: flour, seq: 5, op: "upsert", row: { name: "Flour", quantity: "3.000" } },
                ],
                cursor: "c5",
                has_more: false,
                server_time: "x",
            },
        };
    };
    const client = createSyncClient({
        store,
        scopeId: scope,
        clientId: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        entities: ["items", "stock", "movements"],
        transport,
    });

    assert.deepEqual(await client.sync(), { status: "synced", pulled: 2, pushed: 1 });
    assert.equal(answered, true);
    assert.equal(quantity(store), "3.000");
    assert.deepEqual(
        movements(store).map((movement) => movement.sync_id),
        [serverMovement],
    );
});

test("provisional results need the second engine schema", () => {
    const db = memoryDatabase();
    migrate(db, [
        syncEngineSchemaV1,
        `CREATE TABLE items (sync_id TEXT PRIMARY KEY NOT NULL, scope_id TEXT NOT NULL, server_id INTEGER, server_seq INTEGER,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT, name TEXT NOT NULL)`,
    ]);
    const store = createLocalStore(db, { tables: ["items"], newId: () => "01JBC59ZQ0B2D4F6H8K0M2P4T0" });
    db.run("INSERT INTO items (sync_id, scope_id, name, created_at, updated_at) VALUES ('a', ?, 'A', 'x', 'x')", [scope]);

    assert.throws(
        () => store.update("items", "a", { name: "B" }, { provisional: () => {} }),
        /syncEngineSchemaV2/,
    );
    // Without it, everything else works as before.
    store.update("items", "a", { name: "B" });
    assert.equal(store.isProvisional("items", "a"), false);
});

test("a remap onto a row this device has keeps the provisional delta on that row", () => {
    const { store } = kitchen();
    const server = "01JBC59ZQ0B2D4F6H8K0M2P4S1";
    pulled(store, "stock", server, { name: "Sugar", quantity: "5.000" }, 3);
    const sugar = store.insert("stock", { scope_id: scope, name: "Sugar" });
    store.update("items", cake, { notes: "Baked" }, { provisional: (local) => local.add("stock", sugar, "quantity", "-2.000") });
    const [create, use] = takeBatch(store, scope);
    const sugarStock = () => store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [server])?.quantity;

    applyAcks(store, scope, [{ change_id: create.change_id, status: "applied", seq: 4, remap: { from: sugar, to: server } }]);
    assert.equal(sugarStock(), "3.000");

    applyAcks(store, scope, [
        { change_id: use.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null } },
    ]);
    assert.equal(sugarStock(), "5.000");
    applyPullPage(store, scope, { changes: [], cursor: "c9", has_more: false });
    assert.equal(sugarStock(), "5.000");
});

test("a remapped scope still settles its results when the client uses the old id", () => {
    const { store } = setup();
    const guest = store.insert("scopes", { name: "Guest" });
    const cakeId = store.insert(
        "items",
        { scope_id: guest, name: "Cake" },
        { provisional: (local) => local.insert("rooms", { scope_id: guest, name: "Provisional pantry" }) },
    );
    const [create, bake] = takeBatch(store, guest);
    applyAcks(store, guest, [
        { change_id: create.change_id, status: "applied", seq: 1, remap: { from: guest, to: scope } },
        { change_id: bake.change_id, status: "applied", seq: 2 },
    ]);
    applyPullPage(store, guest, {
        changes: [{ entity: "items", id: cakeId, seq: 2, op: "upsert", row: { name: "Cake" } }],
        cursor: "c2",
        has_more: false,
    });

    assert.equal(provisional(store), 0);
    assert.equal(store.get("SELECT 1 FROM rooms"), null);
});

test("a cancelled row takes the provisional rows that need it", () => {
    const { store } = kitchen();
    const jar = store.insert("items", { scope_id: scope, name: "Jar" });
    let part = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                part = local.insert("parts", { scope_id: scope, item_id: jar, name: "Lid" });
            },
        },
    );

    store.remove("items", jar);

    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [part]), null);
    assert.equal(provisional(store), 0);
    assert.equal(outbox(store).length, 1);
});

test("no write path changes or points into a provisional row", () => {
    const { store } = kitchen();
    pulled(store, "counters", "01JBC59ZQ0B2D4F6H8K0M2P4S2", { name: "Bakes", quantity: 0 }, 3);
    let stock = "";
    let room = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                stock = local.insert("stock", { scope_id: scope, name: "Butter", quantity: "5.000" });
                room = local.insert("scopes", { name: "Provisional scope" });
            },
        },
    );
    const bakes = "01JBC59ZQ0B2D4F6H8K0M2P4S2";

    assert.throws(
        () => store.operate("counters", bakes, "adjust", { delta: 1 }, (local) => local.put("stock", stock, { name: "Ghee" })),
        ProvisionalRowError,
    );
    assert.throws(() => store.insert("rooms", { scope_id: room, name: "Hall" }), ProvisionalRowError);
    assert.equal(outbox(store).length, 1);
});

const tally = "01JBC59ZQ0B2D4F6H8K0M2P4S2";

/** Bakes the cake, which makes provisional stock, then a later change uses some of it. */
function bakeThenUse(store: Store) {
    pulled(store, "counters", tally, { name: "Bakes", quantity: 0 }, 3);
    let butter = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        { provisional: (local) => void (butter = local.insert("stock", { scope_id: scope, name: "Butter", quantity: "5.000" })) },
    );
    store.update(
        "counters",
        tally,
        { name: "Bakes today" },
        {
            provisional: (local) => {
                local.insert("movements", { scope_id: scope, stock_id: butter, delta: "-2.000" });
                local.add("stock", butter, "quantity", "-2.000");
            },
        },
    );
    const butterQuantity = () =>
        store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [butter])?.quantity;
    return { butter, butterQuantity };
}

test("a later provisional result builds on a provisional row", () => {
    const { store } = kitchen();
    const { butterQuantity } = bakeThenUse(store);

    assert.equal(butterQuantity(), "3.000");
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.op]),
        [
            ["items", "upsert"],
            ["counters", "upsert"],
        ],
    );

    // Its own change refused: only its delta goes.
    const [, use] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: use.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Bakes", quantity: 0 } },
    ]);
    assert.equal(butterQuantity(), "5.000");
    assert.equal(provisional(store), 1);
});

test("a delta on a provisional row goes with the row", () => {
    const { store } = kitchen();
    const { butter } = bakeThenUse(store);

    const [bake, use] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: bake.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null } },
    ]);
    assert.equal(store.get("SELECT 1 FROM stock WHERE sync_id = ?", [butter]), null);
    // The later movement stays until its own change settles.
    assert.deepEqual(movements(store).map((row) => row.delta), ["-2.000"]);

    applyAcks(store, scope, [{ change_id: use.change_id, status: "applied", seq: 4 }]);
    applyPullPage(store, scope, { changes: [], cursor: "c4", has_more: false });
    assert.equal(movements(store).length, 0);
    assert.equal(provisional(store), 0);
});

test("both results settle after the server ran both changes", () => {
    const { store } = kitchen();
    const { butter } = bakeThenUse(store);

    const [bake, use] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: bake.change_id, status: "applied", seq: 4 },
        { change_id: use.change_id, status: "applied", seq: 5 },
    ]);
    applyPullPage(store, scope, {
        changes: [{ entity: "stock", id: serverMovement, seq: 6, op: "upsert", row: { name: "Butter", quantity: "3.000" } }],
        cursor: "c6",
        has_more: false,
    });

    assert.equal(store.get("SELECT 1 FROM stock WHERE sync_id = ?", [butter]), null);
    assert.equal(store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [serverMovement])?.quantity, "3.000");
    assert.equal(movements(store).length, 0);
    assert.equal(provisional(store), 0);
});

const serverButter = "01JBC59ZQ0B2D4F6H8K0M2P4T1";

/**
 * The server ran only the bake: its butter arrives, with the use still waiting. Its row may
 * hold another amount or unit than the device's, such as an item the server already had.
 */
function serverRunsOnlyTheBake(store: Store, quantity = "5.000") {
    const [bake] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: bake.change_id, status: "applied", seq: 4 }]);
    applyPullPage(store, scope, {
        changes: [{ entity: "stock", id: serverButter, seq: 4, op: "upsert", row: { name: "Butter", quantity } }],
        cursor: "c4",
        has_more: false,
    });
}

function stockRows(store: Store) {
    return store
        .all<{ sync_id: string; quantity: string }>(
            "SELECT sync_id, quantity FROM stock WHERE name = 'Butter' ORDER BY sync_id",
        )
        .map(({ sync_id, quantity }) => ({ sync_id, quantity }));
}

/** Answers what waits as applied, with nothing more from the server. */
function serverRunsTheRest(store: Store, seq: number) {
    const waiting = store.all<{ change_id: string }>("SELECT change_id FROM sync_outbox ORDER BY position");
    applyAcks(
        store,
        scope,
        waiting.map((change, index) => ({ change_id: change.change_id, status: "applied", seq: seq + index })),
    );
    applyPullPage(store, scope, { changes: [], cursor: `c${seq}`, has_more: false });
}

test("a provisional row stays beside the server's row while a later result on it waits", () => {
    const { store, db } = kitchen();
    const { butter } = bakeThenUse(store);

    // The server's row keeps its own amount; the device never moves a result onto it.
    serverRunsOnlyTheBake(store, "0.005");
    const both = [
        { sync_id: serverButter, quantity: "0.005" },
        { sync_id: butter, quantity: "3.000" },
    ];
    assert.deepEqual(stockRows(store), both);

    // It is stored, so a restart shows the same.
    const reopened = setup({ db }).store;
    assert.deepEqual(stockRows(reopened), both);
    assert.equal(reopened.isProvisional("stock", butter), true);

    // The use settles: the server's rows hold the truth, and the provisional row goes.
    serverRunsTheRest(reopened, 5);
    assert.deepEqual(stockRows(reopened), [{ sync_id: serverButter, quantity: "0.005" }]);
    assert.equal(provisional(reopened), 0);
});

test("refusing one waiting result leaves the others on the kept row", () => {
    const { store, db } = kitchen();
    const { butter } = bakeThenUse(store);
    const hall = "01JBC59ZQ0B2D4F6H8K0M2P4T3";
    pulled(store, "rooms", hall, { name: "Hall" }, 3);
    store.update(
        "rooms",
        hall,
        { name: "Front hall" },
        { provisional: (local) => local.add("stock", butter, "quantity", "-1.000") },
    );

    serverRunsOnlyTheBake(store);
    assert.equal(stockRows(store).find((row) => row.sync_id === butter)?.quantity, "2.000");

    // The use is refused: only its own delta goes. The other change still waits.
    const use = store.get<{ change_id: string }>("SELECT change_id FROM sync_outbox WHERE entity = 'counters'")!;
    applyAcks(store, scope, [
        { change_id: use.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Bakes", quantity: 0 } },
    ]);
    assert.deepEqual(stockRows(store), [
        { sync_id: serverButter, quantity: "5.000" },
        { sync_id: butter, quantity: "4.000" },
    ]);
    assert.equal(outbox(store).length, 1);
    assert.equal(setup({ db }).store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [butter])?.quantity, "4.000");

    serverRunsTheRest(store, 6);
    assert.deepEqual(stockRows(store), [{ sync_id: serverButter, quantity: "5.000" }]);
    assert.equal(provisional(store), 0);
});

test("a kept row keeps the waiting rows that point at it", () => {
    const { store } = kitchen();
    pulled(store, "counters", tally, { name: "Bakes", quantity: 0 }, 3);
    let room = "";
    let tin = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        { provisional: (local) => void (room = local.insert("rooms", { scope_id: scope, name: "Pantry" })) },
    );
    store.update(
        "counters",
        tally,
        { name: "Bakes today" },
        { provisional: (local) => void (tin = local.insert("items", { scope_id: scope, name: "Tin", room_id: room })) },
    );

    const [bake] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: bake.change_id, status: "applied", seq: 4 }]);
    applyPullPage(store, scope, { changes: [], cursor: "c4", has_more: false });
    assert.equal(store.isProvisional("rooms", room), true);
    assert.equal(store.get<{ room_id: string | null }>("SELECT room_id FROM items WHERE sync_id = ?", [tin])?.room_id, room);

    serverRunsTheRest(store, 5);
    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(store.get("SELECT 1 FROM items WHERE sync_id = ?", [tin]), null);
    assert.equal(provisional(store), 0);
});

test("provisional rows that point at a provisional row follow it when it goes", () => {
    const { store } = kitchen();
    let room = "";
    let jar = "";
    let lid = "";
    let hall = "";
    // One change makes a room, an item and a scope; a later change points at them.
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                room = local.insert("rooms", { scope_id: scope, name: "Pantry" });
                jar = local.insert("items", { scope_id: scope, name: "Jar" });
            },
        },
    );
    const extra = store.insert("counters", { scope_id: scope, name: "Bakes" });
    store.update(
        "counters",
        extra,
        { name: "Bakes today" },
        {
            provisional: (local) => {
                const shelf = local.insert("scopes", { name: "Shelf" });
                hall = local.insert("items", { scope_id: scope, name: "Tin", room_id: room });
                lid = local.insert("parts", { scope_id: scope, item_id: jar, name: "Lid" });
                local.insert("rooms", { scope_id: shelf, name: "On the shelf" });
            },
        },
    );

    const [bake] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: bake.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null } },
    ]);

    // An optional link is cleared, a required one takes the row along.
    assert.equal(store.get<{ room_id: string | null }>("SELECT room_id FROM items WHERE sync_id = ?", [hall])?.room_id, null);
    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [lid]), null);
    assert.equal(store.get("SELECT 1 FROM items WHERE sync_id = ?", [jar]), null);
});

test("an operation's local effect cannot point into a provisional row", () => {
    const { store } = kitchen();
    pulled(store, "counters", "01JBC59ZQ0B2D4F6H8K0M2P4S3", { name: "Bakes", quantity: 0 }, 3);
    let room = "";
    let shelf = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                room = local.insert("rooms", { scope_id: scope, name: "Pantry" });
                shelf = local.insert("scopes", { name: "Shelf" });
            },
        },
    );
    const bakes = "01JBC59ZQ0B2D4F6H8K0M2P4S3";

    assert.throws(
        () =>
            store.operate("counters", bakes, "adjust", { delta: 1 }, (local) =>
                local.put("items", "01JBC59ZQ0B2D4F6H8K0M2P4S4", { scope_id: scope, name: "Tin", room_id: room }),
            ),
        ProvisionalRowError,
    );
    assert.throws(
        () =>
            store.operate("counters", bakes, "adjust", { delta: 1 }, (local) =>
                local.put("rooms", "01JBC59ZQ0B2D4F6H8K0M2P4S5", { scope_id: shelf, name: "Hall" }),
            ),
        ProvisionalRowError,
    );
});

test("a scope found through the entity spec is guarded and followed too", () => {
    const db = memoryDatabase();
    migrate(db, [
        ...testMigrations,
        `CREATE TABLE entries (sync_id TEXT PRIMARY KEY NOT NULL, owner_scope TEXT NOT NULL, name TEXT NOT NULL,
            server_id INTEGER, server_seq INTEGER, deleted_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
    ]);
    let id = 0;
    const store = createLocalStore(db, {
        tables: ["scopes", "items", "entries", "counters"],
        entities: {
            scopes: { scope: (row) => row.sync_id as string },
            entries: { scope: (row) => row.owner_scope as string },
            counters: { operations: { adjust: { counters: [{ field: "quantity", delta: "delta" }] } } },
        },
        newId: () => `ID${String(++id).padStart(4, "0")}`,
    });
    db.run("INSERT INTO scopes (sync_id, name, server_seq, created_at, updated_at) VALUES (?, 'Home', 1, 'x', 'x')", [scope]);
    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: cake, seq: 1, op: "upsert", row: { name: "Cake" } },
            { entity: "counters", id: flour, seq: 2, op: "upsert", row: { name: "Bakes", quantity: 0 } },
        ],
        cursor: "c2",
        has_more: false,
    });
    let shelf = "";
    store.update("items", cake, { notes: "Baked" }, { provisional: (local) => { shelf = local.insert("scopes", { name: "Shelf" }); } });

    assert.throws(() => store.insert("entries", { owner_scope: shelf, name: "Jar" }), ProvisionalRowError);
    assert.throws(
        () => store.operate("counters", flour, "adjust", { delta: 1 }, (local) => local.put("entries", "ID9999", { owner_scope: shelf, name: "Jar" })),
        ProvisionalRowError,
    );
    let jar = "";
    store.operate("counters", flour, "adjust", { delta: 1 }, undefined, {
        provisional: (local) => {
            jar = local.insert("entries", { owner_scope: shelf, name: "Jar" });
        },
    });

    const [bake] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: bake.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null } }]);

    assert.equal(store.get("SELECT 1 FROM scopes WHERE sync_id = ?", [shelf]), null);
    assert.equal(store.get("SELECT 1 FROM entries WHERE sync_id = ?", [jar]), null);
});

test("a purged scope takes all its provisional results, also rows in other scopes", async () => {
    const { store } = kitchen();
    let shelf = "";
    let room = "";
    store.update("items", cake, { notes: "Baked" }, {
        provisional: (local) => {
            shelf = local.insert("scopes", { name: "Shelf" });
            room = local.insert("rooms", { scope_id: shelf, name: "Room" });
            local.add("stock", flour, "quantity", "-2.000");
        },
    });

    const outcome = await createSyncClient({
        store,
        scopeId: scope,
        clientId: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        entities: ["items", "rooms", "scopes"],
        transport: async () => ({ status: 404, body: {} }),
    }).sync();

    assert.equal(outcome.status, "gone");
    assert.equal(store.get("SELECT 1 FROM scopes WHERE sync_id = ?", [shelf]), null);
    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(provisional(store), 0);
});

/** A store whose entries find their scope through a lookup of the scope row. */
function lookupScopes() {
    const db = memoryDatabase();
    migrate(db, [
        ...testMigrations,
        `CREATE TABLE entries (sync_id TEXT PRIMARY KEY NOT NULL, owner_scope TEXT NOT NULL, name TEXT NOT NULL,
            server_id INTEGER, server_seq INTEGER, deleted_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
    ]);
    let id = 0;
    const store = createLocalStore(db, {
        tables: ["scopes", "items", "entries"],
        entities: {
            scopes: { scope: (row) => row.sync_id as string },
            entries: {
                scope: (row, reader) =>
                    reader.get<{ sync_id: string }>("SELECT sync_id FROM scopes WHERE sync_id = ?", [row.owner_scope])
                        ?.sync_id ?? null,
            },
        },
        newId: () => `ID${String(++id).padStart(4, "0")}`,
    });
    db.run("INSERT INTO scopes (sync_id, name, server_seq, created_at, updated_at) VALUES (?, 'Home', 1, 'x', 'x')", [scope]);
    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: cake, seq: 1, op: "upsert", row: { name: "Cake" } },
            { entity: "items", id: flour, seq: 2, op: "upsert", row: { name: "Bread" } },
        ],
        cursor: "c2",
        has_more: false,
    });
    return store;
}

test("an update cannot move a row into a provisional scope", () => {
    const store = lookupScopes();
    const entry = store.insert("entries", { owner_scope: scope, name: "Jar" });
    let shelf = "";
    store.update("items", cake, { notes: "Baked" }, { provisional: (local) => { shelf = local.insert("scopes", { name: "Shelf" }); } });

    assert.throws(() => store.update("entries", entry, { owner_scope: shelf }), ProvisionalRowError);
    assert.equal(store.get<{ owner_scope: string }>("SELECT owner_scope FROM entries WHERE sync_id = ?", [entry])?.owner_scope, scope);
});

test("a provisional row follows a provisional scope that its resolver reads", () => {
    const store = lookupScopes();
    let shelf = "";
    let jar = "";
    store.update("items", cake, { notes: "Baked" }, { provisional: (local) => { shelf = local.insert("scopes", { name: "Shelf" }); } });
    store.update("items", flour, { notes: "Baked" }, { provisional: (local) => { jar = local.insert("entries", { owner_scope: shelf, name: "Jar" }); } });

    const [bakeCake] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: bakeCake.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null } }]);

    assert.equal(store.get("SELECT 1 FROM scopes WHERE sync_id = ?", [shelf]), null);
    assert.equal(store.get("SELECT 1 FROM entries WHERE sync_id = ?", [jar]), null);
    assert.equal(store.isProvisional("entries", jar), false);
});

test("a scope resolver that reads the stored row still guards inserts and local effects", () => {
    const db = memoryDatabase();
    migrate(db, [
        ...testMigrations,
        `CREATE TABLE entries (sync_id TEXT PRIMARY KEY NOT NULL, owner_scope TEXT NOT NULL, name TEXT NOT NULL,
            server_id INTEGER, server_seq INTEGER, deleted_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
    ]);
    let id = 0;
    const store = createLocalStore(db, {
        tables: ["scopes", "items", "entries", "counters"],
        entities: {
            scopes: { scope: (row) => row.sync_id as string },
            entries: {
                scope: (row, reader) =>
                    reader.get<{ sync_id: string }>(
                        "SELECT scopes.sync_id FROM entries JOIN scopes ON scopes.sync_id = entries.owner_scope WHERE entries.sync_id = ?",
                        [row.sync_id],
                    )?.sync_id ?? null,
            },
            counters: { operations: { adjust: { counters: [{ field: "quantity", delta: "delta" }] } } },
        },
        newId: () => `ID${String(++id).padStart(4, "0")}`,
    });
    db.run("INSERT INTO scopes (sync_id, name, server_seq, created_at, updated_at) VALUES (?, 'Home', 1, 'x', 'x')", [scope]);
    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: cake, seq: 1, op: "upsert", row: { name: "Cake" } },
            { entity: "counters", id: flour, seq: 2, op: "upsert", row: { name: "Bakes", quantity: 0 } },
        ],
        cursor: "c2",
        has_more: false,
    });
    let shelf = "";
    store.update("items", cake, { notes: "Baked" }, { provisional: (local) => { shelf = local.insert("scopes", { name: "Shelf" }); } });
    store.operate("counters", flour, "adjust", { delta: 1 }, (local) => local.put("entries", "ID9000", { owner_scope: scope, name: "Jar" }));
    const queued = () => store.get<{ count: number }>("SELECT COUNT(*) AS count FROM sync_outbox")?.count;
    const before = queued();

    assert.throws(() => store.insert("entries", { owner_scope: shelf, name: "Tin" }), ProvisionalRowError);
    assert.throws(
        () => store.operate("counters", flour, "adjust", { delta: 1 }, (local) => local.put("entries", "ID9000", { owner_scope: shelf })),
        ProvisionalRowError,
    );
    assert.equal(queued(), before);
    assert.equal(store.get<{ owner_scope: string }>("SELECT owner_scope FROM entries WHERE sync_id = 'ID9000'")?.owner_scope, scope);
});

test("a provisional delta lands only on a counter, which no write can overwrite", () => {
    const { store } = kitchen();

    assert.throws(
        () =>
            store.update("items", cake, { notes: "Baked" }, {
                provisional: (local) => local.add("items", cake, "notes", "1"),
            }),
        /not a counter/,
    );
    assert.throws(() => store.update("stock", flour, { quantity: "7.000" }), /is a counter/);
    // Not through an operation's local effect either.
    pulled(store, "counters", "01JBC59ZQ0B2D4F6H8K0M2P4S6", { name: "Bakes", quantity: 0 }, 9);
    assert.throws(
        () =>
            store.operate("counters", "01JBC59ZQ0B2D4F6H8K0M2P4S6", "adjust", { delta: 1 }, (local) =>
                local.put("stock", flour, { quantity: "7.000" }),
            ),
        /is a counter/,
    );
    assert.equal(quantity(store), "5.000");
});

/** A change that waits for a file, so no drained pull settles its provisional rows. */
function waitingAction(store: Store, write: (local: ProvisionalWriter) => void) {
    store.update("items", cake, { notes: "Baked" }, { dependsOn: ["offline-photo"], provisional: write });
}

test("a pulled tombstone takes the provisional rows that need the deleted row, and clears optional links", () => {
    const { store } = kitchen();
    const jar = "01JBC59ZQ0B2D4F6H8K0M2P4S7";
    const pantry = "01JBC59ZQ0B2D4F6H8K0M2P4S8";
    pulled(store, "items", jar, { name: "Jar" }, 3);
    pulled(store, "rooms", pantry, { name: "Pantry" }, 4);
    let lid = "";
    let tin = "";
    waitingAction(store, (local) => {
        lid = local.insert("parts", { scope_id: scope, item_id: jar, name: "Lid" });
        tin = local.insert("items", { scope_id: scope, name: "Tin", room_id: pantry });
    });
    let heard = 0;
    store.subscribe(["parts", "items"], () => heard++);

    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: jar, seq: 5, op: "delete" },
            { entity: "rooms", id: pantry, seq: 6, op: "delete" },
        ],
        cursor: "c6",
        has_more: false,
    });

    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [lid]), null);
    assert.equal(store.isProvisional("parts", lid), false);
    assert.equal(store.get<{ room_id: string | null }>("SELECT room_id FROM items WHERE sync_id = ?", [tin])?.room_id, null);
    assert.equal(store.isProvisional("items", tin), true);
    assert.equal(heard, 1);
});

test("a resync sweep takes the provisional rows that need a swept row", async () => {
    const { store } = kitchen();
    const jar = "01JBC59ZQ0B2D4F6H8K0M2P4S9";
    pulled(store, "items", jar, { name: "Jar" }, 3);
    let lid = "";
    waitingAction(store, (local) => {
        lid = local.insert("parts", { scope_id: scope, item_id: jar, name: "Lid" });
    });
    let request = 0;
    const transport: SyncTransport = async () => {
        request++;
        if (request === 1) return { status: 409, body: { error: { code: "resync_required", message: "Again." } } };
        return {
            status: 200,
            body: {
                protocol: 1,
                acks: [],
                changes: [{ entity: "items", id: cake, seq: 9, op: "upsert", row: { name: "Cake" } }],
                cursor: "c9",
                has_more: false,
                server_time: "x",
            },
        };
    };

    await createSyncClient({
        store,
        scopeId: scope,
        clientId: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        entities: ["items", "parts"],
        transport,
        isUploaded: () => false,
    }).sync();

    assert.equal(store.get("SELECT 1 FROM items WHERE sync_id = ?", [jar]), null);
    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [lid]), null);
});

test("an acked delete and a refused insert take the provisional rows that need them", () => {
    const { store } = kitchen();
    const jar = "01JBC59ZQ0B2D4F6H8K0M2P4T2";
    pulled(store, "items", jar, { name: "Jar" }, 3);
    const box = store.insert("items", { scope_id: scope, name: "Box" });
    let lid = "";
    let flap = "";
    waitingAction(store, (local) => {
        lid = local.insert("parts", { scope_id: scope, item_id: jar, name: "Lid" });
        flap = local.insert("parts", { scope_id: scope, item_id: box, name: "Flap" });
    });
    store.remove("items", jar);

    const batch = takeBatch(store, scope, { isUploaded: () => false });
    const changeFor = (id: string) => batch.find((change) => change.id === id)!.change_id;
    applyAcks(store, scope, [
        { change_id: changeFor(jar), status: "applied", seq: 4 },
        { change_id: changeFor(box), status: "rejected", error: { code: "validation", message: "No." }, current: null },
    ]);

    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [lid]), null);
    assert.equal(store.get("SELECT 1 FROM parts WHERE sync_id = ?", [flap]), null);
});

test("removing a new row keeps it when a new row that needs it carries an action", () => {
    const { store } = kitchen();
    const jar = store.insert("items", { scope_id: scope, name: "Jar" });
    const lid = store.insert(
        "parts",
        { scope_id: scope, item_id: jar, name: "Lid" },
        { provisional: (local) => local.add("stock", flour, "quantity", "-1.000") },
    );

    store.remove("parts", lid);
    store.remove("items", jar);

    // The lid's action needs the jar on the server, so both go, then both deletes.
    assert.equal(quantity(store), "4.000");
    assert.deepEqual(
        takeBatch(store, scope).map((change) => [change.entity, change.op]),
        [
            ["items", "upsert"],
            ["parts", "upsert"],
            ["parts", "delete"],
            ["items", "delete"],
        ],
    );
});
