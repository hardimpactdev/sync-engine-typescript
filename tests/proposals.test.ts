import assert from "node:assert/strict";
import test from "node:test";
import { createSyncClient, type SyncTransport } from "../src/client.ts";
import { ProvisionalRowError } from "../src/store.ts";
import { applyAcks, applyPullPage, noteFeatures, takeBatch } from "../src/sync.ts";
import { outbox, pulled, scope, setup, type Store } from "./setup.ts";

// Proposed rows (protocol §5.5). An action the server runs, here a bake that shelves the
// cake in a new room, makes rows on the server. The device shows the room at once under an
// id it proposes; the server adopts that id, or names the room it used. So the room takes
// writes and references at once, and the server's answer replaces nothing.

const cake = "01JBC59ZQ0B2D4F6H8K0M2P4R7";
const serverRoom = "01JBC59ZQ0B2D4F6H8K0M2P4V1";

function kitchen({ proposals = true } = {}) {
    const { store, db } = setup();
    pulled(store, "items", cake, { name: "Cake", notes: null, room_id: null }, 1);
    if (proposals) noteFeatures(store, scope, ["proposals"]);
    return { store, db };
}

/** Bakes the cake; the server shelves it in a new room, which the device shows at once. */
function bake(store: Store, notes = "Baked") {
    let room = "";
    store.update(
        "items",
        cake,
        { notes },
        { provisional: (local) => void (room = local.insert("rooms", { scope_id: scope, name: "Pantry" }, { key: "room" })) },
    );
    return room;
}

function provisional(store: Store) {
    return store.get<{ count: number }>("SELECT COUNT(*) AS count FROM sync_provisional")!.count;
}

function roomOf(store: Store, item: string) {
    return store.get<{ room_id: string | null }>("SELECT room_id FROM items WHERE sync_id = ?", [item])?.room_id;
}

/** The server applies the bake and answers which room it made or used. */
function serverBakes(store: Store, made: Record<string, string> | undefined, seq = 5) {
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: change.change_id, status: "applied", seq, ...(made ? { made } : {}) }]);
    return change;
}

test("before the server announces proposals, a change proposes nothing and its row stays read-only", () => {
    const { store } = kitchen({ proposals: false });
    const room = bake(store);

    const [change] = takeBatch(store, scope);
    assert.equal(change.proposals, undefined);
    assert.equal(store.isProposed("rooms", room), false);
    assert.throws(() => store.update("rooms", room, { name: "Larder" }), ProvisionalRowError);
});

test("each answer says whether the server takes proposals", () => {
    const { store } = kitchen();
    assert.equal(store.isProposed("rooms", bake(store)), true);
    noteFeatures(store, scope, []);
    assert.equal(store.isProposed("rooms", bake(store, "Baked again")), false);
});

test("a proposed row takes writes, references and links at once, after its change", () => {
    const { store } = kitchen();
    const room = bake(store);

    store.update("rooms", room, { name: "Larder" });
    const jar = store.insert("items", { scope_id: scope, name: "Jar", room_id: room });
    assert.equal(store.isProposed("rooms", room), true);

    const batch = takeBatch(store, scope);
    assert.deepEqual(
        batch.map((change) => [change.entity, change.op, change.proposals ?? null]),
        [
            ["items", "upsert", { room }],
            ["rooms", "upsert", null],
            ["items", "upsert", null],
        ],
    );
    assert.deepEqual(batch[2].fields, { name: "Jar", room_id: room });
    assert.equal(roomOf(store, jar), room);
});

test("a row made under its proposed id is the server's: it stays, and the pull brings its values", () => {
    const { store } = kitchen();
    const room = bake(store);

    serverBakes(store, { room });
    assert.equal(store.isProvisional("rooms", room), false);
    // Known to the server, with no seq of its own until its row arrives.
    assert.equal(store.get<{ server_seq: number }>("SELECT server_seq FROM rooms WHERE sync_id = ?", [room])?.server_seq, 0);

    applyPullPage(store, scope, {
        changes: [{ entity: "rooms", id: room, seq: 6, op: "upsert", row: { name: "Pantry shelf" } }],
        cursor: "c6",
        has_more: false,
    });
    assert.equal(store.get<{ name: string }>("SELECT name FROM rooms WHERE sync_id = ?", [room])?.name, "Pantry shelf");
    assert.equal(provisional(store), 0);
});

test("a row the server made under another id moves there, with the unsent changes that need it", () => {
    const { store } = kitchen();
    const room = bake(store);
    store.update("rooms", room, { name: "Larder" });
    const jar = store.insert("items", { scope_id: scope, name: "Jar", room_id: room });
    // Only the bake is sent; the rename and the jar wait.
    const [first] = takeBatch(store, scope, { limit: 1 });
    applyAcks(store, scope, [{ change_id: first.change_id, status: "applied", seq: 5, made: { room: serverRoom } }]);

    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(store.get<{ name: string }>("SELECT name FROM rooms WHERE sync_id = ?", [serverRoom])?.name, "Larder");
    assert.equal(roomOf(store, jar), serverRoom);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.row_id, entry.payload]),
        [
            ["rooms", serverRoom, { name: "Larder" }],
            ["items", jar, { name: "Jar", room_id: serverRoom }],
        ],
    );
});

test("a row the server used already takes the proposed row's place", () => {
    const { store } = kitchen();
    pulled(store, "rooms", serverRoom, { name: "Pantry" }, 2);
    const room = bake(store);
    const jar = store.insert("items", { scope_id: scope, name: "Jar", room_id: room });

    serverBakes(store, { room: serverRoom });
    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(roomOf(store, jar), serverRoom);
    assert.equal(store.isProvisional("rooms", serverRoom), false);
});

test("a key the server did not make takes its row and the unsent changes that need it", () => {
    const { store } = kitchen();
    const room = bake(store);
    store.update("rooms", room, { name: "Larder" });
    const jar = store.insert("items", { scope_id: scope, name: "Jar", room_id: room });
    const [first] = takeBatch(store, scope, { limit: 1 });
    applyAcks(store, scope, [{ change_id: first.change_id, status: "applied", seq: 5, made: {} }]);

    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    // The rename goes; the jar stays, without the link (an optional one).
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.payload]),
        [["items", { name: "Jar", room_id: null }]],
    );
    assert.equal(roomOf(store, jar), null);
    assert.equal(provisional(store), 0);
});

test("an answer without made, from an older server, takes the proposed rows along", () => {
    const { store } = kitchen();
    const room = bake(store);
    store.update("rooms", room, { name: "Larder" });
    const [first] = takeBatch(store, scope, { limit: 1 });
    applyAcks(store, scope, [{ change_id: first.change_id, status: "applied", seq: 5 }]);

    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(outbox(store).length, 0);
});

test("a refused change takes its proposed rows and their unsent changes along", () => {
    const { store } = kitchen();
    const room = bake(store);
    store.update("rooms", room, { name: "Larder" });
    const [first] = takeBatch(store, scope, { limit: 1 });
    applyAcks(store, scope, [
        { change_id: first.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null, room_id: null } },
    ]);

    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]), null);
    assert.equal(outbox(store).length, 0);
    assert.equal(provisional(store), 0);
});

test("removing a proposed row before its change is answered deletes it after the change", () => {
    const { store } = kitchen();
    const room = bake(store);
    store.remove("rooms", room);

    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.op]),
        [
            ["items", "upsert"],
            ["rooms", "delete"],
        ],
    );
    assert.notEqual(store.get<{ deleted_at: string | null }>("SELECT deleted_at FROM rooms WHERE sync_id = ?", [room])?.deleted_at, null);

    serverBakes(store, { room }, 5);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.row_id, entry.op]),
        [["rooms", room, "delete"]],
    );
});

test("an action that proposes rows stays whole: a later write never merges into it", () => {
    const { store } = kitchen();
    const room = bake(store);
    // Undoing the bake would merge into it, and the server would make no room; the rename
    // that needs the room would go too. So each goes as its own change.
    store.update("items", cake, { notes: null });
    store.update("rooms", room, { name: "Larder" });

    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.payload]),
        [
            ["items", { notes: "Baked" }],
            ["items", { notes: null }],
            ["rooms", { name: "Larder" }],
        ],
    );
});

test("a key used again within one change moves to the newer row", () => {
    const { store } = kitchen();
    let first = "";
    let second = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        {
            provisional: (local) => {
                first = local.insert("rooms", { scope_id: scope, name: "Pantry" }, { key: "room" });
                second = local.insert("rooms", { scope_id: scope, name: "Shelf" }, { key: "room" });
            },
        },
    );

    const [change] = takeBatch(store, scope);
    assert.deepEqual(change.proposals, { room: second });
    assert.equal(store.isProposed("rooms", second), true);
    assert.equal(store.isProposed("rooms", first), false);
});

test("a duplicate ack repeats made", () => {
    const { store } = kitchen();
    const room = bake(store);
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: change.change_id, status: "duplicate", seq: 5, made: { room } }]);
    assert.equal(store.isProvisional("rooms", room), false);
    assert.equal(store.get("SELECT 1 FROM rooms WHERE sync_id = ?", [room]) !== null, true);
});

test("a sync run learns the feature, sends proposals, and adopts what the server made", async () => {
    const { store } = kitchen({ proposals: false });
    const bodies: { changes: { proposals?: Record<string, string> }[] }[] = [];
    let seq = 10;
    const transport: SyncTransport = async (body) => {
        bodies.push(body);
        return {
            status: 200,
            body: {
                protocol: 1,
                acks: body.changes.map((change) => ({
                    change_id: change.change_id,
                    status: "applied",
                    seq: ++seq,
                    ...(change.proposals ? { made: change.proposals } : {}),
                })),
                changes: [],
                cursor: `c${seq}`,
                has_more: false,
                server_time: "x",
                features: ["proposals"],
            },
        };
    };
    const client = createSyncClient({
        store,
        scopeId: scope,
        clientId: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        entities: ["items", "rooms"],
        transport,
    });

    await client.sync();
    const room = bake(store);
    store.update("rooms", room, { name: "Larder" });
    await client.sync();

    const sent = bodies.at(-1)!.changes;
    assert.deepEqual(sent[0].proposals, { room });
    assert.equal(store.isProvisional("rooms", room), false);
    assert.equal(store.get<{ name: string }>("SELECT name FROM rooms WHERE sync_id = ?", [room])?.name, "Larder");
    assert.equal(outbox(store).length, 0);
});

// A delta counted in a unit, such as grams, lands in what its row holds now, such as
// kilograms: the server may hold the row in another unit, or a proposed row may move to an
// existing row in another one. Here stock is counted in pieces or in dozens, by its name.
const dozens = (name: unknown) => String(name).endsWith("(dozens)");
const convert = (_field: string, delta: unknown, basis: string, target: Record<string, unknown>) => {
    const pieces = basis === "dozen" ? Number(delta) * 12 : Number(delta);
    return (dozens(target.name) ? pieces / 12 : pieces).toFixed(3);
};
const eggs = "01JBC59ZQ0B2D4F6H8K0M2P4V2";

test("a provisional delta lands in what its row holds, also after the server changed it", () => {
    const { store } = setup({ convert });
    pulled(store, "items", cake, { name: "Cake", notes: null, room_id: null }, 1);
    pulled(store, "stock", eggs, { name: "Eggs", quantity: "24.000" }, 2);
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        { provisional: (local) => local.add("stock", eggs, "quantity", "-6.000", "piece") },
    );
    const quantity = () => store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [eggs])?.quantity;
    assert.equal(quantity(), "18.000");

    // The web counts eggs in dozens now; the waiting use still takes six eggs.
    pulled(store, "stock", eggs, { name: "Eggs (dozens)", quantity: "2.000" }, 3);
    assert.equal(quantity(), "1.500");

    // Refused: exactly the six eggs come back.
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: change.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null, room_id: null } },
    ]);
    assert.equal(quantity(), "2.000");
});

test("a queued operation's delta lands in its row's unit, also on a row it moved to", () => {
    const { store } = setup({ convert });
    pulled(store, "items", cake, { name: "Cake", notes: null, room_id: null }, 1);
    noteFeatures(store, scope, ["proposals"]);
    let box = "";
    store.update(
        "items",
        cake,
        { notes: "Bought eggs" },
        { provisional: (local) => void (box = local.insert("stock", { scope_id: scope, name: "Eggs", quantity: "12.000" }, { key: "stock" })) },
    );
    // A correction on the proposed box, counted in pieces.
    const correction = "01JBC59ZQ0B2D4F6H8K0M2P4V3";
    store.operate("movements", correction, "record", { stock_id: box, delta: "6.000", unit: "piece" }, (local) =>
        local.put("movements", correction, { scope_id: scope, stock_id: box, delta: "6.000" }),
    );
    assert.equal(store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [box])?.quantity, "18.000");

    // The server put the eggs on its box counted in dozens; the correction lands there in dozens.
    const [first] = takeBatch(store, scope, { limit: 1 });
    applyAcks(store, scope, [{ change_id: first.change_id, status: "applied", seq: 5, made: { stock: eggs } }]);
    applyPullPage(store, scope, {
        changes: [{ entity: "stock", id: eggs, seq: 6, op: "upsert", row: { name: "Eggs (dozens)", quantity: "3.000" } }],
        cursor: "c6",
        has_more: false,
    });
    assert.equal(store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [eggs])?.quantity, "3.500");
});

test("an action keeps its place after the changes its result builds on", () => {
    const { store } = setup();
    const jar = store.insert("items", { scope_id: scope, name: "Jar" });
    // A later change makes the stock the action's result lands on.
    const flour = store.insert("stock", { scope_id: scope, name: "Flour" });
    store.update(
        "items",
        jar,
        { notes: "Filled" },
        { provisional: (local) => local.add("stock", flour, "quantity", "1.000") },
    );

    // Merged into the jar's insert, the action would reach the server before the flour.
    assert.deepEqual(
        outbox(store).map((entry) => [entry.entity, entry.payload]),
        [
            ["items", { name: "Jar" }],
            ["stock", { name: "Flour" }],
            ["items", { notes: "Filled" }],
        ],
    );
});

test("a proposed row whose own change fails passes to the next waiting change with a result on it", () => {
    const { store } = kitchen();
    const tally = "01JBC59ZQ0B2D4F6H8K0M2P4V4";
    pulled(store, "counters", tally, { name: "Bakes", quantity: 0 }, 2);
    let butter = "";
    store.update(
        "items",
        cake,
        { notes: "Baked" },
        { provisional: (local) => void (butter = local.insert("stock", { scope_id: scope, name: "Butter", quantity: "5.000" }, { key: "stock" })) },
    );
    const [bake] = takeBatch(store, scope);
    // A second purchase of butter waits; its result lands on the butter the bake makes.
    store.update("counters", tally, { name: "Bakes today" }, { provisional: (local) => local.add("stock", butter, "quantity", "2.000") });

    applyAcks(store, scope, [
        { change_id: bake.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null, room_id: null } },
    ]);

    // The butter stays, with only the second purchase on it, and that change now proposes it.
    assert.equal(store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE sync_id = ?", [butter])?.quantity, "2.000");
    assert.equal(store.isProposed("stock", butter), true);
    const [purchase] = takeBatch(store, scope);
    assert.deepEqual(purchase.proposals, { stock: butter });

    applyAcks(store, scope, [{ change_id: purchase.change_id, status: "applied", seq: 6, made: { stock: butter } }]);
    assert.equal(store.isProvisional("stock", butter), false);
});

test("a result built on another change's row that did not come true goes whole", () => {
    const { store } = setup({ builtOn: ["room_id"] });
    pulled(store, "items", cake, { name: "Cake", notes: null, room_id: null }, 1);
    pulled(store, "stock", "01JBC59ZQ0B2D4F6H8K0M2P4R6", { name: "Flour", quantity: "5.000" }, 2);
    const tally = "01JBC59ZQ0B2D4F6H8K0M2P4V5";
    pulled(store, "counters", tally, { name: "Bakes", quantity: 0 }, 3);
    noteFeatures(store, scope, ["proposals"]);
    const room = bake(store);
    const [first] = takeBatch(store, scope);
    // A later change, computed from the bake's room: a tin in it, and flour it takes.
    let tin = "";
    store.update(
        "counters",
        tally,
        { name: "Bakes today" },
        {
            provisional: (local) => {
                tin = local.insert("items", { scope_id: scope, name: "Tin", room_id: room });
                local.add("stock", "01JBC59ZQ0B2D4F6H8K0M2P4R6", "quantity", "-1.000");
            },
        },
    );

    applyAcks(store, scope, [
        { change_id: first.change_id, status: "rejected", error: { code: "validation", message: "No." }, current: { name: "Cake", notes: null, room_id: null } },
    ]);

    // The bake's room never came true, so the tin and the flour it took go; the change waits.
    assert.equal(store.get("SELECT 1 FROM items WHERE sync_id = ?", [tin]), null);
    assert.equal(store.get<{ quantity: string }>("SELECT quantity FROM stock WHERE name = 'Flour'")?.quantity, "5.000");
    assert.deepEqual(outbox(store).map((entry) => entry.entity), ["counters"]);
    assert.equal(provisional(store), 0);
});
