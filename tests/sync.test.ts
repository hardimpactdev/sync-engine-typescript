import assert from "node:assert/strict";
import test from "node:test";
import type { Row } from "../src/database.ts";
import type { Ack, SyncResponse, WireChange } from "../src/protocol.ts";
import {
    applyAcks,
    applyPullPage,
    cursorOf,
    returnToPending,
    takeBatch,
} from "../src/sync.ts";
import { fixture, scope, outbox, pulled, setup, type Store } from "./setup.ts";

const item = "01JBC59ZQ0B2D4F6H8K0M2P4R6";
const milk = "01JBC5A3X5G9J1L3N5Q7S9V1X4";
const serverMilk = "01JAQ0M8N6P4R2T0V8X6Z4B2D0";
const hall = "01J9YY7Q6R5S4T3V2W1X0Y9Z8A";
const kitchen = "01J9ZZ0A1B2C3D4E5F6G7H8J9K";

type PushRequest = {
    protocol: 1;
    client_id: string;
    cursor: string | null;
    entities: string[];
    changes: WireChange[];
};

function row(store: Store, table: string, id: string) {
    return store.get<Row>(`SELECT * FROM ${table} WHERE sync_id = ?`, [id]);
}

/** The phone of the fixtures: one item and one room pulled, then three edits. */
function fixturePhone() {
    const phone = setup({
        ids: ["01JBC5A2W4F8H0K2M4P6R8T0V2", "01JBC5A3X5G9J1L3N5Q7S9V1X3", "01JBC5A4Y6H0K2M4P6R8T0V2X4"],
    });
    const { store, at } = phone;
    applyPullPage(store, scope, {
        changes: [
            { entity: "rooms", id: hall, seq: 4100, op: "upsert", row: { name: "Hall" } },
            { entity: "rooms", id: kitchen, seq: 4101, op: "upsert", row: { name: "Kitchen" } },
            {
                entity: "items",
                id: item,
                seq: 4182,
                op: "upsert",
                row: { name: "Dish washer", room_id: null, notes: null },
            },
        ],
        cursor: "opaque-cursor-1",
        has_more: false,
    });

    at("2026-10-20T09:58:11.204Z");
    store.update("items", item, { name: "Dishwasher", room_id: kitchen }, { dependsOn: [] });
    at("2026-10-20T09:58:13.019Z");
    store.operate("counters", milk, "adjust", { name: "Milk", delta: 1 }, (local) =>
        local.put("counters", milk, { scope_id: scope, name: "Milk" }),
    );
    at("2026-10-20T09:58:14.502Z");
    store.remove("rooms", hall);
    return phone;
}

test("the outbox becomes the push request of the fixture", () => {
    const { store } = fixturePhone();
    const request: PushRequest = {
        protocol: 1,
        client_id: "01JBC4Y0R8T5D8M4QKX3N2V6ZA",
        cursor: cursorOf(store, scope),
        entities: ["items", "rooms", "counters"],
        changes: takeBatch(store, scope),
    };
    assert.deepEqual(request, fixture<PushRequest>("push-request.json"));
    assert.deepEqual(
        outbox(store).map((entry) => [entry.state, entry.attempts]),
        [
            ["sending", 1],
            ["sending", 1],
            ["sending", 1],
        ],
    );
});

test("the push response of the fixture clears the outbox and lands the server rows", () => {
    const { store } = fixturePhone();
    takeBatch(store, scope);
    const response = fixture<SyncResponse>("push-response.json");

    const acks = applyAcks(store, scope, response.acks);
    applyPullPage(store, scope, response);

    assert.deepEqual(acks, {
        applied: response.acks.map((ack) => ack.change_id),
        rejected: [],
        remapped: [{ entity: "counters", from: milk, to: serverMilk }],
    });
    assert.deepEqual(outbox(store), []);
    assert.equal(row(store, "rooms", hall), null);
    assert.equal(row(store, "counters", milk), null);
    assert.equal(row(store, "counters", serverMilk)?.quantity, 3);
    assert.equal(row(store, "counters", serverMilk)?.server_seq, 4191);
    const dishwasher = row(store, "items", item);
    assert.equal(dishwasher?.name, "Dishwasher");
    assert.equal(dishwasher?.room_id, kitchen);
    assert.equal(dishwasher?.server_seq, 4190);
    assert.equal(cursorOf(store, scope), "opaque-cursor-2");
    assert.equal(
        store.get<{ snapshot_complete: number }>("SELECT snapshot_complete FROM sync_state")
            ?.snapshot_complete,
        1,
    );
});

test("a rejected change keeps its values and the row returns to the server's version", () => {
    const { store } = setup({ ids: ["01JBC5A2W4F8H0K2M4P6R8T0V2"] });
    pulled(store, "items", item, { name: "Dishwasher", room_id: kitchen, notes: null }, 4182);
    store.update("items", item, { name: "" });
    takeBatch(store, scope);
    store.update("items", item, { notes: "Keep the manual" });

    const rejected = fixture<Ack>("rejected-ack.json");
    const result = applyAcks(store, scope, [rejected]);

    assert.deepEqual(result.rejected, [
        {
            change_id: rejected.change_id,
            entity: "items",
            id: item,
            op: "upsert",
            code: "validation",
            message: "The name field is required.",
        },
    ]);
    const kept = store.get<Row>("SELECT * FROM sync_rejected");
    assert.equal(kept?.payload, JSON.stringify({ name: "" }));
    assert.equal(kept?.base_seq, 4182);
    assert.deepEqual(JSON.parse(String(kept?.error_fields)), rejected.error?.fields);
    assert.deepEqual(JSON.parse(String(kept?.current)), rejected.current);

    // The other pending change is replayed on top of the restored row.
    const restored = row(store, "items", item);
    assert.equal(restored?.name, "Dishwasher");
    assert.equal(restored?.notes, "Keep the manual");
    assert.deepEqual(
        outbox(store).map((entry) => entry.payload),
        [{ notes: "Keep the manual" }],
    );
});

test("a rejected insert with no server row removes the row and keeps later edits", () => {
    const { store } = setup();
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    const [insert] = takeBatch(store, scope);
    store.update("items", lamp, { notes: "Blue" });

    const result = applyAcks(store, scope, [
        {
            change_id: insert.change_id,
            status: "rejected",
            error: { code: "validation", message: "No." },
            current: null,
        },
    ]);
    assert.deepEqual(
        result.rejected.map((change) => change.code),
        ["validation", "parent_rejected"],
    );
    assert.equal(row(store, "items", lamp), null);
    assert.deepEqual(outbox(store), []);
    assert.equal(store.all("SELECT * FROM sync_rejected").length, 2);
});

test("a rejected operation takes its delta back off the other row", () => {
    const { store } = setup();
    pulled(store, "stock", "RICE", { name: "Rice", quantity: "2.000" }, 7);
    const movement = store.newId();
    store.operate("movements", movement, "record", { stock_id: "RICE", delta: "-0.500" }, (local) =>
        local.put("movements", movement, { scope_id: scope, stock_id: "RICE", delta: "-0.500" }),
    );
    assert.equal(row(store, "stock", "RICE")?.quantity, "1.500");
    const [change] = takeBatch(store, scope);

    applyAcks(store, scope, [
        {
            change_id: change.change_id,
            status: "rejected",
            error: { code: "validation", message: "Too much." },
            current: null,
        },
    ]);
    assert.equal(row(store, "stock", "RICE")?.quantity, "2.000");
    assert.equal(row(store, "movements", movement), null);
});

test("a rejected delete brings the row back", () => {
    const { store } = setup();
    pulled(store, "rooms", hall, { name: "Hall" }, 4100);
    store.remove("rooms", hall);
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [
        {
            change_id: change.change_id,
            status: "rejected",
            error: { code: "forbidden", message: "No." },
            current: { name: "Hall" },
        },
    ]);
    assert.equal(row(store, "rooms", hall)?.deleted_at, null);
});

test("an acked delete removes the hidden row, and an unknown ack changes nothing", () => {
    const { store } = setup();
    pulled(store, "rooms", hall, { name: "Hall" }, 4100);
    store.remove("rooms", hall);
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [
        { change_id: "01JBC5A2W4F8H0K2M4P6R8T0ZZ", status: "applied", seq: 1 },
        { change_id: change.change_id, status: "duplicate", seq: 4200 },
    ]);
    assert.equal(row(store, "rooms", hall), null);
    assert.deepEqual(outbox(store), []);
});

test("an acked insert makes the row known, so its delete is sent", () => {
    const { store } = setup();
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp" });
    const [change] = takeBatch(store, scope);
    applyAcks(store, scope, [{ change_id: change.change_id, status: "applied", seq: 12 }]);
    store.remove("items", lamp);
    assert.deepEqual(
        outbox(store).map((entry) => [entry.op, entry.base_seq]),
        [["delete", 12]],
    );
});

test("a remap rewrites references in the tables and in unsent changes", () => {
    const { store } = setup();
    const attic = store.insert("rooms", { scope_id: scope, name: "Attic" });
    const [roomChange] = takeBatch(store, scope);
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp", room_id: attic });
    const lampEntry = outbox(store)[1];

    applyAcks(store, scope, [
        {
            change_id: roomChange.change_id,
            status: "applied",
            seq: 20,
            remap: { from: attic, to: "SERVER-ATTIC" },
        },
    ]);
    assert.equal(row(store, "rooms", attic), null);
    assert.equal(row(store, "rooms", "SERVER-ATTIC")?.server_seq, 20);
    assert.equal(row(store, "items", lamp)?.room_id, "SERVER-ATTIC");
    assert.deepEqual(outbox(store), [{ ...lampEntry, payload: { name: "Lamp", room_id: "SERVER-ATTIC" } }]);
});

test("a remap onto a row the phone has merges the two and leaves sent changes as they are", () => {
    const { store } = setup();
    pulled(store, "rooms", "SERVER-ATTIC", { name: "Attic" }, 5);
    const attic = store.insert("rooms", { scope_id: scope, name: "attic" });
    const lamp = store.insert("items", { scope_id: scope, name: "Lamp", room_id: attic });
    const [roomChange] = takeBatch(store, scope);

    applyAcks(store, scope, [
        { change_id: roomChange.change_id, status: "applied", seq: 6, remap: { from: attic, to: "SERVER-ATTIC" } },
    ]);
    assert.equal(row(store, "rooms", attic), null);
    assert.equal(row(store, "rooms", "SERVER-ATTIC")?.name, "Attic");
    assert.equal(row(store, "items", lamp)?.room_id, "SERVER-ATTIC");
    // The item's insert was sent in the same batch, so a resend must stay byte-identical.
    assert.deepEqual(outbox(store)[0].payload, { name: "Lamp", room_id: attic });
});

test("a pull keeps pending fields, adds pending deltas and keeps pending deletes hidden", () => {
    const { store } = setup();
    pulled(store, "items", item, { name: "Dishwasher", notes: null }, 1);
    pulled(store, "counters", milk, { name: "Milk", quantity: 2 }, 2);
    pulled(store, "stock", "RICE", { name: "Rice", quantity: "1.000" }, 3);
    pulled(store, "rooms", hall, { name: "Hall" }, 4);

    store.update("items", item, { notes: "Mine" });
    store.operate("counters", milk, "adjust", { delta: 1 });
    const movement = store.newId();
    store.operate("movements", movement, "record", { stock_id: "RICE", delta: "0.500" }, (local) =>
        local.put("movements", movement, { scope_id: scope, stock_id: "RICE", delta: "0.500" }),
    );
    store.remove("rooms", hall);
    takeBatch(store, scope, { limit: 1 }); // a sent change is still pending until acked

    applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: item, seq: 10, op: "upsert", row: { name: "Dish washer", notes: "Theirs" } },
            { entity: "counters", id: milk, seq: 11, op: "upsert", row: { name: "Milk", quantity: 5 } },
            { entity: "stock", id: "RICE", seq: 12, op: "upsert", row: { name: "Rice", quantity: "4.000" } },
            { entity: "rooms", id: hall, seq: 13, op: "upsert", row: { name: "Big hall" } },
        ],
        cursor: "c2",
        has_more: true,
    });

    assert.equal(row(store, "items", item)?.name, "Dish washer");
    assert.equal(row(store, "items", item)?.notes, "Mine");
    assert.equal(row(store, "items", item)?.server_seq, 10);
    assert.equal(row(store, "counters", milk)?.quantity, 6);
    assert.equal(row(store, "stock", "RICE")?.quantity, "4.500");
    assert.equal(row(store, "rooms", hall)?.name, "Big hall");
    assert.ok(row(store, "rooms", hall)?.deleted_at);
    assert.equal(outbox(store).length, 4);
    assert.equal(cursorOf(store, scope), "c2");
});

test("a pulled tombstone deletes the row and drops its pending changes", () => {
    const { store } = setup();
    pulled(store, "items", item, { name: "Dishwasher" }, 1);
    pulled(store, "rooms", hall, { name: "Hall" }, 2);
    store.update("items", item, { notes: "Mine" });

    const result = applyPullPage(store, scope, {
        changes: [
            { entity: "items", id: item, seq: 3, op: "delete" },
            { entity: "rooms", id: hall, seq: 4, op: "delete" },
            { entity: "gardens", id: "G", seq: 5, op: "upsert", row: { name: "Back" } },
        ],
        cursor: "c3",
        has_more: false,
    });
    assert.deepEqual(result.deletedElsewhere, [{ entity: "items", id: item }]);
    assert.equal(row(store, "items", item), null);
    assert.equal(row(store, "rooms", hall), null);
    assert.deepEqual(outbox(store), []);
    assert.equal(cursorOf(store, scope), "c3");
});

test("a pull skips a version older than the one the phone has", () => {
    const { store } = setup();
    pulled(store, "items", item, { name: "New" }, 9);
    pulled(store, "items", item, { name: "Old" }, 8);
    assert.equal(row(store, "items", item)?.name, "New");
});

test("a page and its cursor land together or not at all", () => {
    const { store } = setup();
    applyPullPage(store, scope, { changes: [], cursor: "c1", has_more: false });
    assert.throws(() =>
        applyPullPage(store, scope, {
            changes: [
                { entity: "rooms", id: hall, seq: 2, op: "upsert", row: { name: "Hall" } },
                { entity: "rooms", id: "BROKEN", seq: 3, op: "upsert", row: { name: null } },
            ],
            cursor: "c2",
            has_more: false,
        }),
    );
    assert.equal(cursorOf(store, scope), "c1");
    assert.equal(row(store, "rooms", hall), null);
});

test("a batch holds at most 100 changes in outbox order", () => {
    const { store } = setup();
    store.transaction(() => {
        for (let index = 0; index < 105; index++) {
            store.insert("rooms", { scope_id: scope, name: `Room ${index}` });
        }
    });
    const batch = takeBatch(store, scope, { limit: 500 });
    assert.equal(batch.length, 100);
    assert.equal(batch[99].fields?.name, "Room 99");
});

test("changes wait for their files, and so do later changes to that row and rows pointing at it", () => {
    const { store } = setup();
    const attic = store.insert("rooms", { scope_id: scope, name: "Attic" }, { dependsOn: ["photo-1"] });
    store.update("rooms", attic, { name: "Loft" });
    const hallway = store.insert("rooms", { scope_id: scope, name: "Hallway" });
    store.insert("items", { scope_id: scope, name: "Lamp", room_id: attic });
    store.insert("items", { scope_id: scope, name: "Rug", room_id: hallway });

    const uploaded = new Set<string>();
    const first = takeBatch(store, scope, { isUploaded: (file) => uploaded.has(file) });
    assert.deepEqual(
        first.map((change) => change.fields?.name),
        ["Hallway", "Rug"],
    );
    applyAcks(
        store,
        scope,
        first.map((change) => ({ change_id: change.change_id, status: "applied" as const, seq: 1 })),
    );

    uploaded.add("photo-1");
    const second = takeBatch(store, scope, { isUploaded: (file) => uploaded.has(file) });
    assert.deepEqual(
        second.map((change) => [change.fields?.name, change.depends_on]),
        [
            ["Loft", ["photo-1"]],
            ["Lamp", undefined],
        ],
    );
});

test("a batch still marked sending is sent again with the same change ids", () => {
    const { store } = setup();
    store.insert("rooms", { scope_id: scope, name: "Attic" });
    const first = takeBatch(store, scope);
    store.insert("rooms", { scope_id: scope, name: "Cellar" });
    const again = takeBatch(store, scope);
    assert.deepEqual(again, first);
    assert.equal(outbox(store)[0].attempts, 2);

    returnToPending(store, scope, [first[0].change_id]);
    assert.deepEqual(
        takeBatch(store, scope).map((change) => change.fields?.name),
        ["Attic", "Cellar"],
    );
});
