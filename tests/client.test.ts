import assert from "node:assert/strict";
import test from "node:test";
import {
    createSyncClient,
    type SyncRequestBody,
    type SyncTransport,
    type SyncTransportResult,
} from "../src/client.ts";
import { createSyncScheduler } from "../src/scheduler.ts";
import { cursorOf } from "../src/sync.ts";
import { scope, outbox, pulled, setup, type Store } from "./setup.ts";

const client = "01JBC4Y0R8T5D8M4QKX3N2V6ZA";
const kettle = "01JBC59ZQ0B2D4F6H8K0M2P4R6";
const toaster = "01JBC59ZQ0B2D4F6H8K0M2P4R7";
const hall = "01J9YY7Q6R5S4T3V2W1X0Y9Z8A";

/** A server that answers from a script and records what it was sent. */
type Answer = SyncTransportResult | Error | ((body: SyncRequestBody) => SyncTransportResult);

function scripted(...answers: Answer[]) {
    const sent: SyncRequestBody[] = [];
    const transport: SyncTransport = async (body) => {
        sent.push(structuredClone(body));
        const answer = answers.shift();
        if (!answer) throw new Error("The script ran out.");
        if (answer instanceof Error) throw answer;
        return typeof answer === "function" ? answer(body) : answer;
    };
    return { sent, transport };
}

function page(changes: unknown[], cursor: string, hasMore = false, acks: unknown[] = []): SyncTransportResult {
    return {
        status: 200,
        body: { protocol: 1, acks, changes, cursor, has_more: hasMore, server_time: "2026-10-20T10:00:00Z" },
    };
}

function syncWith(store: Store, transport: SyncTransport) {
    return createSyncClient({ store, scopeId: scope, clientId: client, entities: ["rooms", "items"], transport });
}

function names(store: Store) {
    return store.all<{ name: string }>("SELECT name FROM items ORDER BY name").map((row) => row.name);
}

test("a first sync pulls every page from a null cursor and stores the cursor", async () => {
    const { store } = setup();
    const server = scripted(
        page([{ entity: "items", id: kettle, seq: 1, op: "upsert", row: { name: "Kettle" } }], "c1", true),
        page([{ entity: "items", id: toaster, seq: 2, op: "upsert", row: { name: "Toaster" } }], "c2"),
    );

    const outcome = await syncWith(store, server.transport).sync();

    assert.deepEqual(outcome, { status: "synced", pulled: 2, pushed: 0 });
    assert.deepEqual(server.sent.map((body) => body.cursor), [null, "c1"]);
    assert.deepEqual(server.sent[0], { protocol: 1, client_id: client, cursor: null, entities: ["rooms", "items"], changes: [] });
    assert.deepEqual(names(store), ["Kettle", "Toaster"]);
    assert.equal(cursorOf(store, scope), "c2");
});

test("a sync pushes the outbox, applies the acks and stops when nothing is left", async () => {
    const { store } = setup();
    pulled(store, "items", kettle, { name: "Kettle" }, 4);
    store.update("items", kettle, { notes: "Descale" });
    const [change] = outbox(store);
    assert.equal(change.op, "upsert");

    const server = scripted((body) => page([], "c5", false, [{ change_id: body.changes[0].change_id, status: "applied", seq: 5 }]));

    const outcome = await syncWith(store, server.transport).sync();

    assert.deepEqual(outcome, { status: "synced", pulled: 0, pushed: 1 });
    assert.equal(server.sent[0].changes.length, 1);
    assert.equal(server.sent[0].cursor, "cursor-4");
    assert.deepEqual(outbox(store), []);
});

test("offline keeps the batch queued with the same change ids", async () => {
    const { store } = setup();
    pulled(store, "items", kettle, { name: "Kettle" }, 4);
    store.update("items", kettle, { notes: "Descale" });

    const offline = scripted(new TypeError("Network request failed"));
    assert.deepEqual(await syncWith(store, offline.transport).sync(), { status: "offline" });
    assert.equal(outbox(store)[0].state, "pending");

    const later = scripted(page([], "c5"));
    await syncWith(store, later.transport).sync();
    assert.equal(later.sent[0].changes[0].change_id, offline.sent[0].changes[0].change_id);
});

test("server errors map to outcomes and never lose the batch", async () => {
    const cases: [SyncTransportResult, string][] = [
        [{ status: 503, body: {}, retryAfter: 30 }, "retry"],
        [{ status: 429, body: {} }, "retry"],
        [{ status: 200, body: { nonsense: true } }, "retry"],
        [{ status: 401, body: {} }, "unauthorized"],
        [{ status: 426, body: { error: { code: "upgrade_required", message: "Update" } } }, "upgrade_required"],
        [{ status: 422, body: { error: { code: "invalid_request", message: "Bad" } } }, "invalid"],
    ];
    for (const [answer, status] of cases) {
        const { store } = setup();
        pulled(store, "items", kettle, { name: "Kettle" }, 4);
        store.update("items", kettle, { notes: "Descale" });

        const outcome = await syncWith(store, scripted(answer).transport).sync();

        assert.equal(outcome.status, status);
        assert.equal(outbox(store).length, 1);
        assert.equal(outbox(store)[0].state, "pending");
    }
    const { store } = setup();
    const outcome = await syncWith(store, scripted({ status: 503, body: {}, retryAfter: 30 }).transport).sync();
    assert.deepEqual(outcome, { status: "retry", retryAfterMs: 30_000 });
});

test("a scope the user lost is purged, and the lost changes are counted", async () => {
    const { store } = setup();
    pulled(store, "items", kettle, { name: "Kettle" }, 4);
    store.update("items", kettle, { notes: "Descale" });

    const outcome = await syncWith(store, scripted({ status: 404, body: {} }).transport).sync();

    assert.deepEqual(outcome, { status: "gone", lostChanges: 1 });
    assert.deepEqual(names(store), []);
    assert.deepEqual(outbox(store), []);
    assert.equal(cursorOf(store, scope), null);
});

test("a resync pulls from the start and sweeps rows the server no longer has", async () => {
    const { store } = setup();
    pulled(store, "items", kettle, { name: "Kettle" }, 4);
    pulled(store, "items", toaster, { name: "Toaster" }, 5);
    pulled(store, "rooms", hall, { name: "Hall" }, 6);
    store.update("rooms", hall, { name: "Big hall" });

    const server = scripted(
        { status: 409, body: { error: { code: "resync_required", message: "Old cursor" } } },
        page([{ entity: "items", id: kettle, seq: 9, op: "upsert", row: { name: "Kettle" } }], "r1", true),
        page([], "r2"),
        // The queued room edit was not acked yet, so the client sends it once more.
        (body) => page([], "r3", false, [{ change_id: body.changes[0].change_id, status: "applied", seq: 10 }]),
    );
    const outcome = await syncWith(store, server.transport).sync();

    assert.deepEqual(outcome, { status: "synced", pulled: 1, pushed: 1 });
    assert.deepEqual(server.sent.map((body) => body.cursor), ["cursor-6", null, "r1", "r2"]);
    assert.deepEqual(names(store), ["Kettle"]);
    // The room has a queued change, so the sweep keeps it.
    assert.equal(store.get<{ name: string }>("SELECT name FROM rooms WHERE sync_id = ?", [hall])?.name, "Big hall");
});

test("an interrupted resync starts again from the first page", async () => {
    const { store } = setup();
    pulled(store, "items", kettle, { name: "Kettle" }, 4);

    const first = scripted(
        { status: 409, body: { error: { code: "resync_required", message: "Old cursor" } } },
        page([], "r1", true),
        new TypeError("Network request failed"),
    );
    assert.deepEqual(await syncWith(store, first.transport).sync(), { status: "offline" });

    const second = scripted(page([], "r9"));
    assert.equal((await syncWith(store, second.transport).sync()).status, "synced");
    assert.equal(second.sent[0].cursor, null);
    assert.deepEqual(names(store), []);
});

test("a sync during a run waits for that run and runs once more", async () => {
    const { store } = setup();
    let release!: () => void;
    let calls = 0;
    const transport: SyncTransport = async () => {
        calls++;
        if (calls === 1) await new Promise<void>((resolve) => (release = resolve));
        return page([], `c${calls}`);
    };
    const sync = syncWith(store, transport);

    const first = sync.sync();
    const second = sync.sync();
    const third = sync.sync();
    release();

    assert.equal(await first, await second);
    assert.equal(await third, await first);
    assert.equal(calls, 2);
});

test("the scheduler debounces writes and backs off after failures", async () => {
    const timers: { ms: number; run(): void }[] = [];
    const outcomes: string[] = [];
    let answers = ["offline", "offline", "synced"];
    const scheduler = createSyncScheduler({
        client: { sync: async () => (answers.shift() === "synced" ? { status: "synced", pulled: 0, pushed: 0 } : { status: "offline" }) },
        onOutcome: (outcome, trigger) => outcomes.push(`${trigger}:${outcome.status}`),
        setTimer: (run, ms) => {
            const timer = { ms, run };
            timers.push(timer);
            return timer;
        },
        clearTimer: (timer) => timers.splice(timers.indexOf(timer as never), 1),
        random: () => 0.5,
    });
    const tick = async () => {
        const timer = timers.shift()!;
        timer.run();
        await new Promise((resolve) => setImmediate(resolve));
        return timer.ms;
    };

    scheduler.trigger("write");
    scheduler.trigger("write");
    assert.deepEqual(timers.map((timer) => timer.ms), [200]);

    assert.equal(await tick(), 200);
    assert.equal(await tick(), 750);
    assert.equal(await tick(), 1500);
    assert.deepEqual(outcomes, ["write:offline", "retry:offline", "retry:synced"]);
    assert.equal(timers.length, 0);

    answers = ["offline"];
    scheduler.trigger("foreground");
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(timers.length, 1, "an offline outcome retries");

    scheduler.stop();
    scheduler.trigger("manual");
    assert.equal(timers.length, 0);
});

test("the scheduler survives a run that throws and reports when it is idle", async () => {
    let release!: () => void;
    const errors: unknown[] = [];
    const timers: number[] = [];
    const scheduler = createSyncScheduler({
        client: {
            sync: () =>
                new Promise((_, reject) => {
                    release = () => reject(new Error("database is closed"));
                }),
        },
        onError: (error) => errors.push(error),
        setTimer: (_run, ms) => timers.push(ms),
        clearTimer: () => undefined,
        random: () => 0,
    });

    scheduler.trigger("start");
    let idle = false;
    const waiting = scheduler.idle().then(() => (idle = true));
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(idle, false);

    release();
    await waiting;
    assert.equal(errors.length, 1);
    assert.deepEqual(timers, [500], "a thrown run retries after a backoff");
});

test("a pull waits while the server has seen changes whose acks were lost", async () => {
    const { store } = setup();
    pulled(store, "counters", "01JBC5A3X5G9J1L3N5Q7S9V1X4", { name: "Milk", quantity: 2 }, 4);
    store.operate("counters", "01JBC5A3X5G9J1L3N5Q7S9V1X4", "adjust", { name: "Milk", delta: 1 });

    const server = scripted(
        // The server applied the change, but this answer never reached the phone.
        new TypeError("Network request failed"),
        // A later answer with the new total, but without the ack.
        page([{ entity: "counters", id: "01JBC5A3X5G9J1L3N5Q7S9V1X4", seq: 5, op: "upsert", row: { name: "Milk", quantity: 3 } }], "c5"),
        // The resend gets its ack from the receipt, and the page comes again.
        (body) => page([{ entity: "counters", id: "01JBC5A3X5G9J1L3N5Q7S9V1X4", seq: 5, op: "upsert", row: { name: "Milk", quantity: 3 } }], "c5", false, [
            { change_id: body.changes[0].change_id, status: "duplicate", seq: 5 },
        ]),
    );
    const transport: SyncTransport = async (body) => {
        const answer = await server.transport(body);
        if (server.sent.length === 2) (answer.body as Record<string, unknown>).last_position = body.changes[0]?.position ?? 1;
        return answer;
    };
    const sync = createSyncClient({ store, scopeId: scope, clientId: client, entities: ["counters"], transport });

    assert.deepEqual(await sync.sync(), { status: "offline" });
    assert.equal((await sync.sync()).status, "synced");
    assert.equal(store.get<{ quantity: number }>("SELECT quantity FROM counters")?.quantity, 3, "the delta counts once");
    assert.deepEqual(outbox(store), []);
});

