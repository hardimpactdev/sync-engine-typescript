import assert from "node:assert/strict";
import test from "node:test";
import { createRealtimeSync } from "../src/realtime.ts";
import type { SyncTrigger } from "../src/scheduler.ts";
import { pulled, scope, setup } from "./setup.ts";

const me = "01JBC4Y0R8T5D8M4QKX3N2V6ZA";
const other = "01JBC4Y0R8T5D8M4QKX3N2V6ZB";
const kettle = "01JBC59ZQ0B2D4F6H8K0M2P4R6";

/** A realtime hook on a fresh store, with a scheduler and timers that only record. */
function realtime({ seq }: { seq?: number } = {}) {
    const { store } = setup();
    if (seq !== undefined) pulled(store, "items", kettle, { name: "Kettle" }, seq);
    const triggers: SyncTrigger[] = [];
    const timers: { ms: number; run(): void }[] = [];
    const sync = createRealtimeSync({
        scheduler: { trigger: (reason) => triggers.push(reason), stop: () => undefined, idle: async () => undefined },
        store,
        scopeId: scope,
        clientId: me,
        intervalMs: 30_000,
        setTimer: (run, ms) => {
            const timer = { ms, run };
            timers.push(timer);
            return timer;
        },
        clearTimer: (timer) => timers.splice(timers.indexOf(timer as never), 1),
    });
    const tick = () => timers.shift()!.run();
    return { sync, triggers, timers, tick };
}

test("a poke from this install is ignored", () => {
    const { sync, triggers } = realtime({ seq: 4 });
    sync.poke({ seq: 9, client_id: me });
    assert.deepEqual(triggers, []);
});

test("a poke at or below the pulled seq is ignored", () => {
    const { sync, triggers } = realtime({ seq: 4 });
    sync.poke({ seq: 3, client_id: other });
    sync.poke({ seq: 4, client_id: null });
    assert.deepEqual(triggers, []);
});

test("a newer poke pulls, from another install, the web or before the first pull", () => {
    const { sync, triggers } = realtime({ seq: 4 });
    sync.poke({ seq: 5, client_id: other });
    sync.poke({ seq: 6, client_id: null });
    assert.deepEqual(triggers, ["poke", "poke"]);

    const fresh = realtime();
    fresh.sync.poke({ seq: 1, client_id: other });
    assert.deepEqual(fresh.triggers, ["poke"]);
});

test("every connect pulls, the first one and each reconnect", () => {
    const { sync, triggers } = realtime();
    sync.connection("connected");
    sync.connection("connected");
    assert.deepEqual(triggers, ["network"], "a repeated connected is no change");

    sync.connection("disconnected");
    sync.connection("connected");
    sync.connection("disconnected");
    sync.connection("connected");
    assert.deepEqual(triggers, ["network", "network", "network"]);
});

test("the interval pulls only while disconnected", () => {
    const { sync, triggers, timers, tick } = realtime();
    assert.deepEqual(timers.map((timer) => timer.ms), [30_000], "it starts disconnected");
    tick();
    tick();
    assert.deepEqual(triggers, ["network", "network"]);
    assert.equal(timers.length, 1);

    sync.connection("connected");
    assert.equal(timers.length, 0, "a connect stops the interval");
    assert.deepEqual(triggers, ["network", "network", "network"]);

    sync.connection("disconnected");
    sync.connection("disconnected");
    assert.deepEqual(timers.map((timer) => timer.ms), [30_000], "a disconnect starts one interval");
    tick();
    assert.deepEqual(triggers, ["network", "network", "network", "network"]);
});

test("stop clears the timer and ignores later pokes and connects", () => {
    const { sync, triggers, timers } = realtime();
    sync.stop();
    assert.equal(timers.length, 0);

    sync.poke({ seq: 1, client_id: other });
    sync.connection("connected");
    sync.connection("disconnected");
    assert.deepEqual(triggers, []);
    assert.equal(timers.length, 0);
});
