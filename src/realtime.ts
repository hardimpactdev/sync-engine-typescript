import type { SyncScheduler } from "./scheduler.ts";
import type { LocalStore } from "./store.ts";
import { pulledSeqOf } from "./sync.ts";

// Turns a real-time channel into scheduler triggers (protocol §4). The app owns
// the socket, for example Laravel Echo, and hands in its pokes and its
// connection state. A poke pulls when it is new. A connect pulls, to cover the
// pokes missed while away. While the socket is down, a timer pulls instead.

/** The `scope.changed` event (protocol §4). */
export type SyncPoke = { seq: number; client_id: string | null };

export type RealtimeConnection = "connected" | "disconnected";

export type RealtimeSyncOptions = {
    scheduler: SyncScheduler;
    store: LocalStore;
    /** The scope's id in the local rows. */
    scopeId: string;
    /** This install's client id. Its own pokes are ignored. */
    clientId: string;
    /** Wait between pulls while disconnected. Default 60 s. */
    intervalMs?: number;
    setTimer?(callback: () => void, ms: number): unknown;
    clearTimer?(timer: unknown): void;
};

export type RealtimeSync = {
    /** Pass each `scope.changed` event. */
    poke(event: SyncPoke): void;
    /** Pass each change of the socket state. It starts as disconnected. */
    connection(state: RealtimeConnection): void;
    /** Stops the timer. Later calls do nothing. The scheduler keeps running. */
    stop(): void;
};

export function createRealtimeSync(options: RealtimeSyncOptions): RealtimeSync {
    const { scheduler, store, scopeId } = options;
    const setTimer = options.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
    const clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer as ReturnType<typeof setTimeout>));
    const interval = options.intervalMs ?? 60_000;

    let connected = false;
    let stopped = false;
    let timer: unknown = null;

    function poll() {
        timer = setTimer(() => {
            timer = null;
            if (stopped || connected) return;
            scheduler.trigger("network");
            poll();
        }, interval);
    }

    function cancel() {
        if (timer !== null) clearTimer(timer);
        timer = null;
    }

    poll();

    return {
        poke(event) {
            if (stopped) return;
            if (event.client_id === options.clientId) return;
            const pulled = pulledSeqOf(store, scopeId);
            if (pulled !== null && event.seq <= pulled) return;
            scheduler.trigger("poke");
        },
        connection(state) {
            if (stopped) return;
            if (state === "connected") {
                if (connected) return;
                connected = true;
                cancel();
                scheduler.trigger("network");
                return;
            }
            if (!connected) return;
            connected = false;
            poll();
        },
        stop() {
            stopped = true;
            cancel();
        },
    };
}
