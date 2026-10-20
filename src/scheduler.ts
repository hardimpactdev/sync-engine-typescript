import type { SyncClient, SyncOutcome } from "./client";

// When to sync: soon after a local write, at once on any other
// trigger, and again after a failure with growing waits. Outcomes that need the
// user or a new app version stop the retries until the next trigger.

export type SyncTrigger = "start" | "write" | "foreground" | "network" | "poke" | "manual";

export type SyncSchedulerOptions = {
    client: SyncClient;
    onOutcome?(outcome: SyncOutcome, trigger: SyncTrigger | "retry"): void;
    /** A run threw, for example because its database closed. It retries like `retry`. */
    onError?(error: unknown): void;
    /** Wait after a write, so a burst of taps goes in one push. Default 200 ms. */
    writeDelayMs?: number;
    /** First wait after a failure. It doubles up to `maxBackoffMs`. Default 1 s. */
    minBackoffMs?: number;
    /** Default 5 minutes. */
    maxBackoffMs?: number;
    setTimer?(callback: () => void, ms: number): unknown;
    clearTimer?(timer: unknown): void;
    /** A number in [0, 1) for jitter. */
    random?(): number;
};

export type SyncScheduler = {
    trigger(reason: SyncTrigger): void;
    /** Stops all timers. Later triggers do nothing. */
    stop(): void;
    /** Resolves when no run is in flight. Close the database after it. */
    idle(): Promise<void>;
};

const stopping: ReadonlySet<SyncOutcome["status"]> = new Set(["upgrade_required", "gone", "unauthorized", "invalid"]);

export function createSyncScheduler(options: SyncSchedulerOptions): SyncScheduler {
    const setTimer = options.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
    const clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer as ReturnType<typeof setTimeout>));
    const random = options.random ?? Math.random;
    const writeDelay = options.writeDelayMs ?? 200;
    const minBackoff = options.minBackoffMs ?? 1000;
    const maxBackoff = options.maxBackoffMs ?? 300_000;

    let timer: unknown = null;
    let failures = 0;
    let stopped = false;
    let inFlight: Promise<void> | null = null;

    function schedule(ms: number, trigger: SyncTrigger | "retry") {
        if (timer !== null) clearTimer(timer);
        timer = setTimer(() => {
            timer = null;
            void run(trigger);
        }, ms);
    }

    function run(trigger: SyncTrigger | "retry"): Promise<void> {
        const current = attempt(trigger).finally(() => {
            if (inFlight === current) inFlight = null;
        });
        inFlight = current;
        return current;
    }

    async function attempt(trigger: SyncTrigger | "retry") {
        if (stopped) return;
        let outcome: SyncOutcome;
        try {
            outcome = await options.client.sync();
        } catch (error) {
            if (stopped) return;
            options.onError?.(error);
            outcome = { status: "retry", retryAfterMs: null };
        }
        if (stopped) return;
        options.onOutcome?.(outcome, trigger);
        if (outcome.status === "synced") {
            failures = 0;
            return;
        }
        if (stopping.has(outcome.status)) {
            failures = 0;
            return;
        }
        failures++;
        const backoff = Math.min(maxBackoff, minBackoff * 2 ** (failures - 1));
        const wait =
            outcome.status === "retry" && outcome.retryAfterMs !== null
                ? Math.max(outcome.retryAfterMs, minBackoff)
                : backoff / 2 + (backoff / 2) * random();
        // A newer trigger may have set a timer while this run was in flight.
        if (timer === null) schedule(wait, "retry");
    }

    return {
        trigger(reason) {
            if (stopped) return;
            if (reason === "write") {
                schedule(writeDelay, reason);
                return;
            }
            failures = 0;
            if (timer !== null) {
                clearTimer(timer);
                timer = null;
            }
            void run(reason);
        },
        stop() {
            stopped = true;
            if (timer !== null) clearTimer(timer);
            timer = null;
        },
        async idle() {
            while (inFlight) await inFlight.catch(() => undefined);
        },
    };
}
