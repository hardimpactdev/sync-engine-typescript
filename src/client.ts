import type { Row } from "./database";
import type { Ack, PullPage, WireChange } from "./protocol";
import { engineOf, type LocalStore } from "./store";
import {
    applyAcks,
    applyPullPage,
    cursorOf,
    returnToPending,
    takeBatch,
    type RejectedChange,
    type TakeBatchOptions,
} from "./sync";

// One sync run for one scope (protocol §3): push the next batch of the
// outbox, apply the acks, then pull pages until the feed is drained. The app
// supplies the transport, so authentication and the base URL stay in the app.

export type SyncRequestBody = {
    protocol: 1;
    client_id: string;
    cursor: string | null;
    entities: string[];
    changes: WireChange[];
};

/** The answer of one request. A thrown error means the request did not complete (offline). */
export type SyncTransportResult = {
    status: number;
    body: unknown;
    /** Seconds from a `Retry-After` header, when there was one. */
    retryAfter?: number | null;
};

export type SyncTransport = (body: SyncRequestBody) => Promise<SyncTransportResult>;

export type SyncOutcome =
    /** The outbox is sent and the feed is drained. */
    | { status: "synced"; pulled: number; pushed: number }
    /** The request did not complete. Try again later. */
    | { status: "offline" }
    /** The server could not answer now (429, 5xx or a garbled answer). */
    | { status: "retry"; retryAfterMs: number | null }
    /** The server speaks a newer protocol. Local edits stay queued. */
    | { status: "upgrade_required" }
    /** The user cannot read this scope any more. Its local data is gone. */
    | { status: "gone"; lostChanges: number }
    /** Authentication failed. The app decides what to do with the session. */
    | { status: "unauthorized"; httpStatus: number }
    /** The server refused the request as malformed: a bug in this app. */
    | { status: "invalid"; message: string };

export type SyncClientOptions = {
    store: LocalStore;
    /** The scope's id in the local rows. */
    scopeId: string;
    clientId: string;
    /** The entities this app version knows. */
    entities: readonly string[];
    transport: SyncTransport;
    isUploaded?: TakeBatchOptions["isUploaded"];
    onRejected?(changes: RejectedChange[]): void;
    onDeletedElsewhere?(rows: { entity: string; id: string }[]): void;
    /** Requests per run, to stop a run that never ends. Default 25. */
    maxRequests?: number;
};

export type SyncClient = {
    /** Runs a sync. A call during a run waits for that run and one more. */
    sync(): Promise<SyncOutcome>;
};

const resyncKey = (scopeId: string) => `sync.resync.${scopeId}`;

export function createSyncClient(options: SyncClientOptions): SyncClient {
    const { store, scopeId } = options;
    const engine = engineOf(store);
    let running: Promise<SyncOutcome> | null = null;
    let again = false;

    async function runOnce(): Promise<SyncOutcome> {
        let pulled = 0;
        let pushed = 0;
        let seen = resyncing() ? new Set<string>() : null;

        for (let request = 0; request < (options.maxRequests ?? 25); request++) {
            const batch = takeBatch(store, scopeId, { isUploaded: options.isUploaded });
            const ids = batch.map((change) => change.change_id);
            const body: SyncRequestBody = {
                protocol: 1,
                client_id: options.clientId,
                cursor: seen && !seenStarted(seen) ? null : cursorOf(store, scopeId),
                entities: [...options.entities],
                changes: batch,
            };

            let result: SyncTransportResult;
            try {
                result = await options.transport(body);
            } catch {
                if (ids.length) returnToPending(store, scopeId, ids);
                return { status: "offline" };
            }

            const failure = failed(result, ids);
            if (failure) {
                if (failure.status === "resync") {
                    store.transaction(() => setResync(true));
                    seen = new Set<string>();
                    continue;
                }
                return failure;
            }

            const response = result.body as PullPage & { acks: Ack[]; last_position?: number };
            if (ids.length) {
                const acks = applyAcks(store, scopeId, response.acks);
                const answered = new Set(response.acks.map((ack) => ack.change_id));
                const unanswered = ids.filter((id) => !answered.has(id));
                if (unanswered.length) returnToPending(store, scopeId, unanswered);
                if (acks.rejected.length) options.onRejected?.(acks.rejected);
                pushed += ids.length - unanswered.length;
            }

            // The server saw changes whose acks this device has not got, for example
            // after a lost answer. Their effect may already be in this page, so
            // send them again first; the receipts answer them, then pull again.
            if (typeof response.last_position === "number" && hasUnackedThrough(response.last_position)) continue;

            const page = applyPullPage(store, scopeId, response);
            if (page.deletedElsewhere.length) options.onDeletedElsewhere?.(page.deletedElsewhere);
            pulled += response.changes.length;
            if (seen) {
                seen.add(startedMark);
                for (const change of response.changes) {
                    if (change.op === "upsert") seen.add(`${change.entity}:${change.id}`);
                }
            }

            if (response.has_more) continue;
            if (seen) {
                sweep(seen);
                seen = null;
            }
            if (ids.length && hasSendable()) continue;
            return { status: "synced", pulled, pushed };
        }
        return { status: "retry", retryAfterMs: null };
    }

    /** Maps a non-200 answer to its outcome, after putting the batch back. */
    function failed(
        result: SyncTransportResult,
        ids: string[],
    ): SyncOutcome | { status: "resync" } | null {
        if (result.status === 200 && isResponse(result.body)) return null;
        if (ids.length) returnToPending(store, scopeId, ids);
        const code = errorCode(result.body);
        switch (true) {
            case result.status === 200:
                return { status: "retry", retryAfterMs: null };
            case result.status === 401 || result.status === 403:
                return { status: "unauthorized", httpStatus: result.status };
            case result.status === 404:
                return { status: "gone", lostChanges: purge() };
            case result.status === 409 && code === "resync_required":
                return { status: "resync" };
            case result.status === 426:
                return { status: "upgrade_required" };
            case result.status === 422:
                return { status: "invalid", message: errorMessage(result.body) };
            default:
                return {
                    status: "retry",
                    retryAfterMs: typeof result.retryAfter === "number" ? result.retryAfter * 1000 : null,
                };
        }
    }

    function hasSendable(): boolean {
        return (
            (engine.db.get<{ count: number }>(
                "SELECT COUNT(*) AS count FROM sync_outbox WHERE scope_id = ? AND state = 'pending'",
                [scopeId],
            )?.count ?? 0) > 0
        );
    }

    function hasUnackedThrough(position: number): boolean {
        return (
            (engine.db.get<{ count: number }>(
                "SELECT COUNT(*) AS count FROM sync_outbox WHERE scope_id = ? AND position <= ?",
                [scopeId, position],
            )?.count ?? 0) > 0
        );
    }

    function resyncing(): boolean {
        return (
            engine.db.get<{ value: string }>("SELECT value FROM local_meta WHERE key = ?", [resyncKey(scopeId)])
                ?.value === "1"
        );
    }

    function setResync(on: boolean) {
        if (on) {
            engine.db.run(
                "INSERT INTO local_meta (key, value) VALUES (?, '1') ON CONFLICT (key) DO UPDATE SET value = '1'",
                [resyncKey(scopeId)],
            );
        } else {
            engine.db.run("DELETE FROM local_meta WHERE key = ?", [resyncKey(scopeId)]);
        }
    }

    /** After a resync pull: rows the server no longer has, and nobody is changing, go. */
    function sweep(seen: Set<string>) {
        store.transaction(() => {
            for (const table of engine.specs.keys()) {
                for (const row of rowsOfScope(table)) {
                    const id = row.sync_id as string;
                    if (seen.has(`${table}:${id}`)) continue;
                    if (engine.entriesFor(table, id).length) continue;
                    engine.deleteRow(table, id);
                }
            }
            setResync(false);
        });
    }

    /** Removes everything of the scope. Returns how many queued changes were lost. */
    function purge(): number {
        return store.transaction(() => {
            const lost =
                engine.db.get<{ count: number }>(
                    "SELECT COUNT(*) AS count FROM sync_outbox WHERE scope_id = ?",
                    [scopeId],
                )?.count ?? 0;
            for (const table of engine.specs.keys()) {
                for (const row of rowsOfScope(table)) engine.deleteRow(table, row.sync_id as string);
            }
            engine.db.run("DELETE FROM sync_outbox WHERE scope_id = ?", [scopeId]);
            engine.db.run("DELETE FROM sync_rejected WHERE scope_id = ?", [scopeId]);
            engine.db.run("DELETE FROM sync_state WHERE scope_id = ?", [scopeId]);
            setResync(false);
            for (const table of ["sync_outbox", "sync_rejected", "sync_state"]) engine.touch(table);
            return lost;
        });
    }

    function rowsOfScope(table: string): Row[] {
        if (engine.columns(table).has(engine.scopeColumn)) {
            return engine.db.all<Row>(`SELECT * FROM ${table} WHERE ${engine.scopeColumn} = ?`, [scopeId]);
        }
        const spec = engine.spec(table);
        return engine.db
            .all<Row>(`SELECT * FROM ${table}`)
            .filter((row) => spec.scope(row, engine.db) === scopeId);
    }

    return {
        sync() {
            if (running) {
                again = true;
                return running;
            }
            const run = (async () => {
                try {
                    let outcome = await runOnce();
                    while (again && outcome.status === "synced") {
                        again = false;
                        outcome = await runOnce();
                    }
                    return outcome;
                } finally {
                    again = false;
                    running = null;
                }
            })();
            running = run;
            return run;
        },
    };
}

/** Marks that a resync has received its first page, so later requests use the cursor. */
const startedMark = "\u0000started";
function seenStarted(seen: Set<string>): boolean {
    return seen.has(startedMark);
}

function isResponse(body: unknown): body is PullPage & { acks: Ack[] } {
    if (!body || typeof body !== "object") return false;
    const value = body as Record<string, unknown>;
    return (
        Array.isArray(value.changes) &&
        Array.isArray(value.acks) &&
        typeof value.cursor === "string" &&
        typeof value.has_more === "boolean"
    );
}

function errorCode(body: unknown): string | null {
    const error = (body as { error?: { code?: unknown } } | null)?.error;
    return typeof error?.code === "string" ? error.code : null;
}

function errorMessage(body: unknown): string {
    const error = (body as { error?: { message?: unknown } } | null)?.error;
    return typeof error?.message === "string" ? error.message : "The server refused the sync request.";
}
