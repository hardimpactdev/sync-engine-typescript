import type { Row } from "./database";
import type { Ack, PullPage, WireChange } from "./protocol";
import { engineOf, type Engine, type LocalStore, type OutboxEntry } from "./store";

// The device's half of a sync run, as functions over the store and a parsed
// response. The HTTP client calls them in this order: takeBatch, post the
// request, then applyAcks and applyPullPage, or returnToPending on a failure.

export const maxBatchSize = 100;

export type TakeBatchOptions = {
    /** At most this many changes, and never more than 100. */
    limit?: number;
    /** Whether a local file has uploaded. Changes that depend on one wait until it has. */
    isUploaded?: (fileId: string) => boolean;
};

/**
 * Marks the next sendable changes of a scope `sending` and returns them in
 * wire format, in outbox order. A change waits while an earlier change to the
 * same row waits, or while a row it points at waits. When a batch is still
 * marked `sending`, for example after the app was closed mid-request, it
 * returns that batch again, so the resend carries the same change ids.
 */
export function takeBatch(
    store: LocalStore,
    scopeId: string,
    { limit = maxBatchSize, isUploaded = () => true }: TakeBatchOptions = {},
): WireChange[] {
    const engine = engineOf(store);
    return engine.transaction(() => {
        let batch = outbox(engine, scopeId, "sending");
        if (batch.length === 0) {
            const waiting = new Set<string>();
            for (const entry of outbox(engine, scopeId, "pending")) {
                if (batch.length >= Math.min(limit, maxBatchSize)) break;
                const key = `${entry.entity}:${entry.row_id}`;
                const waits =
                    waiting.has(key) ||
                    engine.referencesOf(entry).some((reference) => waiting.has(reference)) ||
                    files(entry).some((file) => !isUploaded(file));
                if (waits) waiting.add(key);
                else batch.push(entry);
            }
        }
        for (const entry of batch) {
            engine.db.run(
                "UPDATE sync_outbox SET state = 'sending', attempts = attempts + 1 WHERE position = ?",
                [entry.position],
            );
        }
        if (batch.length > 0) engine.touch("sync_outbox");
        return batch.map(toWire);
    });
}

/** Puts a failed batch back to `pending`. Its changes keep their ids and payloads. */
export function returnToPending(store: LocalStore, scopeId: string, changeIds?: string[]) {
    const engine = engineOf(store);
    engine.transaction(() => {
        if (changeIds) {
            for (const changeId of changeIds) {
                engine.db.run(
                    "UPDATE sync_outbox SET state = 'pending' WHERE scope_id = ? AND change_id = ? AND state = 'sending'",
                    [scopeId, changeId],
                );
            }
        } else {
            engine.db.run(
                "UPDATE sync_outbox SET state = 'pending' WHERE scope_id = ? AND state = 'sending'",
                [scopeId],
            );
        }
        engine.touch("sync_outbox");
    });
}

export type RejectedChange = {
    change_id: string;
    entity: string;
    id: string;
    op: string;
    code: string;
    message: string;
};

export type AckResult = {
    /** Change ids the server applied, now or before. */
    applied: string[];
    rejected: RejectedChange[];
    remapped: { entity: string; from: string; to: string }[];
};

/**
 * Applies the acks of a push in one transaction. Applied and duplicate changes
 * leave the outbox; a remap moves every reference to the server's row. A
 * rejected change moves to `sync_rejected` with its values, and its row is
 * restored from the server's `current` with the other queued changes on top.
 */
export function applyAcks(store: LocalStore, scopeId: string, acks: Ack[]): AckResult {
    const engine = engineOf(store);
    const result: AckResult = { applied: [], rejected: [], remapped: [] };
    engine.transaction(() => {
        for (const ack of acks) {
            const entry = engine.db.get<OutboxEntry>(
                "SELECT * FROM sync_outbox WHERE change_id = ? AND scope_id = ?",
                [ack.change_id, scopeId],
            );
            if (!entry) continue;
            if (ack.status === "rejected") {
                result.rejected.push(...reject(engine, entry, ack));
                continue;
            }
            engine.db.run("DELETE FROM sync_outbox WHERE position = ?", [entry.position]);
            const row = engine.row(entry.entity, entry.row_id);
            if (entry.op === "delete") {
                if (row && row.deleted_at !== null) engine.deleteRow(entry.entity, entry.row_id);
            } else if (row) {
                // The server knows the row now, so a later delete must reach it.
                engine.db.run(
                    `UPDATE ${entry.entity} SET server_seq = MAX(COALESCE(server_seq, 0), ?) WHERE sync_id = ?`,
                    [ack.seq ?? 0, entry.row_id],
                );
            }
            if (ack.remap) {
                engine.remap(entry.entity, ack.remap.from, ack.remap.to);
                result.remapped.push({ entity: entry.entity, ...ack.remap });
            }
            result.applied.push(ack.change_id);
        }
        markState(engine, scopeId, "last_push_at");
        engine.touch("sync_outbox");
    });
    return result;
}

function reject(engine: Engine, entry: OutboxEntry, ack: Ack): RejectedChange[] {
    const code = ack.error?.code ?? "validation";
    const message = ack.error?.message ?? "";
    const rejected = [keep(engine, entry, code, message, ack.error?.fields, ack.current ?? null)];
    // Undo the change's deltas on other rows; its own row is restored below.
    engine.shiftCounters(entry, -1, { entity: entry.entity, row: entry.row_id });
    if (!engine.row(entry.entity, entry.row_id)) {
        // A remap or a tombstone already moved or removed the row; nothing to restore.
        return rejected;
    }
    if (ack.current) {
        engine.rebaseRow(entry.entity, entry.row_id, ack.current, { scopeId: entry.scope_id });
        return rejected;
    }
    // The server has no such row. Unsent changes to it were made on top of the
    // rejected one, so they are kept for the user too.
    for (const later of engine.entriesFor(entry.entity, entry.row_id)) {
        if (later.state !== "pending") continue;
        engine.shiftCounters(later, -1, { entity: later.entity, row: later.row_id });
        rejected.push(
            keep(engine, later, "parent_rejected", "An earlier change to this row was rejected.", undefined, null),
        );
    }
    engine.deleteRow(entry.entity, entry.row_id);
    return rejected;
}

function keep(
    engine: Engine,
    entry: OutboxEntry,
    code: string,
    message: string,
    fields: Record<string, string[]> | undefined,
    current: Row | null,
): RejectedChange {
    engine.db.run(
        "INSERT OR REPLACE INTO sync_rejected (change_id, scope_id, entity, row_id, op, payload, base_seq, error_code, error_message, error_fields, current, created_at, rejected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            entry.change_id,
            entry.scope_id,
            entry.entity,
            entry.row_id,
            entry.op,
            entry.payload,
            entry.base_seq,
            code,
            message,
            fields ? JSON.stringify(fields) : null,
            current ? JSON.stringify(current) : null,
            entry.created_at,
            engine.now(),
        ],
    );
    engine.db.run("DELETE FROM sync_outbox WHERE position = ?", [entry.position]);
    engine.touch("sync_rejected");
    return {
        change_id: entry.change_id,
        entity: entry.entity,
        id: entry.row_id,
        op: entry.op,
        code,
        message,
    };
}

export type PullResult = {
    /** Rows deleted on the server while this device had unsent changes to them. */
    deletedElsewhere: { entity: string; id: string }[];
};

/**
 * Applies one pull page and stores its cursor, in one transaction (protocol §6).
 * Pulled rows keep the fields of queued upserts, counters get queued deltas on
 * top, and rows with a queued delete stay hidden. A tombstone deletes the row
 * and drops its unsent changes. Entities this app does not know are skipped.
 */
export function applyPullPage(store: LocalStore, scopeId: string, page: PullPage): PullResult {
    const engine = engineOf(store);
    const result: PullResult = { deletedElsewhere: [] };
    engine.transaction(() => {
        for (const change of page.changes) {
            if (!engine.specs.has(change.entity)) continue;
            const row = engine.row(change.entity, change.id);
            if (change.op === "delete") {
                const pending = engine
                    .entriesFor(change.entity, change.id)
                    .filter((entry) => entry.state === "pending");
                engine.drop(pending);
                if (pending.some((entry) => entry.op !== "delete")) {
                    result.deletedElsewhere.push({ entity: change.entity, id: change.id });
                }
                if (row) engine.deleteRow(change.entity, change.id);
                continue;
            }
            // An older version than the one this device has is skipped.
            if (row && typeof row.server_seq === "number" && row.server_seq > change.seq) continue;
            engine.rebaseRow(change.entity, change.id, change.row ?? {}, {
                seq: change.seq,
                scopeId,
            });
        }
        engine.db.run(
            `INSERT INTO sync_state (scope_id, cursor, snapshot_complete, last_pull_at) VALUES (?, ?, ?, ?)
             ON CONFLICT (scope_id) DO UPDATE SET cursor = excluded.cursor,
                 snapshot_complete = MAX(sync_state.snapshot_complete, excluded.snapshot_complete),
                 last_pull_at = excluded.last_pull_at`,
            [scopeId, page.cursor, page.has_more ? 0 : 1, engine.now()],
        );
        engine.touch("sync_state");
    });
    return result;
}

/** The stored cursor of a scope, or null before its first pull. */
export function cursorOf(store: LocalStore, scopeId: string): string | null {
    return (
        engineOf(store).db.get<{ cursor: string | null }>(
            "SELECT cursor FROM sync_state WHERE scope_id = ?",
            [scopeId],
        )?.cursor ?? null
    );
}

function markState(engine: Engine, scopeId: string, column: "last_push_at") {
    engine.db.run(
        `INSERT INTO sync_state (scope_id, ${column}) VALUES (?, ?)
         ON CONFLICT (scope_id) DO UPDATE SET ${column} = excluded.${column}`,
        [scopeId, engine.now()],
    );
    engine.touch("sync_state");
}

function outbox(engine: Engine, scopeId: string, state: OutboxEntry["state"]): OutboxEntry[] {
    return engine.db.all<OutboxEntry>(
        "SELECT * FROM sync_outbox WHERE scope_id = ? AND state = ? ORDER BY position",
        [scopeId, state],
    );
}

function files(entry: OutboxEntry): string[] {
    return entry.depends_on === null ? [] : (JSON.parse(entry.depends_on) as string[]);
}

function toWire(entry: OutboxEntry): WireChange {
    const payload = JSON.parse(entry.payload) as Row;
    return {
        change_id: entry.change_id,
        entity: entry.entity,
        id: entry.row_id,
        op: entry.op,
        ...(entry.op === "upsert" ? { fields: payload } : entry.op === "delete" ? {} : { args: payload }),
        base_seq: entry.base_seq,
        position: entry.position,
        ...(entry.depends_on === null ? {} : { depends_on: files(entry) }),
        client_time: entry.created_at,
    };
}
