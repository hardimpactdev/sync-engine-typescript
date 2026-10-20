import type { Row } from "./database";

// The wire format of docs/protocol-v1.md, as the device sends and reads it.

/** One change in a push request (§3.1). */
export type WireChange = {
    change_id: string;
    entity: string;
    id: string;
    op: string;
    fields?: Row;
    args?: Row;
    base_seq: number | null;
    /** The change's place in this install's outbox. It only grows. */
    position?: number;
    depends_on?: string[];
    merge?: boolean;
    client_time: string;
};

export type RejectionCode =
    | "validation"
    | "deleted"
    | "parent_rejected"
    | "read_only_field"
    | "unknown_entity"
    | "unknown_op"
    | "change_id_reused"
    | "forbidden";

/** The server's answer to one change (§3.3). */
export type Ack = {
    change_id: string;
    status: "applied" | "duplicate" | "rejected";
    seq?: number;
    remap?: { from: string; to: string };
    overwrote?: string[];
    error?: { code: RejectionCode | string; message: string; fields?: Record<string, string[]> };
    current?: Row | null;
};

/** A row or tombstone in the change feed (§3.3). */
export type PulledChange = {
    entity: string;
    id: string;
    seq: number;
    op: "upsert" | "delete";
    row?: Row;
};

/** The pull part of a sync response. */
export type PullPage = {
    changes: PulledChange[];
    cursor: string;
    has_more: boolean;
};

/** A whole sync response. */
export type SyncResponse = PullPage & {
    protocol: 1;
    acks: Ack[];
    /** The highest outbox position of this install the server has seen. */
    last_position?: number;
    server_time: string;
};
