# Sync protocol v1

This is the contract between the Laravel server
([sync-engine-laravel](https://github.com/hardimpactdev/sync-engine-laravel))
and the TypeScript client
([sync-engine-typescript](https://github.com/hardimpactdev/sync-engine-typescript)).
Both sides follow it.

A breaking change needs a new `protocol` number. Adding an optional field is
not breaking.

## 1. Ids and scope

- Every synced row has a `sync_id`: a 26-character Crockford ULID in upper case,
  made on the device or the server. References between rows are always
  `sync_id`s, never server integer ids. A pulled row may carry its own
  `server_id` as a server-owned field, for apps that still call older REST
  endpoints.
- Every synced row belongs to one **scope**: the unit that syncs together and
  that a user may read, such as a team, a household or a workspace. The app
  decides what a scope is. Its id on the wire is the scope row's `sync_id`.
- Every change has a `change_id`, a ULID made on the device. It is the
  idempotency key.
- Every install has a `client_id`, a ULID made on first launch.

## 2. Server ordering

- Each scope has one counter, `last_seq`. Every server write to a synced row
  takes the next value as the row's `change_seq`, inside the write's
  transaction.
- The counter is bumped by writing first (`UPDATE … SET last_seq = last_seq +
  1`), then reading. The write lock lasts until commit, so seqs commit in order.
  A pull after seq N never misses a row that commits later with a seq at or
  below N.
- Deletes write a tombstone `(scope, entity, sync_id, change_seq)`.
- Tombstones older than the retention period are pruned. The scope's
  `floor_seq` then rises. A cursor below it gets `409 resync_required`.

## 3. Endpoint

`POST` with a JSON body to a route the app defines for one scope, such as
`POST /api/teams/{team}/sync`. The route key is the app's own. The app's
middleware authenticates the user and checks membership before the package runs.

### 3.1 Request

| Field       | Type            | Notes                                                       |
| ----------- | --------------- | ----------------------------------------------------------- |
| `protocol`  | `1`             | Other values get `426`.                                     |
| `client_id` | ULID            |                                                             |
| `cursor`    | string or null  | Opaque. Null for the first pull or a backfill.              |
| `entities`  | string[]        | The entities this app version knows. Pull returns only these. |
| `changes`   | Change[]        | At most 100. May be empty (pull only).                      |
| `limit`     | int, optional   | Pull page size, at most 500. Default 500.                   |

A **Change**:

| Field         | Type                 | Notes                                                         |
| ------------- | -------------------- | ------------------------------------------------------------- |
| `change_id`   | ULID                 |                                                               |
| `entity`      | string               |                                                               |
| `id`          | ULID                 | The row's `sync_id`.                                          |
| `op`          | string               | `upsert`, `delete`, or a named operation of the entity.       |
| `fields`      | object, for `upsert` | Only the listed fields are written. References are `sync_id`s. |
| `args`        | object, for named    | Operation arguments.                                          |
| `base_seq`    | int or null          | The row's `change_seq` the user last saw. Advisory.           |
| `depends_on`  | string[], optional   | File ids that were uploaded first.                            |
| `merge`       | bool, optional       | Account merge: match by content before inserting (§5.4).      |
| `position`    | int, optional        | The change's place in this install's outbox. It only grows.   |
| `client_time` | ISO 8601             | Display and logs only. Never used to order or resolve.        |

### 3.2 Processing

1. Validate the envelope. A bad envelope gets `422 invalid_request` and nothing
   is applied.
2. In one transaction, apply each change in array order, each in its own
   savepoint:
   1. Look up the receipt by `(scope, change_id)`. Same payload hash: return
      the stored ack as `duplicate`. Other hash: reject `change_id_reused`.
   2. Check the entity, op and fields against the entity spec (§5).
   3. Run the app's handler for the entity and op. The handler calls the app's
      domain actions, so validation and side effects stay in the app.
   4. Store the receipt with the ack.
3. Commit. After commit, send one poke for the scope (§4).
4. Build the pull page after the commit, so it includes this push.

A transient failure (database locked, timeout) ends the request with `5xx`. The
device resends the same batch. Receipts make that safe.

### 3.3 Response

```json
{
    "protocol": 1,
    "acks": [],
    "changes": [],
    "cursor": "opaque",
    "has_more": false,
    "last_position": 0,
    "server_time": "2026-10-20T09:58:14Z"
}
```

An **Ack**:

| Field       | Type                        | Notes                                                   |
| ----------- | --------------------------- | ------------------------------------------------------- |
| `change_id` | ULID                        |                                                         |
| `status`    | `applied`, `duplicate`, `rejected` |                                                  |
| `seq`       | int, when applied           | The `change_seq` the change produced.                   |
| `remap`     | `{from, to}`, optional      | The change landed on an existing row (§5.3).            |
| `overwrote` | string[], optional          | Fields that changed after `base_seq` and were overwritten. Telemetry only. |
| `error`     | `{code, message, fields?}`, when rejected |                                           |
| `current`   | row or null, when rejected  | The server's row now, so the device can restore it.      |

Rejection codes: `validation`, `deleted`, `parent_rejected`,
`read_only_field`, `unknown_entity`, `unknown_op`, `change_id_reused`,
`forbidden`.

A pulled **change**:

| Field    | Type                 | Notes                                    |
| -------- | -------------------- | ---------------------------------------- |
| `entity` | string               |                                          |
| `id`     | ULID                 |                                          |
| `seq`    | int                  |                                          |
| `op`     | `upsert` or `delete` |                                          |
| `row`    | object, for `upsert` | The full current row, as the entity spec serializes it. |

`last_position` is the highest `position` of this `client_id` the server has
applied or rejected. A device that still has an unacked change at or below it
does not apply the page: it sends the change again, gets its ack from the
receipt, and pulls again. So a lost answer cannot count a delta twice.

Pulled changes are ordered by `seq`. Each row appears once per page, with its
latest version.

### 3.4 HTTP errors

| Status | `error.code`       | Device action                                |
| ------ | ------------------ | -------------------------------------------- |
| 401    | —                  | Refresh the token once, then retry.          |
| 403    | `forbidden`        | Check the session.                           |
| 404    | —                  | Not a member: purge the scope locally.       |
| 409    | `resync_required`  | Mark and sweep resync.                       |
| 422    | `invalid_request`  | Client bug. Log it, stop syncing this scope. |
| 426    | `upgrade_required` | Show the update notice. Keep queuing.        |
| 429    | —                  | Back off as `Retry-After` says.              |
| 5xx    | —                  | Back off and resend the same batch.          |

### 3.5 Cursor

The cursor is encrypted with the app key: `{scope, seq, protocol}`. The
device stores it as an opaque string. A cursor for another scope gets `422`.

## 4. Real-time poke

- After a commit that assigned any `change_seq` for a scope, the server
  broadcasts one event on the private channel `sync.{scope_sync_id}`:
  `scope.changed` with `{seq, client_id}`. `client_id` is the pushing
  install, or null for web, jobs and imports.
- The poke carries no row data.
- The device pulls when `seq` is above its cursor. It ignores its own pokes that
  its last pull already covered. It pulls once after every connect.

## 5. Entity specs

Each app registers one spec per synced entity. The package enforces the spec;
the app supplies the handlers.

### 5.1 Fields

- **Writable fields:** the device may set them in `upsert`.
- **Server-owned fields:** the server sets them. A change that sets one is
  rejected with `read_only_field`.
- **References:** fields that hold another row's `sync_id`, with the target
  entity. The server maps them to its own keys.

### 5.2 Operations

- `upsert`: create the row if `id` is unknown, else patch the listed fields.
  On a tombstoned `id`: reject `deleted`.
- `delete`: delete the row. On an unknown or tombstoned `id`: `applied` (already
  gone).
- Named operations, such as `adjust {delta}`, for counters and other changes a
  field patch cannot express. They never merge on the device.

### 5.3 Matching rows

A spec may declare **match rules**. When an insert matches an existing row (for
example a room with the same name), the server applies the change to that row
and acks `applied` with `remap: {from, to}`: the device's `sync_id` and the
existing row's `sync_id`. The device rewrites every reference from `from` to
`to` in one transaction.

### 5.4 Account merge

A change with `merge: true` uses the spec's merge rules: match by `sync_id`
first, then by content. A match fills in only missing values and acks a remap.
Counters add up through their named operation.

## 6. Device obligations

- **One writer.** Every write goes through the store, which writes the row and
  its outbox entry in one transaction.
- **Outbox states.** `pending` entries to the same row merge (later values
  win). An unsent insert followed by a delete removes both. A `sending` entry is
  frozen until its ack. An acked entry is removed. A rejected entry moves to the
  rejected list with its values.
- **Order.** Entries are sent in their outbox order. Entries whose `depends_on`
  files are not uploaded wait.
- **Rebase on pull.** For each pulled row: write server values except fields
  with a pending upsert; add pending deltas to counter fields; keep a row with a
  pending delete hidden; a pulled tombstone deletes the row and drops its
  pending changes.
- **Pages.** Apply each page and store its cursor in one transaction.
- **Backfill.** A new entity or column pulls that entity from a null cursor,
  then sets the scope cursor to the lower of the two.
- **Resync.** Mark all rows of the scope, pull from null, delete unmarked
  rows without pending changes. Keep the outbox.

## 7. Fixtures

`fixtures/v1/*.json` pin the wire format. The PHP tests and the TypeScript tests
both read them. Change a fixture only together with this document.

Both repositories hold the same copy of this document and the fixtures.
sync-engine-laravel owns them: change them there first, then copy them to
sync-engine-typescript, whose CI fails while its copy differs.
