# Sync Engine for TypeScript

The client half of Sync Engine: local-first sync for an app that keeps its data
in SQLite on the device. The server half is the Laravel package
[sync-engine-laravel](https://github.com/hardimpactdev/sync-engine-laravel).

- **Store:** every write goes through one store, which writes the row and its
  outbox entry in one transaction. Pending outbox entries to the same row
  merge.
- **Sync client:** pushes the next outbox batch, applies the acks, and pulls the
  server's change feed. Pulled rows get the pending local changes laid over
  them until the server confirms them.
- **Scheduler:** syncs soon after a write, at once on a poke or when the app
  comes back, and backs off after a failure.
- **Realtime:** turns the app's socket pokes and connection state into
  scheduler triggers, with a timer while the socket is down.
- **React hooks (optional):** `useLocalQuery` reads local data and renders
  again when its tables change. Reads are synchronous, so a screen has its data
  on the first render.

It runs on any synchronous SQLite binding (expo-sqlite, node:sqlite,
better-sqlite3) and has no runtime dependencies.

A **scope** is the unit that syncs together and that a user may read: a team, a
household, a workspace. The server decides what a scope is.

The wire contract is [`docs/protocol-v1.md`](docs/protocol-v1.md). The JSON
fixtures in [`fixtures/v1/`](fixtures/v1) pin it. Both are owned by
sync-engine-laravel; CI fails when this copy differs.

> **Status:** 0.x. The API may change between minor versions.

## Requirements

- An ES module environment: Node 22+, a modern browser, or a bundler such as
  Metro or Vite. The package ships compiled JavaScript with type definitions.
- React 18+ only if you use `@hardimpactdev/sync-engine/react`.
- A server running [sync-engine-laravel](https://github.com/hardimpactdev/sync-engine-laravel).

## Install

```sh
npm install @hardimpactdev/sync-engine
```

## Set up the store

Wrap any synchronous SQLite binding in the four-method `SqlDatabase` adapter.
For expo-sqlite:

```ts
import { openDatabaseSync } from "expo-sqlite";
import type { SqlDatabase } from "@hardimpactdev/sync-engine";

const sqlite = openDatabaseSync("app.db");
const db: SqlDatabase = {
    exec: (sql) => sqlite.execSync(sql),
    run: (sql, params = []) => ({ changes: sqlite.runSync(sql, params).changes }),
    all: (sql, params = []) => sqlite.getAllSync(sql, params),
    get: (sql, params = []) => sqlite.getFirstSync(sql, params) ?? null,
};
```

[`tests/memory-database.ts`](tests/memory-database.ts) does the same for
node:sqlite. Each synced table has the convention columns
`sync_id`, the scope column, `server_id`, `server_seq`, `created_at`,
`updated_at` and `deleted_at`. Columns that start with `local_` never sync.

```ts
import { createLocalStore, createUlidFactory, migrate, syncEngineSchemaV1 } from "@hardimpactdev/sync-engine";
import { getRandomBytes } from "expo-crypto"; // or crypto.getRandomValues

migrate(db, [syncEngineSchemaV1, appSchemaV1]);

const store = createLocalStore(db, {
    tables: ["projects", "customers", "counters"],
    entities: {
        projects: { references: { customer_id: "customers" }, serverOwned: ["archived_at"] },
        counters: { operations: { adjust: { counters: [{ field: "quantity", delta: "delta" }] } } },
    },
    scopeColumn: "team_id",
    newId: createUlidFactory((count) => getRandomBytes(count)),
});

const id = store.insert("projects", { team_id: teamId, name: "Launch" });
store.update("projects", id, { name: "Launch v2" });
store.operate("counters", counterId, "adjust", { delta: 1 });
store.remove("projects", id);
```

The scope column is `scope_id` unless you pass `scopeColumn` (for example
`"team_id"`), like `syncScopeColumn()` on the Laravel model. A table without that
column, such as a child row, passes `scope: (row, db) => ...` in its entity spec
to say which scope a row belongs to.

Deleting a row the server never saw removes it with its queued changes. Rows
that point at it follow the reference column, as a foreign key would. A
nullable column is an optional link: it is set to null, in the row and in its
unsent changes, and the row stays. A `NOT NULL` column is a required link: a new
row that it made goes too. An operation's arguments follow the column of the
same name in the operation's own table: a nullable one is cleared and the
operation stays; otherwise the operation is dropped with its deltas.

After a commit the store calls the listeners of the changed tables. A listener
that throws cannot undo the write: the write returns as usual, and the other
listeners still run. The error goes to `onListenerError`. By default the store
throws it again in a microtask, where the app's global error handler sees it.

## Provisional results

Some changes are actions the server runs itself, such as cooking a meal, which
takes stock and writes movements on the server. The device sends only the
action, and never the rows the action makes. Until the server's rows arrive,
it can show the result it expects:

```ts
store.update("meals", mealId, { status: "consumed" }, {
    provisional: (local) => {
        local.insert("movements", { team_id: teamId, stock_id: flourId, delta: "-0.500" });
        local.add("stock", flourId, "quantity", "-0.500");
    },
});
```

`insert`, `update` and `operate` take the option. `add` works on counters only, the fields operations change, since no write may set them. The provisional writes have no
outbox entry. A pull that brings one of those rows keeps the provisional delta
on top, as it keeps queued counters. They go when the server has answered the
change:

- **Applied:** at the end of the next pull with `has_more: false`, which holds
  the server's own rows. During a pull of several pages, a pulled row can show
  both until the last page.
- **Rejected,** or dropped by a pulled tombstone or a cancelled parent: at once.

Removing a row keeps its unsent action, also when the server never saw the
row: the action, with the changes it builds on, goes before the delete, so the
server runs it once and nothing is taken back or counted twice. A row the server
never saw is kept the same way when a new row that needs it carries an action.

A provisional row is read-only. A write to it, a delta on it, a reference to it
or a row in it as a scope throws a `ProvisionalRowError`;
`store.isProvisional(table, id)` tells the app before. Whenever a row goes, by a
remove, a pulled tombstone, an acked delete, a refused insert, a resync sweep or
a purge, the provisional rows that need it follow as through a foreign key: an
optional link is cleared, a required link takes the provisional row along.
This needs `syncEngineSchemaV2`, a later migration step than
`syncEngineSchemaV1`. It adds the `sync_provisional` table and a `merge` column
on the outbox.

## Account merge

An app that copies rows into an account's scope, such as a guest's data at
sign-in, writes the copies with `{ merge: true }`. The change goes as an account
merge (protocol §5.4): the server lands it on a matching row and fills in only
what that row lacks. The flag is stored with the change, so every resend is the
same. Later writes to the row queue as normal changes and never merge into the
copy. This also needs `syncEngineSchemaV2`.

## Sync

The app supplies the transport, so authentication and the base URL stay in the
app:

```ts
import { createSyncClient, createSyncScheduler } from "@hardimpactdev/sync-engine";

const client = createSyncClient({
    store,
    scopeId: teamId, // the scope's sync_id
    clientId, // a ULID made once per install
    entities: ["projects", "customers", "counters"],
    transport: async (body) => {
        const response = await fetch(`${baseUrl}/api/teams/${teamId}/sync`, {
            method: "POST",
            headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
            body: JSON.stringify(body),
        });
        const retryAfter = Number(response.headers.get("Retry-After"));
        return { status: response.status, body: await response.json().catch(() => null), retryAfter: retryAfter || null };
    },
    onRejected: (changes) => showRejected(changes),
});

const scheduler = createSyncScheduler({ client, onOutcome: (outcome) => console.log(outcome.status) });

scheduler.trigger("start");
```

Call `scheduler.trigger("write")` after each local write. The scheduler waits
briefly after a write so a burst goes in one push, syncs at once on other
triggers (`foreground`, `network`, `poke`, `manual`) and backs off
after a failure. Call `scheduler.stop()` and `await scheduler.idle()` before you
close the database.

## Realtime

`createRealtimeSync` decides when a poke or a reconnect needs a pull. The app
owns the socket and passes in two things:

- `poke(event)`: each `scope.changed` event, `{ seq, client_id }`. A poke from
  this install, or with a seq this device has pulled already, does nothing.
  Otherwise it calls `scheduler.trigger("poke")`.
- `connection(state)`: `"connected"` or `"disconnected"`. Every change to
  connected calls `scheduler.trigger("network")`, to cover pokes missed while
  away. While disconnected, a timer triggers `"network"` every `intervalMs`
  (default 60 s). It starts disconnected.

With Laravel Echo and pusher-js (or Reverb):

```ts
import { createRealtimeSync } from "@hardimpactdev/sync-engine";

const realtime = createRealtimeSync({ scheduler, store, scopeId: teamId, clientId });
const connection = echo.connector.pusher.connection;
const state = (current: string) => realtime.connection(current === "connected" ? "connected" : "disconnected");

echo.private(`sync.${teamId}`).listen(".scope.changed", realtime.poke);
connection.bind("state_change", ({ current }: { current: string }) => state(current));
state(connection.state);
```

The package does not depend on Echo, pusher-js or any socket library. Call
`realtime.stop()` before `scheduler.stop()`.

## React

```tsx
import { LocalStoreContext, useLocalQuery } from "@hardimpactdev/sync-engine/react";

<LocalStoreContext.Provider value={store}>
    <Projects />
</LocalStoreContext.Provider>;

function Projects() {
    const projects = useLocalQuery(["projects"], (store) => store.all("SELECT * FROM projects WHERE deleted_at IS NULL ORDER BY name"), []);
    // ...
}
```

## Rules

- **Every write goes through the store,** so each row change has its outbox
  entry. Never write a synced table with raw SQL.
- **References are sync_ids,** never server integer ids.
- **A breaking wire change needs a new protocol number.** Change the protocol
  and fixtures in sync-engine-laravel first, then copy them here.

## Development

| Intent     | Command             |
| ---------- | ------------------- |
| Tests      | `npm test`          |
| Typecheck  | `npm run typecheck` |
| Build      | `npm run build`     |

To release, publish a GitHub release with the tag `vX.Y.Z`. The Publish workflow
sets that version and publishes it to npm with provenance.

## License

MIT. See [LICENSE](LICENSE).
