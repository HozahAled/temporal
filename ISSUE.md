**TL;DR:** `DatabaseHandle.reconnect(force=true)` closes the SQL connection pool *before* checking the refresh throttle. If the refresh is throttled, the pool is gone **and** not rebuilt. Because every SQL store in a service process shares this one handle (`common/persistence/sql/factory.go` — the ref-counted `mainDBConn`), a single sub-second transient error takes down **all SQL persistence for that host** (`no usable database connection found`) until the ~1s throttle window expires, and goroutines still holding the old pool fail with `sql: database is closed`. The handle's state machine self-heals ~1s after errors stop — but in real deployments the churn this creates is itself an outage-sustaining mechanism: each ~1/s forced rebuild spawns a fresh pool while the discarded pools drain asynchronously, which can exhaust server-side connection slots so `connect()` keeps failing (measured in #9747: a 2.5-minute database restart stretched into a 35-minute outage). This is how a "bounded dip" becomes the restart-required incidents operators report. Deterministic repro tests below; no real database required.

## Expected Behavior

A single transient connection error against a healthy database should not make the SQL persistence handle unavailable:

- `DatabaseHandle` should keep (or promptly rebuild) a usable pool.
- A refresh should not fail operations on the previously-borrowed pool.

## Actual Behavior

The pool is discarded before the throttle is consulted (`common/persistence/sql/sqlplugin/db_handle.go` — identical on `v1.31.0` and `main` @ `a31f4762`, so the line refs apply to both):

```go
h.db.Store(nil)          // :88
go prevConn.Close()      // :91  closes the whole *sqlx.DB
// ...
if now.Sub(lastRefresh) < sessionRefreshMinInternal { // :98  throttle checked AFTER the close
    // warn + return nil -> pool gone AND not rebuilt
}
```

This causes two defects.

### Defect 1 — the already-borrowed pool is invalidated

Stdlib `Close` lets queries that already started finish, but any goroutine that borrowed the previous `*sqlx.DB` fails on its next operation with `sql: database is closed`.

### Defect 2 — a healthy DB is reported unavailable without retrying `connect()`

If a refresh-triggering error lands inside the 1s throttle window (`sessionRefreshMinInternal`, `db_handle.go:24`, already marked `// TODO: this should be dynamic config.`):

- The pool is discarded (`:88`/`:91`) but the throttled branch returns without calling `connect()` — even though `connect()` would succeed immediately.
- Until the window expires, `DB()` returns `no usable database connection found`, and `Conn()` hands out an `invalidConn` whose every operation returns that error.
- Under a **burst** of connection errors this repeats: the first error closes the pool, subsequent ones are throttled out of rebuilding it, and a rebuild at window expiry is torn down again by the next error.

### Blast radius

The handle is process-wide: every SQL store created by the persistence factory — shard, execution, task, metadata, cluster metadata, queue, Nexus endpoint — borrows the **same** ref-counted `mainDBConn`, and therefore the same `DatabaseHandle` (`common/persistence/sql/factory.go`: `NewFactory` creates one `mainDBConn`; every `New*Store` calls `f.mainDBConn.Get()`). While the handle is in the trapped state, **all** SQL persistence operations on that host fail, not just the caller that hit the transient error.

### Scope / duration

Two layers to be precise about:

**The handle's state machine self-heals.** Pinned by the tests below: throttled teardowns do **not** advance `lastRefresh` (only an unthrottled pass through `reconnect` does, `:106`), so the recovery horizon stays frozen at `lastRefresh + 1s`. Once refresh-triggering errors stop and `connect()` succeeds, the first `DB()`/`Conn()` call after the horizon rebuilds the pool — worst-case ~1 throttle window. We adversarially stress-tested this (concurrent reconnect storms, queued force-teardowns landing after a rebuild, sustained-load bursts under `-race`): no restart-required wedge is constructible while `connect()` is healthy.

**In real deployments the churn sustains itself.** During a longer disturbance, `reconnect(force=true)` runs ~1/s per host; each cycle abandons a pool via `go prevConn.Close()` (in-flight queries drain asynchronously) while `connect()` builds a new one that ramps toward `MaxConns`. Across many hosts this multiplies the server-side connection footprint until `connect()` itself fails on exhausted slots — see #9747, where a 2.5-minute database restart became a 35-minute outage (~10,000 connections vs ~2,700 expected). On MySQL the loop is self-igniting: error 1040 "too many connections" is itself a refresh-triggering error. Restarting the pod frees all of its draining pools at once, which is why operators experience these incidents as "requires a restart to recover". Related field patterns with the same signature (`no usable database connection found` until restart): post-failover read-only nodes, where `connect()`'s `Ping` succeeds but the first write is a refresh-triggering error that tears the fresh pool down again; and credentials read once at process start failing after rotation.

So: the defect amplifies a sub-second transient against a healthy database into a host-wide availability dip, and under sustained disturbance its own reconnect churn can convert a short outage into a prolonged, restart-required one.

We recognize the eager close is intentional (the comment at `:89-90` aims to stop goroutines "slamming the now-unusable database"). But `database/sql` already discards bad connections per-connection via `driver.ErrBadConn`, so a single transient error does not imply the whole pool is poisoned — and discarding a working pool while refusing to rebuild it is the worse failure mode.

## Steps to Reproduce the Problem

Deterministic tests against the real `DatabaseHandle` via its existing constructor seams (injectable `connect` func and `clock.TimeSource`) — no real database required.

Branch: [`HozahAled/temporal@db-handle-reconnect-throttle-bug`](https://github.com/HozahAled/temporal/tree/db-handle-reconnect-throttle-bug) (exactly one test file added on top of `a31f4762`; production code untouched), file `common/persistence/sql/sqlplugin/db_handle_bug_test.go`:

```
go test -run TestDBHandleBug -v ./common/persistence/sql/sqlplugin/
```

| Test | Result | Shows |
|---|---|---|
| `TestDBHandleBug_EXPECTED_HealthyDBStaysUsable` | **FAILS** (red test) | Asserts the desired behavior; fails on current code — see output below |
| `TestDBHandleBug_ThrottleTrap_UnavailableDespiteHealthyDB` | passes | After one transient error inside the window, the handle is unavailable and `connect()` is never retried |
| `TestDBHandleBug_ForceCloseInvalidatesHeldPool` | passes | An operation on the previously-borrowed pool gets `sql: database is closed` |
| `TestDBHandleBug_SustainedWedgeRequiresFailingConnect` | passes | Pins the scope above: prolonged unavailability only while `connect()` fails; recovery on the first call after it succeeds |

Red-test output on current code:

```
Error:      Received unexpected error:
            no usable database connection found
Messages:   a single transient error must not make a healthy database unavailable
```

## Suggested fix

Either makes the red test pass:

1. **Swap-then-close**: build the replacement pool first; `Close()` the old one only after a successful `connect()`. (Trade-off: brief overlap of up to 2× `MaxConns` against the DB while the new pool is built.)
2. **Keep the existing pool when throttled** instead of discarding it.

Independently, `sessionRefreshMinInternal` could become dynamic config per its existing TODO.

## Related history

- #5926 introduced this reconnect/refresh design.
- #6514 ("Stuck Temporal Server", same error string) was a *different* bug in the *same* throttle logic — `lastRefresh` was advanced even when throttled, so the handle never reconnected. Fixed by #6538 (+ tests in #6652). That fix did **not** move the pool close after the throttle check, which is the defect reported here.
- #9747 documents the field blast of the reconnect churn: pool-per-second rebuilds across 14 hosts exhausted the database's connection slots and turned a 2.5-minute DB restart into a 35-minute outage. The open fix #9891 addresses the churn; the close-before-throttle defect here is the mechanism that discards each pool in the first place, so the two are complementary.
- #8202 (open) is a *boot-time* variant — initial `connect()` fails before the node network is ready and the schema-version check crash-loops the pod (fix in open PR #9878) — not the steady-state trap reported here, but the same eager-discard philosophy.

## Specifications

- **Version**: observed on Temporal Server v1.31.0; reproduced on `main` @ `a31f4762` (`db_handle.go` unchanged between them)
- **Platform**: observed with PostgreSQL (`postgres12_pgx`); the code path and repro are DB-agnostic
