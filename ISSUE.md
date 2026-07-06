**TL;DR:** `DatabaseHandle.reconnect(force=true)` closes the SQL connection pool *before* checking the refresh throttle. If the refresh is throttled, the pool is gone **and** not rebuilt. Because every SQL store in a service process shares this one handle (`common/persistence/sql/factory.go` — the ref-counted `mainDBConn`), a single sub-second transient error takes down **all SQL persistence for that host** (`no usable database connection found`) until the ~1s throttle window expires, and goroutines still holding the old pool fail with `sql: database is closed`. The trap is self-healing: on its own it is a bounded availability dip, **not a prolonged outage** — sustained unavailability additionally requires `connect()` to keep failing or a continuing burst of refresh-triggering errors. Deterministic repro tests below; no real database required.

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

Pinned by the tests below: the throttle self-heals within ~1 refresh interval once errors pause — a *prolonged* outage additionally requires `connect()` to keep failing (i.e., a real database outage) or a sustained burst of refresh-triggering errors, each of which tears down the freshly rebuilt pool. So this is a bounded availability dip, not a permanent wedge; the defect is that a sub-second transient against a healthy database is amplified into a host-wide, visible one, and it may compound with upstream retry behavior.

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
- #8202 (open): unrecoverable `no usable database connection found` after node replacement — possibly field evidence of this failure mode compounding.

## Specifications

- **Version**: observed on Temporal Server v1.31.0; reproduced on `main` @ `a31f4762` (`db_handle.go` unchanged between them)
- **Platform**: observed with PostgreSQL (`postgres12_pgx`); the code path and repro are DB-agnostic
