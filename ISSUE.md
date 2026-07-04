## Expected Behavior

A single transient connection error against a healthy database should not make the SQL persistence handle unavailable: `DatabaseHandle` should keep (or promptly rebuild) a usable pool, and a refresh should not fail operations on the previously-borrowed pool.

## Actual Behavior

`DatabaseHandle.reconnect(force=true)` closes the connection pool **before** checking the refresh throttle (`common/persistence/sql/sqlplugin/db_handle.go` — identical on `v1.31.0` and `main` @ `a31f4762`, so the line refs apply to both):

```go
h.db.Store(nil)          // :88
go prevConn.Close()      // :91  closes the whole *sqlx.DB
// ...
if now.Sub(lastRefresh) < sessionRefreshMinInternal { // :98  throttle checked AFTER the close
    // warn + return nil -> pool gone AND not rebuilt
}
```

Two defects:

1. **Operations issued on the already-borrowed pool fail with `sql: database is closed`.** Stdlib `Close` lets queries that already started finish, but any goroutine that borrowed the previous `*sqlx.DB` fails on its next operation.
2. **A healthy DB is reported unavailable without retrying `connect()`.** If a refresh-triggering error lands inside the 1s `sessionRefreshMinInternal` window (`db_handle.go:24`, already marked `// TODO: this should be dynamic config.`), the pool is discarded and not rebuilt: `DB()` returns `no usable database connection found` (and `Conn()` hands out an `invalidConn` whose every operation returns it) for up to ~1s, even though `connect()` would succeed immediately. Under a burst of connection errors this repeats: the first error closes the pool, subsequent ones are throttled out of rebuilding it, and a rebuild at window expiry is torn down again by the next error.

Scoping (pinned by the tests below): the throttle self-heals within ~1 refresh interval once errors pause — a prolonged outage additionally requires `connect()` to keep failing. The defects amplify a sub-second transient into a visible availability dip, and may compound with upstream retry behavior.

We recognize the eager close is intentional (the comment at `:89-90` aims to stop goroutines "slamming the now-unusable database"). But `database/sql` already discards bad connections per-connection via `driver.ErrBadConn`, so a single transient error does not imply the whole pool is poisoned — and discarding a working pool while refusing to rebuild it is the worse failure mode.

## Steps to Reproduce the Problem

Deterministic tests against the real `DatabaseHandle` via its injectable clock/connect seams — no real database required. Branch [`HozahAled/temporal@db-handle-reconnect-throttle-bug`](https://github.com/HozahAled/temporal/tree/db-handle-reconnect-throttle-bug), file `common/persistence/sql/sqlplugin/db_handle_bug_test.go`:

```
go test -run TestDBHandleBug -v ./common/persistence/sql/sqlplugin/
```

- `TestDBHandleBug_EXPECTED_HealthyDBStaysUsable` — **red test** asserting the desired behavior; fails on current code with:

  ```
  Error:      Received unexpected error:
              no usable database connection found
  Messages:   a single transient error must not make a healthy database unavailable
  ```

- `TestDBHandleBug_ThrottleTrap_UnavailableDespiteHealthyDB` — passes: after one transient error inside the window, the handle is unavailable and `connect()` is never retried.
- `TestDBHandleBug_ForceCloseInvalidatesHeldPool` — passes: an operation on the previously-borrowed pool gets `sql: database is closed`.
- `TestDBHandleBug_SustainedWedgeRequiresFailingConnect` — passes: pins the scoping above (prolonged unavailability only while `connect()` fails; recovery on the first call after it succeeds).

## Suggested fix

Either makes the red test pass:

1. **Swap-then-close**: build the replacement pool first; `Close()` the old one only after a successful `connect()`. (Trade-off: brief overlap of up to 2× `MaxConns` against the DB while the new pool is built.)
2. **Keep the existing pool when throttled** instead of discarding it.

Independently, `sessionRefreshMinInternal` could become dynamic config per its existing TODO.

## Specifications

- Version: observed on Temporal Server v1.31.0; reproduced on `main` @ `a31f4762` (`db_handle.go` unchanged between them)
- Platform: observed with PostgreSQL (`postgres12_pgx`); the code path and repro are DB-agnostic
