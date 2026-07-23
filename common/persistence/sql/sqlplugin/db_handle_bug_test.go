package sqlplugin

// Tests for the DatabaseHandle refresh behavior: a transient connection error
// against a healthy database must not leave the handle without a pool. The
// throttled path keeps the existing pool, refreshes swap-then-close, and a
// genuinely unreachable database still fails fast (pool discarded while
// connect() keeps failing). All tests pass with the fix on this branch.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// errTrigger is treated by the injected needsRefresh as a connection-level
// error, driving reconnect(true) exactly like a real bad-conn/EOF/reset.
var errTrigger = errors.New("simulated bad connection")

// Minimal fake driver: once the pool is Close()d, the next operation yields
// stdlib "sql: database is closed"; no real SQL is exercised.
type bugConnector struct{}

func (bugConnector) Connect(context.Context) (driver.Conn, error) { return bugConn{}, nil }
func (bugConnector) Driver() driver.Driver                        { return nil }

type bugConn struct{}

func (bugConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (bugConn) Close() error                        { return nil }
func (bugConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (bugConn) Exec(string, []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func openFakeDB() *sqlx.DB { return sqlx.NewDb(sql.OpenDB(bugConnector{}), "postgres") }

func newHandle(t *testing.T, ts *clock.EventTimeSource, connect func() (*sqlx.DB, error)) *DatabaseHandle {
	t.Helper()
	needsRefresh := func(err error) bool { return errors.Is(err, errTrigger) }
	h := NewDatabaseHandle(DbKindUnknown, connect, needsRefresh,
		log.NewNoopLogger(), metrics.NoopMetricsHandler, ts)
	require.NotNil(t, h)
	t.Cleanup(h.Close)
	return h
}

// A successful refresh swaps in the new pool, then closes the previous one:
// an operation issued on the already-borrowed old pool fails with stdlib
// "sql: database is closed", but the handle itself is never without a pool.
func TestDBHandleBug_ForceCloseInvalidatesHeldPool(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	h := newHandle(t, ts, func() (*sqlx.DB, error) { return openFakeDB(), nil })

	p0, err := h.DB()
	require.NoError(t, err)
	_, execErr := p0.Exec("select 1")
	require.NoError(t, execErr)

	ts.Advance(2 * sessionRefreshMinInternal) // ensure this refresh is not throttled
	_ = h.ConvertError(errTrigger)            // reconnect(true): swap + async close

	assert.Eventually(t, func() bool {
		_, e := p0.Exec("select 1")
		return e != nil && strings.Contains(e.Error(), "sql: database is closed")
	}, time.Second, 2*time.Millisecond,
		"operation on the previously-borrowed pool should fail once it is closed")

	// The handle itself rebuilt a fresh, usable pool (not throttled here).
	p1, err := h.DB()
	require.NoError(t, err)
	_, execErr = p1.Exec("select 1")
	assert.NoError(t, execErr)
}

// A transient error inside the 1s throttle window keeps the existing pool:
// no discard, no connect() churn, the handle stays usable.
func TestDBHandleBug_ThrottledRefreshKeepsPool(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	connects := 0
	h := newHandle(t, ts, func() (*sqlx.DB, error) { connects++; return openFakeDB(), nil })

	require.Equal(t, 1, connects, "one connect at construction")
	_, err := h.DB()
	require.NoError(t, err)

	ts.Advance(100 * time.Millisecond) // inside the throttle window
	_ = h.ConvertError(errTrigger)     // pool closed, then throttle skips the rebuild

	db, err := h.DB()
	assert.NoError(t, err)
	assert.NotNil(t, db, "throttled refresh must keep the existing pool")
	assert.Equal(t, 1, connects, "no connect() churn while throttled")
}

// A sustained outage requires connect() itself to keep failing; the handle
// recovers on the first call after connectivity returns, with no restart.
func TestDBHandleBug_SustainedWedgeRequiresFailingConnect(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	failures := 0
	dbDown := true
	connect := func() (*sqlx.DB, error) {
		if dbDown {
			failures++
			return nil, errTrigger
		}
		return openFakeDB(), nil
	}
	h := newHandle(t, ts, connect) // construction attempt fails (dbDown)

	for i := 0; i < 3; i++ {
		ts.Advance(sessionRefreshMinInternal + time.Millisecond)
		_, err := h.DB()
		require.ErrorIs(t, err, DatabaseUnavailableError)
	}
	require.Equal(t, 4, failures, "every well-spaced attempt retried connect() and failed")

	dbDown = false
	ts.Advance(sessionRefreshMinInternal + time.Millisecond)
	db, err := h.DB()
	assert.NoError(t, err)
	assert.NotNil(t, db)
}

// TestDBHandleBug_EXPECTED_HealthyDBStaysUsable is the red test from the bug
// report: a single transient error inside the throttle window used to discard
// the pool without retrying connect(). With the fix it passes.
func TestDBHandleBug_EXPECTED_HealthyDBStaysUsable(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	h := newHandle(t, ts, func() (*sqlx.DB, error) { return openFakeDB(), nil }) // never fails

	_, err := h.DB()
	require.NoError(t, err)

	ts.Advance(100 * time.Millisecond) // inside the throttle window
	_ = h.ConvertError(errTrigger)     // one transient error; the DB itself is fine

	db, err := h.DB()
	require.NoError(t, err, "a single transient error must not make a healthy database unavailable")
	require.NotNil(t, db)
}
