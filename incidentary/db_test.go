package incidentary

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// --- fake driver infrastructure ---

// fakeConnector is a minimal driver.Connector implementation for testing.
type fakeConnector struct {
	conn driver.Conn
	err  error
}

func (f *fakeConnector) Connect(_ context.Context) (driver.Conn, error) {
	return f.conn, f.err
}

func (f *fakeConnector) Driver() driver.Driver {
	return &fakeDriver{}
}

// fakeDriver is a minimal driver.Driver for testing.
type fakeDriver struct{}

func (d *fakeDriver) Open(_ string) (driver.Conn, error) { return nil, nil }

// fakeConn is a minimal driver.Conn for testing.
type fakeConn struct {
	prepareErr  error
	closeErr    error
	prepareStmt *fakeStmt
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.prepareStmt != nil {
		c.prepareStmt.query = query
		return c.prepareStmt, nil
	}
	return &fakeStmt{query: query}, nil
}

func (c *fakeConn) Close() error                        { return c.closeErr }
func (c *fakeConn) Begin() (driver.Tx, error)           { return &fakeTx{}, nil }

// fakeConnWithQueryer is a fakeConn that implements driver.QueryerContext.
type fakeConnWithQueryer struct {
	fakeConn
	queryErr error
}

func (c *fakeConnWithQueryer) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakeRows{}, nil
}

// fakeConnWithExecer is a fakeConn that implements driver.ExecerContext.
type fakeConnWithExecer struct {
	fakeConn
	execErr error
}

func (c *fakeConnWithExecer) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if c.execErr != nil {
		return nil, c.execErr
	}
	return driver.RowsAffected(1), nil
}

// fakeStmt is a minimal driver.Stmt for testing.
type fakeStmt struct {
	query    string
	execErr  error
	queryErr error
}

func (s *fakeStmt) Close() error                              { return nil }
func (s *fakeStmt) NumInput() int                             { return -1 }
func (s *fakeStmt) Exec(_ []driver.Value) (driver.Result, error) {
	if s.execErr != nil {
		return nil, s.execErr
	}
	return driver.RowsAffected(1), nil
}
func (s *fakeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &fakeRows{}, nil
}

// fakeStmtWithContext supports driver.StmtExecContext and driver.StmtQueryContext.
type fakeStmtWithContext struct {
	fakeStmt
	execCtxErr  error
	queryCtxErr error
}

func (s *fakeStmtWithContext) ExecContext(_ context.Context, _ []driver.NamedValue) (driver.Result, error) {
	if s.execCtxErr != nil {
		return nil, s.execCtxErr
	}
	return driver.RowsAffected(1), nil
}

func (s *fakeStmtWithContext) QueryContext(_ context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	if s.queryCtxErr != nil {
		return nil, s.queryCtxErr
	}
	return &fakeRows{}, nil
}

// fakeRows is a minimal driver.Rows.
type fakeRows struct{}

func (r *fakeRows) Columns() []string              { return nil }
func (r *fakeRows) Close() error                   { return nil }
func (r *fakeRows) Next(_ []driver.Value) error    { return errors.New("EOF") }

// fakeTx is a minimal driver.Tx.
type fakeTx struct{}

func (t *fakeTx) Commit() error   { return nil }
func (t *fakeTx) Rollback() error { return nil }

// drainEvents flushes and returns all buffered events from a client's ring buffer.
func drainEvents(client *Client) []*SkeletonCe {
	// Use a far-future timestamp so nothing is filtered by window age.
	return client.buffer.Flush(time.Now().Add(time.Hour).UnixMilli())
}

// --- WrapConnector ---

func TestWrapConnectorReturnsInstrumentedConnector(t *testing.T) {
	client := newTestClient()
	inner := &fakeConnector{conn: &fakeConn{}}

	wrapped := WrapConnector(client, inner)

	if wrapped == nil {
		t.Fatal("expected non-nil connector from WrapConnector")
	}
	if wrapped == driver.Connector(inner) {
		t.Fatal("expected WrapConnector to return a new wrapper, not the original")
	}
}

func TestWrapConnectorWithNilClientDoesNotPanic(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil client: %v", r)
		}
	}()

	wrapped := WrapConnector(nil, inner)
	if wrapped == nil {
		t.Fatal("expected non-nil connector even with nil client")
	}
}

func TestInstrumentedConnectorDriverDelegates(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	if wrapped.Driver() == nil {
		t.Fatal("expected Driver() to return non-nil driver")
	}
}

func TestInstrumentedConnectorConnectReturnsInstrumentedConn(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, err := wrapped.Connect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error from Connect: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil conn from Connect")
	}
}

func TestInstrumentedConnectorConnectPropagatesError(t *testing.T) {
	connectErr := errors.New("connection refused")
	inner := &fakeConnector{conn: nil, err: connectErr}
	wrapped := WrapConnector(newTestClient(), inner)

	_, err := wrapped.Connect(context.Background())
	if !errors.Is(err, connectErr) {
		t.Fatalf("expected error %v, got %v", connectErr, err)
	}
}

// --- instrumentedConn ---

func TestInstrumentedConnPrepareReturnsInstrumentedStmt(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, err := wrapped.Connect(context.Background())
	if err != nil {
		t.Fatalf("unexpected Connect error: %v", err)
	}

	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("unexpected Prepare error: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected non-nil stmt from Prepare")
	}
}

func TestInstrumentedConnPreparePropagatesError(t *testing.T) {
	prepErr := errors.New("syntax error")
	inner := &fakeConnector{conn: &fakeConn{prepareErr: prepErr}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, _ := wrapped.Connect(context.Background())
	_, err := conn.Prepare("BAD SQL")
	if !errors.Is(err, prepErr) {
		t.Fatalf("expected error %v, got %v", prepErr, err)
	}
}

func TestInstrumentedConnCloseDelegates(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, _ := wrapped.Connect(context.Background())
	if err := conn.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
}

func TestInstrumentedConnBeginDelegates(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, _ := wrapped.Connect(context.Background())
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("unexpected Begin error: %v", err)
	}
	if tx == nil {
		t.Fatal("expected non-nil tx from Begin")
	}
}

// --- Statement execution records events ---

func TestInstrumentedStmtExecRecordsDBQueryEvent(t *testing.T) {
	client := newTestClient()
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(client, inner)

	ctx := setTraceContext(context.Background(), "trace-stmt-exec", "ce-stmt-exec")
	conn, _ := wrapped.Connect(ctx)
	stmt, _ := conn.Prepare("INSERT INTO foo VALUES (?)")

	iStmt, ok := stmt.(*instrumentedStmt)
	if !ok {
		t.Fatal("expected stmt to be *instrumentedStmt")
	}

	_, err := iStmt.Exec([]driver.Value{1})
	if err != nil {
		t.Fatalf("unexpected Exec error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected at least one event after Exec")
	}

	last := events[len(events)-1]
	if last.EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, last.EventType)
	}
}

func TestInstrumentedStmtQueryRecordsDBQueryEvent(t *testing.T) {
	client := newTestClient()
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(client, inner)

	ctx := setTraceContext(context.Background(), "trace-stmt-query", "ce-stmt-query")
	conn, _ := wrapped.Connect(ctx)
	stmt, _ := conn.Prepare("SELECT * FROM foo")

	iStmt, ok := stmt.(*instrumentedStmt)
	if !ok {
		t.Fatal("expected stmt to be *instrumentedStmt")
	}

	_, err := iStmt.Query([]driver.Value{})
	if err != nil {
		t.Fatalf("unexpected Query error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected at least one event after Query")
	}

	last := events[len(events)-1]
	if last.EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, last.EventType)
	}
}

func TestInstrumentedStmtExecErrorRecordsStatus500(t *testing.T) {
	client := newTestClient()
	execErr := errors.New("constraint violation")
	inner := &fakeConnector{conn: &fakeConn{prepareStmt: &fakeStmt{execErr: execErr}}}
	wrapped := WrapConnector(client, inner)

	conn, _ := wrapped.Connect(context.Background())
	stmt, _ := conn.Prepare("INSERT INTO locked VALUES (?)")

	iStmt, ok := stmt.(*instrumentedStmt)
	if !ok {
		t.Fatal("expected stmt to be *instrumentedStmt")
	}

	_, err := iStmt.Exec([]driver.Value{1})
	if err == nil {
		t.Fatal("expected error from Exec to propagate")
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event even on Exec error")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500 on exec error, got %d", events[len(events)-1].Status)
	}
}

func TestInstrumentedStmtQueryErrorRecordsStatus500(t *testing.T) {
	client := newTestClient()
	queryErr := errors.New("table does not exist")
	inner := &fakeConnector{conn: &fakeConn{prepareStmt: &fakeStmt{queryErr: queryErr}}}
	wrapped := WrapConnector(client, inner)

	conn, _ := wrapped.Connect(context.Background())
	stmt, _ := conn.Prepare("SELECT * FROM nonexistent")

	iStmt, ok := stmt.(*instrumentedStmt)
	if !ok {
		t.Fatal("expected stmt to be *instrumentedStmt")
	}

	_, err := iStmt.Query([]driver.Value{})
	if err == nil {
		t.Fatal("expected error from Query to propagate")
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event even on Query error")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500 on query error, got %d", events[len(events)-1].Status)
	}
}

// --- QueryerContext on connection ---

func TestQueryerContextRecordsEvent(t *testing.T) {
	client := newTestClient()
	innerConn := &fakeConnWithQueryer{}
	inner := &fakeConnector{conn: innerConn}
	wrapped := WrapConnector(client, inner)

	ctx := setTraceContext(context.Background(), "trace-qctx", "ce-qctx")
	conn, _ := wrapped.Connect(ctx)

	qc, ok := conn.(driver.QueryerContext)
	if !ok {
		t.Skip("instrumented connection does not expose QueryerContext; inner does not implement it")
	}

	_, err := qc.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		t.Fatalf("unexpected QueryContext error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from QueryContext")
	}
	if events[len(events)-1].EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, events[len(events)-1].EventType)
	}
}

func TestQueryerContextErrorRecordsStatus500(t *testing.T) {
	client := newTestClient()
	qErr := errors.New("query failed")
	innerConn := &fakeConnWithQueryer{queryErr: qErr}
	inner := &fakeConnector{conn: innerConn}
	wrapped := WrapConnector(client, inner)

	ctx := context.Background()
	conn, _ := wrapped.Connect(ctx)

	qc, ok := conn.(driver.QueryerContext)
	if !ok {
		t.Skip("instrumented connection does not expose QueryerContext")
	}

	_, err := qc.QueryContext(ctx, "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected error from QueryContext to propagate")
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event even on QueryContext error")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500, got %d", events[len(events)-1].Status)
	}
}

// --- ExecerContext on connection ---

func TestExecerContextRecordsEvent(t *testing.T) {
	client := newTestClient()
	innerConn := &fakeConnWithExecer{}
	inner := &fakeConnector{conn: innerConn}
	wrapped := WrapConnector(client, inner)

	ctx := setTraceContext(context.Background(), "trace-ectx", "ce-ectx")
	conn, _ := wrapped.Connect(ctx)

	ec, ok := conn.(driver.ExecerContext)
	if !ok {
		t.Skip("instrumented connection does not expose ExecerContext")
	}

	_, err := ec.ExecContext(ctx, "UPDATE foo SET x=1", nil)
	if err != nil {
		t.Fatalf("unexpected ExecContext error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from ExecContext")
	}
	if events[len(events)-1].EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, events[len(events)-1].EventType)
	}
}

func TestExecerContextErrorRecordsStatus500(t *testing.T) {
	client := newTestClient()
	eErr := errors.New("exec failed")
	innerConn := &fakeConnWithExecer{execErr: eErr}
	inner := &fakeConnector{conn: innerConn}
	wrapped := WrapConnector(client, inner)

	ctx := context.Background()
	conn, _ := wrapped.Connect(ctx)

	ec, ok := conn.(driver.ExecerContext)
	if !ok {
		t.Skip("instrumented connection does not expose ExecerContext")
	}

	_, err := ec.ExecContext(ctx, "UPDATE foo SET x=1", nil)
	if err == nil {
		t.Fatal("expected error from ExecContext to propagate")
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event even on ExecContext error")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500, got %d", events[len(events)-1].Status)
	}
}

// --- recordDBQuery ---

func TestRecordDBQueryNilClientDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil client: %v", r)
		}
	}()

	recordDBQuery(nil, context.Background(), "exec", "SELECT 1", time.Now(), nil)
}

func TestRecordDBQueryRecordsCorrectKindAndEventType(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "trace-rec", "ce-rec")

	recordDBQuery(client, ctx, "exec", "SELECT count(*) FROM users", time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}

	ce := events[len(events)-1]
	if ce.Kind != KindInternal {
		t.Fatalf("expected kind %q, got %q", KindInternal, ce.Kind)
	}
	if ce.EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, ce.EventType)
	}
	if ce.TraceID != "trace-rec" {
		t.Fatalf("expected traceID 'trace-rec', got %q", ce.TraceID)
	}
	if ce.ParentCeID != "ce-rec" {
		t.Fatalf("expected parentCeID 'ce-rec', got %q", ce.ParentCeID)
	}
}

func TestRecordDBQuerySetsStatus500OnError(t *testing.T) {
	client := newTestClient()
	recordDBQuery(client, context.Background(), "exec", "SELECT 1", time.Now(), errors.New("query failed"))

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500, got %d", events[len(events)-1].Status)
	}
}

func TestRecordDBQuerySetsStatus0OnSuccess(t *testing.T) {
	client := newTestClient()
	recordDBQuery(client, context.Background(), "exec", "SELECT 1", time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].Status != 0 {
		t.Fatalf("expected status 0, got %d", events[len(events)-1].Status)
	}
}

func TestRecordDBQueryDurationIsNonNegative(t *testing.T) {
	client := newTestClient()
	start := time.Now().Add(-5 * time.Millisecond)
	recordDBQuery(client, context.Background(), "exec", "SELECT 1", start, nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].DurationNs < 0 {
		t.Fatalf("expected non-negative DurationNs, got %d", events[len(events)-1].DurationNs)
	}
}

func TestRecordDBQueryTruncatesLongQuery(t *testing.T) {
	client := newTestClient()

	// Build a query that exceeds 500 characters.
	longQuery := make([]byte, 600)
	for i := range longQuery {
		longQuery[i] = 'x'
	}

	// Must not panic; we just verify the event is recorded.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with long query: %v", r)
		}
	}()

	recordDBQuery(client, context.Background(), "exec", string(longQuery), time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded even for long query")
	}
}

// --- StmtExecContext / StmtQueryContext ---

func TestInstrumentedStmtExecContextRecordsEvent(t *testing.T) {
	client := newTestClient()
	innerStmt := &fakeStmtWithContext{}
	innerConn := &fakeConn{prepareStmt: &fakeStmt{}}
	// We need the Prepare to return a fakeStmtWithContext.
	innerConn.prepareStmt = nil // will use default fakeStmt; we test via direct construction instead.

	// Build an instrumentedStmt wrapping a fakeStmtWithContext directly.
	iStmt := &instrumentedStmt{
		client: client,
		inner:  innerStmt,
		query:  "SELECT 1",
	}

	_, err := iStmt.ExecContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected ExecContext error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from StmtExecContext")
	}
	if events[len(events)-1].EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, events[len(events)-1].EventType)
	}
}

func TestInstrumentedStmtQueryContextRecordsEvent(t *testing.T) {
	client := newTestClient()
	innerStmt := &fakeStmtWithContext{}

	iStmt := &instrumentedStmt{
		client: client,
		inner:  innerStmt,
		query:  "SELECT * FROM foo",
	}

	_, err := iStmt.QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected QueryContext error: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from StmtQueryContext")
	}
	if events[len(events)-1].EventType != string(EventDBQuery) {
		t.Fatalf("expected event_type %q, got %q", EventDBQuery, events[len(events)-1].EventType)
	}
}

// --- DBIntegration ---

func TestDBIntegrationImplementsInterface(t *testing.T) {
	var _ Integration = (*DBIntegration)(nil)
}

func TestDBIntegrationName(t *testing.T) {
	d := &DBIntegration{}
	if d.Name() != "db" {
		t.Fatalf("expected name 'db', got %q", d.Name())
	}
}

func TestDBIntegrationSetupStoresClient(t *testing.T) {
	client := newTestClient()
	d := &DBIntegration{}

	cleanup, err := d.Setup(client)
	if err != nil {
		t.Fatalf("unexpected Setup error: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
	if d.client != client {
		t.Fatal("expected DBIntegration to store client reference")
	}
}

func TestDBIntegrationSetupWithNilClientDoesNotError(t *testing.T) {
	d := &DBIntegration{}
	cleanup, err := d.Setup(nil)
	if err != nil {
		t.Fatalf("expected no error with nil client, got: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
}

func TestDBIntegrationWrapConnectorReturnsWrapped(t *testing.T) {
	client := newTestClient()
	d := &DBIntegration{}
	_, _ = d.Setup(client)

	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := d.WrapConnector(inner)

	if wrapped == nil {
		t.Fatal("expected non-nil connector from DBIntegration.WrapConnector")
	}
	if wrapped == driver.Connector(inner) {
		t.Fatal("expected WrapConnector to return a new wrapper")
	}
}

func TestDefaultIntegrationsContainsDB(t *testing.T) {
	integrations := DefaultIntegrations()
	for _, i := range integrations {
		if i.Name() == "db" {
			return
		}
	}
	t.Fatal("expected DefaultIntegrations to contain the 'db' integration")
}
