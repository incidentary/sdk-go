package incidentary

import (
	"context"
	"database/sql/driver"
	"testing"
)

func driverTxOptions() driver.TxOptions { return driver.TxOptions{} }
type driverNamedValue = driver.NamedValue

// --- QueueIntegration ---

func TestQueueIntegrationImplementsInterface(t *testing.T) {
	var _ Integration = (*QueueIntegration)(nil)
}

func TestQueueIntegrationName(t *testing.T) {
	q := &QueueIntegration{}
	if q.Name() != "queue" {
		t.Fatalf("expected name 'queue', got %q", q.Name())
	}
}

func TestQueueIntegrationSetupStoresClientAndReturnsNilCleanup(t *testing.T) {
	client := newTestClient()
	q := &QueueIntegration{}

	cleanup, err := q.Setup(client)
	if err != nil {
		t.Fatalf("unexpected Setup error: %v", err)
	}
	if cleanup != nil {
		t.Fatal("expected nil cleanup from QueueIntegration.Setup")
	}
	if q.client != client {
		t.Fatal("expected QueueIntegration to store client reference")
	}
}

func TestQueueIntegrationSetupWithNilClientDoesNotError(t *testing.T) {
	q := &QueueIntegration{}
	cleanup, err := q.Setup(nil)
	if err != nil {
		t.Fatalf("expected no error with nil client, got: %v", err)
	}
	if cleanup != nil {
		t.Fatal("expected nil cleanup")
	}
}

func TestQueueIntegrationConsumerWrapsHandler(t *testing.T) {
	client := newTestClient()
	q := &QueueIntegration{}
	_, _ = q.Setup(client)

	var handlerCalled bool
	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		handlerCalled = true
		return nil
	}

	wrapped := q.Consumer(handler)
	if wrapped == nil {
		t.Fatal("expected non-nil wrapped consumer")
	}

	_ = wrapped(context.Background(), QueueHeaders{}, []byte("test"))
	if !handlerCalled {
		t.Fatal("expected inner handler to be called")
	}
}

func TestQueueIntegrationInjectHeadersAddsTraceContext(t *testing.T) {
	client := newTestClient()
	q := &QueueIntegration{}
	_, _ = q.Setup(client)

	ctx := setTraceContext(context.Background(), "trace-queue-inject", "ce-queue-inject")
	result := q.InjectHeaders(ctx, QueueHeaders{})

	if result[TraceIDHeader] != "trace-queue-inject" {
		t.Fatalf("expected trace header 'trace-queue-inject', got %q", result[TraceIDHeader])
	}
}

func TestQueueIntegrationInjectHeadersWithEmptyContextReturnsEmptyHeaders(t *testing.T) {
	client := newTestClient()
	q := &QueueIntegration{}
	_, _ = q.Setup(client)

	result := q.InjectHeaders(context.Background(), QueueHeaders{})
	if result[TraceIDHeader] != "" {
		t.Fatalf("expected no trace header when context is empty, got %q", result[TraceIDHeader])
	}
}

func TestDefaultIntegrationsContainsQueue(t *testing.T) {
	integrations := DefaultIntegrations()
	for _, i := range integrations {
		if i.Name() == "queue" {
			return
		}
	}
	t.Fatal("expected DefaultIntegrations to contain the 'queue' integration")
}

// --- DB integration extra coverage ---

func TestInstrumentedConnBeginTxFallsBackToBeginWhenNotSupported(t *testing.T) {
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(newTestClient(), inner)

	conn, _ := wrapped.Connect(context.Background())
	iConn, ok := conn.(*instrumentedConn)
	if !ok {
		t.Fatal("expected *instrumentedConn")
	}

	// fakeConn does not implement ConnBeginTx — should fall back to Begin.
	tx, err := iConn.BeginTx(context.Background(), driverTxOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx == nil {
		t.Fatal("expected non-nil tx from BeginTx fallback")
	}
}

func TestInstrumentedStmtCloseAndNumInput(t *testing.T) {
	iStmt := &instrumentedStmt{
		client: newTestClient(),
		inner:  &fakeStmt{},
		query:  "SELECT 1",
	}
	if err := iStmt.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if iStmt.NumInput() != -1 {
		t.Fatalf("expected NumInput -1, got %d", iStmt.NumInput())
	}
}

func TestInstrumentedStmtExecContextFallsBackToExecWhenNoContextSupport(t *testing.T) {
	// fakeStmt does not implement StmtExecContext — should fall back to Exec.
	client := newTestClient()
	iStmt := &instrumentedStmt{
		client: client,
		inner:  &fakeStmt{},
		query:  "INSERT INTO t VALUES (?)",
	}

	_, err := iStmt.ExecContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error from ExecContext fallback: %v", err)
	}
}

func TestInstrumentedStmtQueryContextFallsBackToQueryWhenNoContextSupport(t *testing.T) {
	client := newTestClient()
	iStmt := &instrumentedStmt{
		client: client,
		inner:  &fakeStmt{},
		query:  "SELECT * FROM t",
	}

	_, err := iStmt.QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error from QueryContext fallback: %v", err)
	}
}

func TestInstrumentedConnQueryContextSkipsWhenNoQueryerContext(t *testing.T) {
	// fakeConn does not implement QueryerContext — should return driver.ErrSkip.
	client := newTestClient()
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(client, inner)

	conn, _ := wrapped.Connect(context.Background())
	iConn, ok := conn.(*instrumentedConn)
	if !ok {
		t.Fatal("expected *instrumentedConn")
	}

	_, err := iConn.QueryContext(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected driver.ErrSkip when inner conn doesn't implement QueryerContext")
	}
}

func TestInstrumentedConnExecContextSkipsWhenNoExecerContext(t *testing.T) {
	client := newTestClient()
	inner := &fakeConnector{conn: &fakeConn{}}
	wrapped := WrapConnector(client, inner)

	conn, _ := wrapped.Connect(context.Background())
	iConn, ok := conn.(*instrumentedConn)
	if !ok {
		t.Fatal("expected *instrumentedConn")
	}

	_, err := iConn.ExecContext(context.Background(), "UPDATE t SET x=1", nil)
	if err == nil {
		t.Fatal("expected driver.ErrSkip when inner conn doesn't implement ExecerContext")
	}
}

func TestNamedToValuesConvertsCorrectly(t *testing.T) {
	named := []driverNamedValue{
		{Ordinal: 1, Value: "hello"},
		{Ordinal: 2, Value: int64(42)},
	}
	result := namedToValues(named)
	if len(result) != 2 {
		t.Fatalf("expected 2 values, got %d", len(result))
	}
	if result[0] != "hello" {
		t.Fatalf("expected 'hello', got %v", result[0])
	}
	if result[1] != int64(42) {
		t.Fatalf("expected 42, got %v", result[1])
	}
}

func TestNamedToValuesNilInputReturnsNil(t *testing.T) {
	result := namedToValues(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestTruncateQueryTruncatesLongQuery(t *testing.T) {
	long := make([]byte, 600)
	for i := range long {
		long[i] = 'x'
	}
	result := truncateQuery(string(long))
	if len(result) > dbQueryMaxLen {
		t.Fatalf("expected truncated query <= %d chars, got %d", dbQueryMaxLen, len(result))
	}
}

func TestTruncateQueryPreservesShortQuery(t *testing.T) {
	short := "SELECT * FROM users WHERE id = ?"
	result := truncateQuery(short)
	if result != short {
		t.Fatalf("expected unchanged short query, got %q", result)
	}
}

func TestTruncateQueryHandlesUTF8Boundary(t *testing.T) {
	// Build a string that is exactly at the boundary with multi-byte runes.
	// "€" is 3 bytes (U+20AC). Put it near byte 500 to test alignment.
	prefix := make([]byte, 498)
	for i := range prefix {
		prefix[i] = 'a'
	}
	utf8Query := string(prefix) + "€€€"
	result := truncateQuery(utf8Query)
	// Result must be valid UTF-8 (no partial rune).
	for _, r := range result {
		if r == '\uFFFD' {
			t.Fatal("expected valid UTF-8 in truncated query, got replacement character")
		}
	}
}
