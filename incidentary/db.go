package incidentary

import (
	"context"
	"database/sql/driver"
	"time"
	"unicode/utf8"
)

const dbQueryMaxLen = 500

// WrapConnector wraps a driver.Connector to automatically record db_query
// events for all queries executed through connections it creates. If client
// is nil, the returned connector behaves identically to inner but records
// no events.
func WrapConnector(client *Client, connector driver.Connector) driver.Connector {
	return &instrumentedConnector{client: client, inner: connector}
}

// instrumentedConnector wraps a driver.Connector and injects instrumentation
// into every connection it produces.
type instrumentedConnector struct {
	client *Client
	inner  driver.Connector
}

func (c *instrumentedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &instrumentedConn{client: c.client, inner: conn}, nil
}

func (c *instrumentedConnector) Driver() driver.Driver {
	return c.inner.Driver()
}

// instrumentedConn wraps a driver.Conn and adds event recording to query
// operations. It implements driver.Conn plus optional context-aware interfaces
// (driver.QueryerContext, driver.ExecerContext, driver.ConnBeginTx) when the
// underlying connection supports them.
type instrumentedConn struct {
	client *Client
	inner  driver.Conn
}

// Prepare returns an instrumentedStmt that records an event on each execution.
func (c *instrumentedConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.inner.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &instrumentedStmt{
		client: c.client,
		inner:  stmt,
		query:  truncateQuery(query),
	}, nil
}

func (c *instrumentedConn) Close() error {
	return c.inner.Close()
}

func (c *instrumentedConn) Begin() (driver.Tx, error) {
	return c.inner.Begin() //nolint:staticcheck // Begin is the only interface method available here
}

// QueryContext is implemented when the inner connection supports
// driver.QueryerContext. The database/sql package detects this via a type
// assertion and bypasses Prepare+Execute for direct queries.
func (c *instrumentedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	rows, err := qc.QueryContext(ctx, query, args)
	recordDBQuery(c.client, ctx, "query", query, start, err)
	return rows, err
}

// ExecContext is implemented when the inner connection supports
// driver.ExecerContext. The database/sql package detects this via a type
// assertion and bypasses Prepare+Execute for direct exec calls.
func (c *instrumentedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.inner.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	result, err := ec.ExecContext(ctx, query, args)
	recordDBQuery(c.client, ctx, "exec", query, start, err)
	return result, err
}

// BeginTx is implemented when the inner connection supports driver.ConnBeginTx.
func (c *instrumentedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	cbt, ok := c.inner.(driver.ConnBeginTx)
	if !ok {
		return c.inner.Begin() //nolint:staticcheck
	}
	return cbt.BeginTx(ctx, opts)
}

// instrumentedStmt wraps a driver.Stmt and records a db_query event for each
// Exec or Query call. It also implements driver.StmtExecContext and
// driver.StmtQueryContext when the underlying statement supports them.
type instrumentedStmt struct {
	client *Client
	inner  driver.Stmt
	query  string
}

func (s *instrumentedStmt) Close() error    { return s.inner.Close() }
func (s *instrumentedStmt) NumInput() int   { return s.inner.NumInput() }

func (s *instrumentedStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	result, err := s.inner.Exec(args)
	recordDBQuery(s.client, context.Background(), "exec", s.query, start, err)
	return result, err
}

func (s *instrumentedStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	rows, err := s.inner.Query(args)
	recordDBQuery(s.client, context.Background(), "query", s.query, start, err)
	return rows, err
}

// ExecContext is implemented when the inner statement supports
// driver.StmtExecContext.
func (s *instrumentedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sec, ok := s.inner.(driver.StmtExecContext)
	if !ok {
		// Fall back to Exec by converting named values.
		values := namedToValues(args)
		return s.Exec(values)
	}
	start := time.Now()
	result, err := sec.ExecContext(ctx, args)
	recordDBQuery(s.client, ctx, "exec", s.query, start, err)
	return result, err
}

// QueryContext is implemented when the inner statement supports
// driver.StmtQueryContext.
func (s *instrumentedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	sqc, ok := s.inner.(driver.StmtQueryContext)
	if !ok {
		// Fall back to Query by converting named values.
		values := namedToValues(args)
		return s.Query(values)
	}
	start := time.Now()
	rows, err := sqc.QueryContext(ctx, args)
	recordDBQuery(s.client, ctx, "query", s.query, start, err)
	return rows, err
}

// recordDBQuery writes a db_query SkeletonCe to the client's ring buffer.
// If client is nil this is a no-op. Never panics.
func recordDBQuery(client *Client, ctx context.Context, operation string, query string, startTime time.Time, err error) {
	if client == nil {
		return
	}

	duration := time.Since(startTime)
	if duration < 0 {
		duration = 0
	}

	traceID, parentCe := ContextTrace(ctx)
	if traceID == "" {
		traceID = randomUUID()
	}

	status := 0
	if err != nil {
		status = 500
	}

	ce := &SkeletonCe{
		CeID:       randomUUID(),
		TraceID:    traceID,
		ParentCeID: parentCe,
		ServiceID:  client.ServiceName,
		WallTsNs:   startTime.UnixNano(),
		Kind:       KindDBQuery,
		EventType:  string(EventDBQuery),
		StatusCode: status,
		DurationNs: duration.Nanoseconds(),
	}

	if operation != "" || query != "" {
		q := truncateQuery(query)
		ce.Attributes = map[string]interface{}{
			"operation": operation,
			"query":     q,
		}
	}

	client.WriteEvent(ce)
}

// truncateQuery returns the query truncated to dbQueryMaxLen bytes, ensuring
// the result does not end with a partial UTF-8 rune.
func truncateQuery(query string) string {
	if len(query) <= dbQueryMaxLen {
		return query
	}
	// Find the last valid rune boundary at or before the limit.
	i := dbQueryMaxLen
	for i > 0 && !utf8.RuneStart(query[i]) {
		i--
	}
	return query[:i]
}

// namedToValues converts a slice of driver.NamedValue to driver.Value for
// use with non-context-aware Exec/Query methods.
func namedToValues(named []driver.NamedValue) []driver.Value {
	if named == nil {
		return nil
	}
	values := make([]driver.Value, len(named))
	for i, nv := range named {
		values[i] = nv.Value
	}
	return values
}
