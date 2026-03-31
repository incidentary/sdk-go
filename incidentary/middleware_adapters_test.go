package incidentary

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- ChiMiddleware ---

func TestChiMiddlewareReturnsWorkingMiddleware(t *testing.T) {
	client := newTestClient()

	var innerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	mw := ChiMiddleware(client)
	if mw == nil {
		t.Fatal("expected non-nil middleware from ChiMiddleware")
	}

	wrapped := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/chi-test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !innerCalled {
		t.Error("expected inner handler to be called by ChiMiddleware")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestChiMiddlewarePropagatesTraceContext(t *testing.T) {
	client := newTestClient()

	var capturedTraceID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID, _ = ContextTrace(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mw := ChiMiddleware(client)
	wrapped := mw(inner)

	req := httptest.NewRequest(http.MethodGet, "/trace-chi", nil)
	req.Header.Set(TraceIDHeader, "chi-trace-id")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if capturedTraceID != "chi-trace-id" {
		t.Fatalf("expected traceID 'chi-trace-id', got %q", capturedTraceID)
	}
}

func TestChiMiddlewareNilClientDoesNotPanic(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mw := ChiMiddleware(nil)
	wrapped := mw(inner)

	req := httptest.NewRequest(http.MethodGet, "/nil-chi", nil)
	rec := httptest.NewRecorder()

	// Must not panic.
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}

// --- GinMiddleware ---

func TestGinMiddlewareReturnsWorkingHandler(t *testing.T) {
	client := newTestClient()

	var innerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
		w.WriteHeader(http.StatusAccepted)
	})

	handler := GinMiddleware(client, inner)
	if handler == nil {
		t.Fatal("expected non-nil handler from GinMiddleware")
	}

	req := httptest.NewRequest(http.MethodPost, "/gin-test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !innerCalled {
		t.Error("expected inner handler to be called by GinMiddleware")
	}
	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", rec.Code)
	}
}

func TestGinMiddlewarePropagatesTraceContext(t *testing.T) {
	client := newTestClient()

	var capturedTraceID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID, _ = ContextTrace(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := GinMiddleware(client, inner)

	req := httptest.NewRequest(http.MethodGet, "/trace-gin", nil)
	req.Header.Set(TraceIDHeader, "gin-trace-id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedTraceID != "gin-trace-id" {
		t.Fatalf("expected traceID 'gin-trace-id', got %q", capturedTraceID)
	}
}

func TestGinMiddlewareNilClientDoesNotPanic(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	handler := GinMiddleware(nil, inner)

	req := httptest.NewRequest(http.MethodGet, "/nil-gin", nil)
	rec := httptest.NewRecorder()

	// Must not panic.
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Errorf("expected 418, got %d", rec.Code)
	}
}

// --- EchoMiddlewareFunc ---

func TestEchoMiddlewareFuncReturnsWorkingMiddlewareFunc(t *testing.T) {
	client := newTestClient()

	var innerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
		w.WriteHeader(http.StatusCreated)
	})

	mwFunc := EchoMiddlewareFunc(client)
	if mwFunc == nil {
		t.Fatal("expected non-nil middleware func from EchoMiddlewareFunc")
	}

	wrapped := mwFunc(inner)
	req := httptest.NewRequest(http.MethodGet, "/echo-test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !innerCalled {
		t.Error("expected inner handler to be called by EchoMiddlewareFunc")
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", rec.Code)
	}
}

func TestEchoMiddlewareFuncPropagatesTraceContext(t *testing.T) {
	client := newTestClient()

	var capturedTraceID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID, _ = ContextTrace(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mwFunc := EchoMiddlewareFunc(client)
	wrapped := mwFunc(inner)

	req := httptest.NewRequest(http.MethodGet, "/trace-echo", nil)
	req.Header.Set(TraceIDHeader, "echo-trace-id")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if capturedTraceID != "echo-trace-id" {
		t.Fatalf("expected traceID 'echo-trace-id', got %q", capturedTraceID)
	}
}

func TestEchoMiddlewareFuncNilClientDoesNotPanic(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mwFunc := EchoMiddlewareFunc(nil)
	wrapped := mwFunc(inner)

	req := httptest.NewRequest(http.MethodGet, "/nil-echo", nil)
	rec := httptest.NewRecorder()

	// Must not panic.
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// --- All adapters assign a new ceID to the context ---

func TestAllAdaptersGenerateCeIDForNewRequests(t *testing.T) {
	client := newTestClient()

	adapters := []struct {
		name    string
		getCtx  func() context.Context
	}{
		{
			name: "chi",
			getCtx: func() context.Context {
				var ctx context.Context
				inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx = r.Context()
					w.WriteHeader(http.StatusOK)
				})
				mw := ChiMiddleware(client)
				wrapped := mw(inner)
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				wrapped.ServeHTTP(httptest.NewRecorder(), req)
				return ctx
			},
		},
		{
			name: "gin",
			getCtx: func() context.Context {
				var ctx context.Context
				inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx = r.Context()
					w.WriteHeader(http.StatusOK)
				})
				handler := GinMiddleware(client, inner)
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				handler.ServeHTTP(httptest.NewRecorder(), req)
				return ctx
			},
		},
		{
			name: "echo",
			getCtx: func() context.Context {
				var ctx context.Context
				inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx = r.Context()
					w.WriteHeader(http.StatusOK)
				})
				mwFunc := EchoMiddlewareFunc(client)
				wrapped := mwFunc(inner)
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				wrapped.ServeHTTP(httptest.NewRecorder(), req)
				return ctx
			},
		},
	}

	for _, adapter := range adapters {
		t.Run(adapter.name, func(t *testing.T) {
			ctx := adapter.getCtx()
			if ctx == nil {
				t.Fatal("expected non-nil context")
			}
			gotTrace, gotCe := ContextTrace(ctx)
			if gotTrace == "" {
				t.Errorf("%s: expected non-empty traceID in context", adapter.name)
			}
			if gotCe == "" {
				t.Errorf("%s: expected non-empty ceID in context", adapter.name)
			}
		})
	}
}
