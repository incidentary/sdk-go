package incidentary

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- test helpers ---

// fakeIntegration is a controllable Integration for use in tests.
type fakeIntegration struct {
	name        string
	setupErr    error
	setupCalled bool
	cleanup     func()
}

func (f *fakeIntegration) Name() string { return f.name }

func (f *fakeIntegration) Setup(client *Client) (func(), error) {
	f.setupCalled = true
	if f.setupErr != nil {
		return nil, f.setupErr
	}
	return f.cleanup, nil
}

// --- IntegrationRegistry ---

func TestRegistryRegisterAddsIntegrations(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	a := &fakeIntegration{name: "alpha"}
	b := &fakeIntegration{name: "beta"}
	registry.Register(a, b)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error from SetupAll: %v", err)
	}

	active := registry.Active()
	if len(active) != 2 {
		t.Fatalf("expected 2 active integrations, got %d", len(active))
	}
}

func TestRegistrySetupAllCallsSetupOnEachIntegration(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	a := &fakeIntegration{name: "first"}
	b := &fakeIntegration{name: "second"}
	registry.Register(a, b)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !a.setupCalled {
		t.Error("expected Setup to be called on first integration")
	}
	if !b.setupCalled {
		t.Error("expected Setup to be called on second integration")
	}
}

func TestRegistrySetupAllContinuesOnPartialFailure(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	failing := &fakeIntegration{name: "failing", setupErr: errors.New("setup failed")}
	succeeding := &fakeIntegration{name: "succeeding"}
	registry.Register(failing, succeeding)

	// SetupAll should not return an error when one integration fails.
	if err := registry.SetupAll(); err != nil {
		t.Fatalf("expected no error from SetupAll on partial failure, got: %v", err)
	}

	// Only the succeeding integration should appear in Active.
	active := registry.Active()
	if len(active) != 1 {
		t.Fatalf("expected 1 active integration, got %d: %v", len(active), active)
	}
	if active[0] != "succeeding" {
		t.Fatalf("expected active[0] to be 'succeeding', got %q", active[0])
	}
}

func TestRegistryTeardownAllCallsAllCleanups(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	cleanupACalled := false
	cleanupBCalled := false

	a := &fakeIntegration{name: "a", cleanup: func() { cleanupACalled = true }}
	b := &fakeIntegration{name: "b", cleanup: func() { cleanupBCalled = true }}
	registry.Register(a, b)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	registry.TeardownAll()

	if !cleanupACalled {
		t.Error("expected cleanup for integration 'a' to be called")
	}
	if !cleanupBCalled {
		t.Error("expected cleanup for integration 'b' to be called")
	}
}

func TestRegistryTeardownAllNilCleanupDoesNotPanic(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	// Integration returns nil cleanup.
	a := &fakeIntegration{name: "no-cleanup", cleanup: nil}
	registry.Register(a)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must not panic.
	registry.TeardownAll()
}

func TestRegistryActiveReturnsNamesOfSuccessfullySetupIntegrations(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	ok1 := &fakeIntegration{name: "ok-one"}
	bad := &fakeIntegration{name: "bad", setupErr: errors.New("oops")}
	ok2 := &fakeIntegration{name: "ok-two"}
	registry.Register(ok1, bad, ok2)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	active := registry.Active()
	if len(active) != 2 {
		t.Fatalf("expected 2 active integrations, got %d: %v", len(active), active)
	}

	nameSet := map[string]bool{}
	for _, n := range active {
		nameSet[n] = true
	}
	if !nameSet["ok-one"] {
		t.Error("expected 'ok-one' in active integrations")
	}
	if !nameSet["ok-two"] {
		t.Error("expected 'ok-two' in active integrations")
	}
	if nameSet["bad"] {
		t.Error("expected 'bad' to NOT be in active integrations")
	}
}

func TestRegistryActiveEmptyBeforeSetupAll(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)
	registry.Register(&fakeIntegration{name: "not-yet-setup"})

	active := registry.Active()
	if len(active) != 0 {
		t.Fatalf("expected 0 active before SetupAll, got %d", len(active))
	}
}

func TestRegistrySetupAllWithNoIntegrations(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("expected no error with zero integrations, got: %v", err)
	}

	if len(registry.Active()) != 0 {
		t.Fatal("expected empty active list")
	}
}

func TestRegistryRegisterMultipleCalls(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	registry.Register(&fakeIntegration{name: "x"})
	registry.Register(&fakeIntegration{name: "y"})

	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(registry.Active()) != 2 {
		t.Fatalf("expected 2 active, got %d", len(registry.Active()))
	}
}

// --- HTTPIntegration ---

func TestHTTPIntegrationImplementsInterface(t *testing.T) {
	// Compile-time check: HTTPIntegration must satisfy Integration.
	var _ Integration = (*HTTPIntegration)(nil)
}

func TestHTTPIntegrationName(t *testing.T) {
	h := &HTTPIntegration{}
	if h.Name() != "http" {
		t.Fatalf("expected name 'http', got %q", h.Name())
	}
}

func TestHTTPIntegrationSetupStoresClient(t *testing.T) {
	client := newTestClient()
	h := &HTTPIntegration{}

	cleanup, err := h.Setup(client)
	if err != nil {
		t.Fatalf("unexpected error from Setup: %v", err)
	}
	// cleanup may be nil or a no-op — both are valid.
	if cleanup != nil {
		cleanup() // must not panic
	}

	if h.client != client {
		t.Fatal("expected HTTPIntegration to store the client reference")
	}
}

func TestHTTPIntegrationSetupWithNilClientDoesNotError(t *testing.T) {
	h := &HTTPIntegration{}
	cleanup, err := h.Setup(nil)
	if err != nil {
		t.Fatalf("expected no error with nil client, got: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
}

func TestHTTPIntegrationMiddlewareInstrumentsRequests(t *testing.T) {
	client := newTestClient()
	h := &HTTPIntegration{}
	if _, err := h.Setup(client); err != nil {
		t.Fatalf("Setup error: %v", err)
	}

	var handlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	mw := h.Middleware()
	if mw == nil {
		t.Fatal("expected non-nil middleware from HTTPIntegration.Middleware()")
	}

	wrapped := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !handlerCalled {
		t.Error("expected inner handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestHTTPIntegrationMiddlewareNilClientDoesNotPanic(t *testing.T) {
	h := &HTTPIntegration{}
	// Do not call Setup — client remains nil.
	mw := h.Middleware()
	if mw == nil {
		t.Fatal("expected non-nil middleware even with nil client")
	}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	wrapped := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	// Must not panic.
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}

func TestHTTPIntegrationTransportReturnsWrapped(t *testing.T) {
	client := newTestClient()
	h := &HTTPIntegration{}
	if _, err := h.Setup(client); err != nil {
		t.Fatalf("Setup error: %v", err)
	}

	base := http.DefaultTransport
	wrapped := h.Transport(base)
	if wrapped == nil {
		t.Fatal("expected non-nil transport")
	}
	// With a client set, the wrapped transport should not be the same as base.
	if wrapped == base {
		t.Fatal("expected Transport() to wrap the base transport, not return it unchanged")
	}
}

func TestHTTPIntegrationTransportNilClientReturnsBase(t *testing.T) {
	h := &HTTPIntegration{}
	// No Setup call — client is nil.
	base := http.DefaultTransport
	wrapped := h.Transport(base)

	// WrapTransport returns base when client is nil.
	if wrapped != base {
		t.Fatal("expected Transport() to return base unchanged when client is nil")
	}
}

// --- DefaultIntegrations ---

func TestDefaultIntegrationsReturnsHTTPIntegration(t *testing.T) {
	integrations := DefaultIntegrations()
	if len(integrations) == 0 {
		t.Fatal("expected at least one default integration")
	}

	hasHTTP := false
	for _, integration := range integrations {
		if integration.Name() == "http" {
			hasHTTP = true
		}
	}
	if !hasHTTP {
		t.Fatal("expected DefaultIntegrations to contain the 'http' integration")
	}
}

func TestDefaultIntegrationsReturnsNewSliceEachCall(t *testing.T) {
	a := DefaultIntegrations()
	b := DefaultIntegrations()

	// Modifying one slice must not affect the other.
	if len(a) > 0 {
		a[0] = &fakeIntegration{name: "mutated"}
	}
	if len(b) > 0 && b[0].Name() == "mutated" {
		t.Fatal("expected DefaultIntegrations to return independent slices")
	}
}

// --- Client integration ---

func TestClientNewCreatesRegistryWithDefaultIntegrations(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	client := New(cfg)

	if client.Registry == nil {
		t.Fatal("expected client.Registry to be non-nil after New()")
	}

	active := client.Registry.Active()
	if len(active) == 0 {
		t.Fatal("expected at least one active integration in default client")
	}
}

func TestClientNewWithCustomIntegrations(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	cfg.Integrations = []Integration{
		&fakeIntegration{name: "custom-a"},
		&fakeIntegration{name: "custom-b"},
	}
	client := New(cfg)

	active := client.Registry.Active()
	if len(active) != 2 {
		t.Fatalf("expected 2 active integrations, got %d: %v", len(active), active)
	}

	nameSet := map[string]bool{}
	for _, n := range active {
		nameSet[n] = true
	}
	if !nameSet["custom-a"] || !nameSet["custom-b"] {
		t.Fatalf("expected custom integrations to be active, got: %v", active)
	}
}

func TestClientTeardownCallsCleanups(t *testing.T) {
	cleanedUp := false

	cfg := DefaultConfig("test-key", "svc")
	cfg.Integrations = []Integration{
		&fakeIntegration{name: "tracked", cleanup: func() { cleanedUp = true }},
	}
	client := New(cfg)

	client.Teardown()

	if !cleanedUp {
		t.Fatal("expected Teardown() to call integration cleanup functions")
	}
}

func TestClientTeardownSafeToCallMultipleTimes(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	client := New(cfg)

	// Must not panic on repeated calls.
	client.Teardown()
	client.Teardown()
}

func TestClientRegistryActiveNotNilWithEmptyIntegrations(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	cfg.Integrations = []Integration{} // explicitly empty
	client := New(cfg)

	if client.Registry == nil {
		t.Fatal("expected client.Registry to be non-nil even with empty integrations")
	}
	// No integrations to set up, so active should be empty.
	active := client.Registry.Active()
	if active == nil {
		t.Fatal("expected Active() to return non-nil empty slice")
	}
}

// --- No-panic guarantees ---

func TestRegistryDoesNotPanicOnSetupAllWithPanickingIntegration(t *testing.T) {
	client := newTestClient()
	registry := NewIntegrationRegistry(client)

	panicking := &panicIntegration{name: "panicker"}
	normal := &fakeIntegration{name: "normal"}
	registry.Register(panicking, normal)

	// Should not panic.
	if err := registry.SetupAll(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The normal one should still succeed.
	active := registry.Active()
	hasNormal := false
	for _, n := range active {
		if n == "normal" {
			hasNormal = true
		}
	}
	if !hasNormal {
		t.Fatalf("expected 'normal' integration to be active after panicking integration, got: %v", active)
	}
}

// panicIntegration panics in Setup to test registry resilience.
type panicIntegration struct{ name string }

func (p *panicIntegration) Name() string { return p.name }
func (p *panicIntegration) Setup(_ *Client) (func(), error) {
	panic(fmt.Sprintf("integration %q panicked", p.name))
}
