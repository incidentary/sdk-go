package incidentary

import "fmt"

// Integration defines the contract for auto-instrumentation modules.
// Each integration is responsible for instrumenting one concern (e.g., HTTP).
// Implementations must be safe to call from multiple goroutines once Setup
// has returned.
type Integration interface {
	// Name returns a human-readable identifier for this integration.
	Name() string
	// Setup registers this integration with the given client.
	// It returns a cleanup function (or nil if none is needed) and any error.
	// Returning an error causes the registry to skip this integration and
	// continue with the remaining ones; it does NOT abort the whole setup.
	Setup(client *Client) (cleanup func(), err error)
}

// IntegrationRegistry manages the lifecycle of a set of integrations for a
// single Client. It is intentionally not goroutine-safe for Register/SetupAll
// calls — wire integrations once at startup before serving traffic.
type IntegrationRegistry struct {
	client       *Client
	integrations []Integration
	cleanups     []func()
	active       []string
}

// NewIntegrationRegistry creates a registry bound to the given client.
// Pass the client returned by New() so integrations can reference it.
func NewIntegrationRegistry(client *Client) *IntegrationRegistry {
	return &IntegrationRegistry{
		client:       client,
		integrations: []Integration{},
		cleanups:     []func(){},
		active:       []string{},
	}
}

// Register appends one or more integrations to the registry.
// Integrations are not set up until SetupAll is called.
func (r *IntegrationRegistry) Register(integrations ...Integration) {
	r.integrations = append(r.integrations, integrations...)
}

// SetupAll calls Setup on every registered integration.
// If an integration's Setup returns an error or panics, that integration is
// skipped and a log-like notification is sent via the client's OnError hook
// (if configured), but the remaining integrations continue to be set up.
// SetupAll always returns nil — errors are handled per-integration.
func (r *IntegrationRegistry) SetupAll() error {
	for _, integration := range r.integrations {
		r.setupOne(integration)
	}
	return nil
}

func (r *IntegrationRegistry) setupOne(integration Integration) {
	defer func() {
		if rec := recover(); rec != nil {
			r.reportError(integration.Name(), fmt.Errorf("integration %q panicked: %v", integration.Name(), rec))
		}
	}()

	cleanup, err := integration.Setup(r.client)
	if err != nil {
		r.reportError(integration.Name(), err)
		return
	}

	r.active = append(r.active, integration.Name())
	if cleanup != nil {
		r.cleanups = append(r.cleanups, cleanup)
	}
}

func (r *IntegrationRegistry) reportError(name string, err error) {
	if r.client != nil && r.client.config.OnError != nil {
		r.client.config.OnError(fmt.Errorf("incidentary: integration %q setup error: %w", name, err))
	}
}

// TeardownAll calls every cleanup function collected during SetupAll.
// It is safe to call multiple times.
func (r *IntegrationRegistry) TeardownAll() {
	for _, cleanup := range r.cleanups {
		safeCallCleanup(cleanup)
	}
	r.cleanups = []func(){}
}

func safeCallCleanup(fn func()) {
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn()
}

// Active returns the names of integrations that were successfully set up by
// the most recent call to SetupAll.
func (r *IntegrationRegistry) Active() []string {
	out := make([]string, len(r.active))
	copy(out, r.active)
	return out
}
