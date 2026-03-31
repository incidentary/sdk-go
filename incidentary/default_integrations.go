package incidentary

// DefaultIntegrations returns the default set of integrations used when no
// explicit Integrations are configured in Config. Each call returns a fresh
// slice with new instances.
func DefaultIntegrations() []Integration {
	return []Integration{
		&HTTPIntegration{},
		&QueueIntegration{},
		&DBIntegration{},
		&GRPCIntegration{},
	}
}
