package incidentary

import "database/sql/driver"

// DBIntegration provides database instrumentation helpers through the standard
// Integration interface.
//
// In Go there is no runtime monkey-patching, so DBIntegration does not
// automatically intercept database calls. Instead, after registering it with a
// registry, users call WrapConnector to obtain an instrumented connector and
// pass it to sql.OpenDB.
//
//	reg := incidentary.NewIntegrationRegistry(client)
//	dbInt := &incidentary.DBIntegration{}
//	reg.Register(dbInt)
//	reg.SetupAll()
//
//	// Wire the connector:
//	db := sql.OpenDB(dbInt.WrapConnector(myConnector))
type DBIntegration struct {
	client *Client
}

// Name returns the integration identifier.
func (d *DBIntegration) Name() string {
	return "db"
}

// Setup stores the client reference so WrapConnector can use it. It performs
// no global side effects and returns a no-op cleanup.
func (d *DBIntegration) Setup(client *Client) (func(), error) {
	d.client = client
	return nil, nil
}

// WrapConnector wraps a driver.Connector with Incidentary database query
// instrumentation. The client stored during Setup is used; if Setup has not
// been called, a nil client is passed to WrapConnector (which handles nil
// gracefully by recording no events).
func (d *DBIntegration) WrapConnector(connector driver.Connector) driver.Connector {
	return WrapConnector(d.client, connector)
}
