package connectors

// PostgresCDCConnector is a built-in connector surface for PostgreSQL CDC ingestion.
type PostgresCDCConnector struct {
	DSN string
}

func (c *PostgresCDCConnector) Name() string { return "postgres-cdc" }
