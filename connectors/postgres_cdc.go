package connectors

type PostgresCDCConnector struct {
	DSN string
}

func (c *PostgresCDCConnector) Name() string { return "postgres-cdc" }
