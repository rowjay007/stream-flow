package connectors

import "testing"

func TestBuiltInConnectorNames(t *testing.T) {
	if (&KafkaBridgeConnector{}).Name() != "kafka-bridge" {
		t.Fatalf("unexpected kafka connector name")
	}
	if (&PostgresCDCConnector{}).Name() != "postgres-cdc" {
		t.Fatalf("unexpected postgres connector name")
	}
	if (&S3Connector{}).Name() != "s3" {
		t.Fatalf("unexpected s3 connector name")
	}
	if (&HTTPConnector{}).Name() != "http" {
		t.Fatalf("unexpected http connector name")
	}
}
