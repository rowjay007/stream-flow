package connectors

import (
	"testing"

	"streamflow/processor"
	"streamflow/schema"
)

func TestInMemorySourceSchemaValidation(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("users", "{id:int,name:string}")
	src := &InMemorySource{Records: []processor.Record{
		processor.Record{"id": 1, "name": "Alice"},
		processor.Record{"id": "bad", "name": "Bob"},
	}, Registry: reg, SchemaName: "users"}

	ch := src.Run()
	var got int
	for r := range ch {
		got++
		if r["name"] == nil {
			t.Fatalf("received invalid record: %#v", r)
		}
	}
	if got != 1 {
		t.Fatalf("expected 1 valid record, got %d", got)
	}
}
