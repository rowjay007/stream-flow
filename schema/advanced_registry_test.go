package schema

import "testing"

func TestAdvancedRegistryFormatsAndCompatibility(t *testing.T) {
	r := NewAdvancedRegistry()
	if _, err := r.Register("orders", FormatJSONSchema, `{"type":"object"}`); err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	if _, err := r.Register("orders", FormatJSONSchema, `{"type":"object","properties":{"id":{"type":"string"}}}`); err != nil {
		t.Fatalf("backward-compatible update failed: %v", err)
	}
	if _, err := r.Register("orders", FormatJSONSchema, `{}`); err == nil {
		t.Fatalf("expected incompatible schema rejection")
	}
	if latest, ok := r.Latest("orders"); !ok || latest.ID == 0 {
		t.Fatalf("expected latest schema")
	}
}
