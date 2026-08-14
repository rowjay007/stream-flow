package schema

import "testing"

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	r.Register("users", "{id:int,name:string}")
	s, ok := r.Get("users")
	if !ok {
		t.Fatal("expected schema present")
	}
	if s != "{id:int,name:string}" {
		t.Fatalf("unexpected schema: %s", s)
	}
}
