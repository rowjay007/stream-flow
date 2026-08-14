package schema

import (
	"strings"
	"sync"
)

// Registry provides a simple in-memory schema registry.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string]string
}

func NewRegistry() *Registry {
	return &Registry{schemas: make(map[string]string)}
}

// Register stores a schema string under the given name.
func (r *Registry) Register(name, schema string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[name] = schema
}

// Get retrieves a schema by name. Second return value indicates presence.
func (r *Registry) Get(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[name]
	return s, ok
}

// Validate checks a record against a simple schema stored under name.
// Schema format: "field:type,field:type" where type is "int" or "string".
func (r *Registry) Validate(name string, rec map[string]interface{}) bool {
	s, ok := r.Get(name)
	if !ok {
		return false
	}
	if s == "" {
		return true
	}
	parts := splitSchema(s)
	for k, typ := range parts {
		v, ok := rec[k]
		if !ok {
			return false
		}
		switch typ {
		case "int":
			switch v.(type) {
			case int:
			default:
				return false
			}
		case "string":
			switch v.(type) {
			case string:
			default:
				return false
			}
		}
	}
	return true
}

func splitSchema(s string) map[string]string {
	out := make(map[string]string)
	// accept either {a:int,b:string} or a:int,b:string
	s = trimBraces(s)
	if s == "" {
		return out
	}
	parts := splitAndTrim(s, ",")
	for _, p := range parts {
		kv := splitAndTrim(p, ":")
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func trimBraces(s string) string {
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		return s[1 : len(s)-1]
	}
	return s
}

func splitAndTrim(s, sep string) []string {
	var res []string
	for _, p := range strings.Split(s, sep) {
		t := strings.TrimSpace(p)
		if t != "" {
			res = append(res, t)
		}
	}
	return res
}
