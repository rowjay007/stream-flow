package schema

import "sync"

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
