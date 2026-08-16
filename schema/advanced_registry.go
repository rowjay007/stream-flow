package schema

import (
	"fmt"
	"sync"
)

type SchemaFormat string

const (
	FormatJSONSchema SchemaFormat = "jsonschema"
	FormatAvro       SchemaFormat = "avro"
	FormatProtobuf   SchemaFormat = "protobuf"
)

type SchemaVersion struct {
	ID      int
	Name    string
	Format  SchemaFormat
	Content string
}

type AdvancedRegistry struct {
	mu       sync.RWMutex
	nextID   int
	versions map[string][]SchemaVersion
}

func NewAdvancedRegistry() *AdvancedRegistry {
	return &AdvancedRegistry{nextID: 1, versions: make(map[string][]SchemaVersion)}
}

func (r *AdvancedRegistry) Register(name string, format SchemaFormat, content string) (SchemaVersion, error) {
	if name == "" || content == "" {
		return SchemaVersion{}, fmt.Errorf("name and content are required")
	}
	if format != FormatJSONSchema && format != FormatAvro && format != FormatProtobuf {
		return SchemaVersion{}, fmt.Errorf("unsupported schema format")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	versions := r.versions[name]
	if len(versions) > 0 {
		last := versions[len(versions)-1]
		if !isBackwardCompatible(last.Content, content) {
			return SchemaVersion{}, fmt.Errorf("schema is not backward compatible")
		}
	}
	sv := SchemaVersion{ID: r.nextID, Name: name, Format: format, Content: content}
	r.nextID++
	r.versions[name] = append(r.versions[name], sv)
	return sv, nil
}

func (r *AdvancedRegistry) Latest(name string) (SchemaVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.versions[name]
	if len(versions) == 0 {
		return SchemaVersion{}, false
	}
	return versions[len(versions)-1], true
}

func isBackwardCompatible(oldSchema, newSchema string) bool {

	if oldSchema == "" {
		return true
	}
	return len(newSchema) >= len(oldSchema)
}
