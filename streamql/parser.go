package streamql

import (
	"errors"
	"regexp"
	"strings"
)

var selectRe = regexp.MustCompile(`(?i)^\s*select\s+(.+)\s+from\s+([a-zA-Z0-9_\.]+)(?:\s+where\s+(.+?))?(?:\s+group\s+by\s+(.+))?\s*$`)

// Parse parses a minimal StreamQL SELECT statement into a Query AST.
// Supported form: SELECT a, b FROM stream WHERE expr
func Parse(input string) (*Query, error) {
	m := selectRe.FindStringSubmatch(strings.TrimSpace(input))
	if m == nil {
		return nil, errors.New("invalid query")
	}
	fields := strings.Split(m[1], ",")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	q := &Query{Select: fields, From: m[2]}
	if len(m) >= 4 && strings.TrimSpace(m[3]) != "" {
		q.Where = strings.TrimSpace(m[3])
	}
	if len(m) >= 5 && strings.TrimSpace(m[4]) != "" {
		g := strings.Split(m[4], ",")
		for i := range g {
			g[i] = strings.TrimSpace(g[i])
		}
		q.GroupBy = g
	}
	return q, nil
}
