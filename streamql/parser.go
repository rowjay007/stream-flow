package streamql

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var selectRe = regexp.MustCompile(`(?i)^\s*select\s+(.+)\s+from\s+([a-zA-Z0-9_\.]+)(?:\s+where\s+(.+?))?(?:\s+group\s+by\s+(.+?))?(?:\s+window\s+([0-9]+)(ms|s)?)?\s*$`)

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
	// optional window (milliseconds or seconds)
	if len(m) >= 6 && strings.TrimSpace(m[5]) != "" {
		// m[5] is numeric value, m[6] optional unit
		n := strings.TrimSpace(m[5])
		unit := ""
		if len(m) >= 7 {
			unit = strings.TrimSpace(m[6])
		}
		// parse number
		if v, err := strconv.Atoi(n); err == nil {
			if unit == "s" {
				q.WindowMs = v * 1000
			} else {
				q.WindowMs = v
			}
		}
	}
	return q, nil
}
