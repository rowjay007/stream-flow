package main

import (
	"flag"
	"fmt"
	"os"

	"streamflow/connectors"
	"streamflow/processor"
	"streamflow/streamql"
)

func main() {
	q := flag.String("query", "", "StreamQL query to run")
	flag.Parse()
	if *q == "" {
		fmt.Println("usage: streamql-runner -query \"SELECT ...\"")
		os.Exit(2)
	}

	parsed, err := streamql.Parse(*q)
	if err != nil {
		fmt.Println("parse error:", err)
		os.Exit(1)
	}
	plan := streamql.Planify(parsed)

	src := &connectors.InMemorySource{Records: []processor.Record{
		{"x": 1, "group": "a"},
		{"x": 2, "group": "a"},
		{"x": 3, "group": "b"},
	}}

	out := streamql.RunPlan(plan, src.Run())
	for r := range out {
		fmt.Println(r)
	}
}
