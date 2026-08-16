package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"streamflow/broker"
	"streamflow/management/api"
)

func main() {
	addr := ":8094"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

	workDir := filepath.Join(os.TempDir(), "streamflow-management")
	b, err := broker.NewBroker(workDir)
	if err != nil {
		log.Fatalf("new broker: %v", err)
	}

	apiKey := os.Getenv("STREAMFLOW_MANAGEMENT_API_KEY")
	srv := api.NewServerWithAPIKey(b, apiKey)

	log.Printf("management-api listening on %s", addr)
	log.Fatal((&http.Server{Addr: addr, Handler: srv.Handler()}).ListenAndServe())
}
