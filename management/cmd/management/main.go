package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	addr := ":8094"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("management-api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
