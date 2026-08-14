package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"streamflow/broker"
)

func main() {
	workDir := filepath.Join(os.TempDir(), "streamflow-broker")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		panic(err)
	}

	br, err := broker.NewBroker(workDir)
	if err != nil {
		panic(err)
	}
	if _, err = br.CreateTopic("orders"); err != nil {
		panic(err)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Topic   string            `json:"topic"`
			Key     string            `json:"key"`
			Value   string            `json:"value"`
			Headers map[string]string `json:"headers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rec, err := br.Produce(req.Topic, []byte(req.Key), []byte(req.Value), req.Headers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rec)
	})

	http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "missing topic", http.StatusBadRequest)
			return
		}
		offset := int64(0)
		max := 100
		if v := r.URL.Query().Get("offset"); v != "" {
			fmt.Sscanf(v, "%d", &offset)
		}
		if v := r.URL.Query().Get("max"); v != "" {
			fmt.Sscanf(v, "%d", &max)
		}
		recs, err := br.Consume(topic, offset, max)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(recs)
	})

	http.HandleFunc("/fetchraw", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "missing topic", http.StatusBadRequest)
			return
		}
		var offset int64
		if v := r.URL.Query().Get("offset"); v != "" {
			fmt.Sscanf(v, "%d", &offset)
		}

		f, pos, length, err := br.FetchRaw(topic, offset)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Try to hijack the connection to get at the underlying socket for
		// zero-copy sendfile. If hijack fails, fallback to copying into
		// the HTTP response normally.
		hj, ok := w.(http.Hijacker)
		if !ok {
			// No hijack support, fallback.
			// Simple read-then-write fallback
			payload := make([]byte, length)
			if _, err := f.ReadAt(payload, pos); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}

		conn, buf, err := hj.Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()
		// First write a minimal HTTP response header
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\r\n")
		_ = buf.Flush()

		// Attempt zero-copy sendfile using the connection's file descriptor.
		// Obtain an *os.File for the connection via Conn's File() when possible.
		var connFile *os.File
		if tcp, ok := conn.(*net.TCPConn); ok {
			if file, ferr := tcp.File(); ferr == nil {
				connFile = file
			}
		}
		if connFile == nil {
			// Fallback: read-then-write
			payload := make([]byte, length)
			if _, err := f.ReadAt(payload, pos); err != nil {
				return
			}
			_, _ = conn.Write(payload)
			return
		}

		// Use broker.SendFile helper which is platform-aware.
		if _, err := broker.SendFile(connFile, f, pos, length); err != nil {
			// On failure, fallback to copy
			payload := make([]byte, length)
			if _, err := f.ReadAt(payload, pos); err != nil {
				return
			}
			_, _ = conn.Write(payload)
			return
		}
	})

	addr := ":9092"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	log.Printf("broker listening on %s (data dir=%s)", addr, workDir)
	log.Fatal(http.ListenAndServe(addr, nil))
}
