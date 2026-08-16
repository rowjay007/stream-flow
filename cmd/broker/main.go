package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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

	type produceReq struct {
		Topic   string            `json:"topic"`
		Key     string            `json:"key"`
		Value   string            `json:"value"`
		Headers map[string]string `json:"headers,omitempty"`
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		var req produceReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.Topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}

		if _, err := br.CreateTopic(req.Topic); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}

		offset := int64(0)
		if v := r.URL.Query().Get("offset"); v != "" {
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				http.Error(w, "invalid offset", http.StatusBadRequest)
				return
			}
			offset = parsed
		}

		max := 100
		if v := r.URL.Query().Get("max"); v != "" {
			parsed, err := strconv.Atoi(v)
			if err != nil || parsed <= 0 {
				http.Error(w, "invalid max", http.StatusBadRequest)
				return
			}
			max = parsed
		}

		recs, err := br.Consume(topic, offset, max)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(recs)
	})

	http.HandleFunc("/fetchraw", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}
		offsetStr := r.URL.Query().Get("offset")
		if offsetStr == "" {
			http.Error(w, "offset is required", http.StatusBadRequest)
			return
		}
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}

		f, pos, length, err := br.FetchRaw(topic, offset)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		if _, err := broker.SendFile(w, f, pos, length); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	addr := ":9092"
	log.Printf("broker listening on %s (dir=%s)", addr, workDir)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}
