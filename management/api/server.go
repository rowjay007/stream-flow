package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"streamflow/broker"
)

// Server exposes management endpoints for topic lifecycle and data-plane checks.
type Server struct {
	broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{broker: b}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/topics", s.handleTopics)
	mux.HandleFunc("/produce", s.handleProduce)
	mux.HandleFunc("/consume", s.handleConsume)
	mux.HandleFunc("/offset/commit", s.handleCommitOffset)
	mux.HandleFunc("/offset", s.handleOffset)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]interface{}{"topics": s.broker.ListTopics()})
	case http.MethodPost:
		defer r.Body.Close()
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			http.Error(w, "invalid topic payload", http.StatusBadRequest)
			return
		}
		if _, err := s.broker.CreateTopic(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"topic": req.Name})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		Topic   string            `json:"topic"`
		Key     string            `json:"key"`
		Value   string            `json:"value"`
		Headers map[string]string `json:"headers,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" {
		http.Error(w, "invalid produce payload", http.StatusBadRequest)
		return
	}
	if _, err := s.broker.CreateTopic(req.Topic); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rec, err := s.broker.Produce(req.Topic, []byte(req.Key), []byte(req.Value), req.Headers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
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
	recs, err := s.broker.Consume(topic, offset, max)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, recs)
}

func (s *Server) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		Topic  string `json:"topic"`
		Group  string `json:"group"`
		Offset int64  `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" || req.Group == "" {
		http.Error(w, "invalid offset commit payload", http.StatusBadRequest)
		return
	}
	if err := s.broker.CommitOffset(req.Topic, req.Group, req.Offset); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (s *Server) handleOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	if topic == "" || group == "" {
		http.Error(w, "topic and group are required", http.StatusBadRequest)
		return
	}
	off, err := s.broker.LoadOffset(topic, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"topic": topic, "group": group, "offset": off})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
