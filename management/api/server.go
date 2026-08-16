package api

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"log"
	"net/http"
	"strconv"
	"streamflow/broker"
	"strings"
	"sync"
	"time"
)

var (
	metricsOnce sync.Once
	reqTotal    = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "streamflow",
			Subsystem: "management",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests handled by the management API.",
		},
		[]string{"method", "path", "status"},
	)
	reqDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "streamflow",
			Subsystem: "management",
			Name:      "http_request_duration_seconds",
			Help:      "Request duration in seconds for management API handlers.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
	produceThroughput = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "streamflow",
			Subsystem: "broker",
			Name:      "produce_records_total",
			Help:      "Total records produced through management API.",
		},
		[]string{"topic"},
	)
	consumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "streamflow",
			Subsystem: "broker",
			Name:      "consumer_lag",
			Help:      "Simple consumer lag gauge based on latest produced offset and committed offset.",
		},
		[]string{"topic", "group"},
	)
	windowCloseLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "streamflow",
			Subsystem: "processor",
			Name:      "window_close_latency_seconds",
			Help:      "Latency of closing windows in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
	)
)

type Server struct {
	broker *broker.Broker
	apiKey string
}

func NewServer(b *broker.Broker) *Server {
	return NewServerWithAPIKey(b, "")
}

func NewServerWithAPIKey(b *broker.Broker, apiKey string) *Server {
	metricsOnce.Do(func() {
		prometheus.MustRegister(reqTotal, reqDuration, produceThroughput, consumerLag, windowCloseLatency)
	})
	return &Server{broker: b, apiKey: apiKey}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/topics", s.handleTopics)
	mux.HandleFunc("/produce", s.handleProduce)
	mux.HandleFunc("/produce/idempotent", s.handleProduceIdempotent)
	mux.HandleFunc("/consume", s.handleConsume)
	mux.HandleFunc("/offset/commit", s.handleCommitOffset)
	mux.HandleFunc("/offset", s.handleOffset)
	mux.HandleFunc("/admin/drain", s.handleDrain)
	mux.HandleFunc("/tx/begin", s.handleTxBegin)
	mux.HandleFunc("/tx/produce", s.handleTxProduce)
	mux.HandleFunc("/tx/commit", s.handleTxCommit)
	mux.HandleFunc("/tx/abort", s.handleTxAbort)
	mux.HandleFunc("/graphql", s.handleGraphQL)
	mux.HandleFunc("/admin/window-close", s.handleWindowClose)

	var h http.Handler = mux
	h = withAuth(h, s.apiKey)
	h = withMetrics(h)
	h = withAuditLog(h)
	h = otelhttp.NewHandler(h, "streamflow-management-api")
	return h
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		status := strconv.Itoa(rec.status)
		reqTotal.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		reqDuration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(start).Seconds())
	})
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
		if err == broker.ErrDraining {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	produceThroughput.WithLabelValues(req.Topic).Inc()
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleProduceIdempotent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		Topic      string            `json:"topic"`
		Key        string            `json:"key"`
		Value      string            `json:"value"`
		Headers    map[string]string `json:"headers,omitempty"`
		ProducerID string            `json:"producer_id"`
		Sequence   int64             `json:"sequence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" || req.ProducerID == "" {
		http.Error(w, "invalid idempotent produce payload", http.StatusBadRequest)
		return
	}
	if _, err := s.broker.CreateTopic(req.Topic); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rec, dup, err := s.broker.ProduceIdempotent(req.Topic, []byte(req.Key), []byte(req.Value), req.Headers, req.ProducerID, req.Sequence)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	produceThroughput.WithLabelValues(req.Topic).Inc()
	writeJSON(w, http.StatusOK, map[string]interface{}{"duplicate": dup, "record": rec})
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
	if latest, err := s.broker.Consume(req.Topic, req.Offset, 1); err == nil {
		if len(latest) > 0 {
			lag := latest[0].Offset - req.Offset
			if lag < 0 {
				lag = 0
			}
			consumerLag.WithLabelValues(req.Topic, req.Group).Set(float64(lag))
		}
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

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		DurationSeconds int `json:"duration_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid drain payload", http.StatusBadRequest)
		return
	}
	if req.DurationSeconds <= 0 || req.DurationSeconds > 3600 {
		http.Error(w, "duration_seconds must be in range 1..3600", http.StatusBadRequest)
		return
	}

	d := time.Duration(req.DurationSeconds) * time.Second
	s.broker.Drain(d)
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "draining", "duration_seconds": req.DurationSeconds})
}

func (s *Server) handleTxBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		ProducerID string `json:"producer_id"`
		Epoch      int64  `json:"epoch"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ProducerID == "" {
		http.Error(w, "invalid tx begin payload", http.StatusBadRequest)
		return
	}
	txID, err := s.broker.BeginTransaction(req.ProducerID, req.Epoch)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"tx_id": txID})
}

func (s *Server) handleTxProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		TxID    string            `json:"tx_id"`
		Topic   string            `json:"topic"`
		Key     string            `json:"key"`
		Value   string            `json:"value"`
		Headers map[string]string `json:"headers,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TxID == "" || req.Topic == "" {
		http.Error(w, "invalid tx produce payload", http.StatusBadRequest)
		return
	}
	if _, err := s.broker.CreateTopic(req.Topic); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.broker.TxProduce(req.TxID, req.Topic, []byte(req.Key), []byte(req.Value), req.Headers); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "buffered"})
}

func (s *Server) handleTxCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		TxID string `json:"tx_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TxID == "" {
		http.Error(w, "invalid tx commit payload", http.StatusBadRequest)
		return
	}
	n, err := s.broker.CommitTransaction(req.TxID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "committed", "records": n})
}

func (s *Server) handleTxAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		TxID string `json:"tx_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TxID == "" {
		http.Error(w, "invalid tx abort payload", http.StatusBadRequest)
		return
	}
	if err := s.broker.AbortTransaction(req.TxID); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (s *Server) handleGraphQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid graphql payload", http.StatusBadRequest)
		return
	}
	q := strings.ToLower(req.Query)
	if strings.Contains(q, "topics") {
		writeJSON(w, http.StatusOK, map[string]interface{}{"data": map[string]interface{}{"topics": s.broker.ListTopics()}})
		return
	}
	if strings.Contains(q, "health") {
		writeJSON(w, http.StatusOK, map[string]interface{}{"data": map[string]interface{}{"health": "ok"}})
		return
	}
	http.Error(w, "unsupported graphql query", http.StatusBadRequest)
}

func (s *Server) handleWindowClose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req struct {
		LatencyMS float64 `json:"latency_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LatencyMS < 0 {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	windowCloseLatency.Observe(req.LatencyMS / 1000.0)
	writeJSON(w, http.StatusOK, map[string]string{"status": "observed"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withAuth(next http.Handler, apiKey string) http.Handler {
	if strings.TrimSpace(apiKey) == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("X-API-Key") != apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withAuditLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("management request method=%s path=%s status=%d latency_ms=%d", r.Method, r.URL.Path, rec.status, time.Since(start).Milliseconds())
	})
}
