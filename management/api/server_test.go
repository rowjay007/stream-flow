package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"streamflow/broker"
)

func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "broker")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, err := broker.NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	return NewServer(b).Handler()
}

func TestTopicsLifecycle(t *testing.T) {
	h := newTestServer(t)

	createReq := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewBufferString(`{"name":"orders"}`))
	createRec := httptest.NewRecorder()
	h.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create topic status: got=%d body=%s", createRec.Code, createRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/topics", nil)
	listRec := httptest.NewRecorder()
	h.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list topics status: got=%d", listRec.Code)
	}
	var got map[string][]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode topics: %v", err)
	}
	if len(got["topics"]) != 1 || got["topics"][0] != "orders" {
		t.Fatalf("unexpected topics: %#v", got)
	}
}

func TestProduceConsumeAndOffsets(t *testing.T) {
	h := newTestServer(t)

	produceBody := `{"topic":"orders","key":"k1","value":"v1"}`
	produceReq := httptest.NewRequest(http.MethodPost, "/produce", bytes.NewBufferString(produceBody))
	produceRec := httptest.NewRecorder()
	h.ServeHTTP(produceRec, produceReq)
	if produceRec.Code != http.StatusOK {
		t.Fatalf("produce status: got=%d body=%s", produceRec.Code, produceRec.Body.String())
	}

	consumeReq := httptest.NewRequest(http.MethodGet, "/consume?topic=orders&offset=0&max=10", nil)
	consumeRec := httptest.NewRecorder()
	h.ServeHTTP(consumeRec, consumeReq)
	if consumeRec.Code != http.StatusOK {
		t.Fatalf("consume status: got=%d body=%s", consumeRec.Code, consumeRec.Body.String())
	}
	var recs []map[string]interface{}
	if err := json.Unmarshal(consumeRec.Body.Bytes(), &recs); err != nil {
		t.Fatalf("decode consume: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got=%d", len(recs))
	}

	commitBody := `{"topic":"orders","group":"g1","offset":1}`
	commitReq := httptest.NewRequest(http.MethodPost, "/offset/commit", bytes.NewBufferString(commitBody))
	commitRec := httptest.NewRecorder()
	h.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusOK {
		t.Fatalf("commit status: got=%d body=%s", commitRec.Code, commitRec.Body.String())
	}

	offsetReq := httptest.NewRequest(http.MethodGet, "/offset?topic=orders&group=g1", nil)
	offsetRec := httptest.NewRecorder()
	h.ServeHTTP(offsetRec, offsetReq)
	if offsetRec.Code != http.StatusOK {
		t.Fatalf("offset status: got=%d body=%s", offsetRec.Code, offsetRec.Body.String())
	}
	var off map[string]interface{}
	if err := json.Unmarshal(offsetRec.Body.Bytes(), &off); err != nil {
		t.Fatalf("decode offset: %v", err)
	}
	if off["offset"].(float64) != 1 {
		t.Fatalf("unexpected offset response: %#v", off)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := newTestServer(t)

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRec := httptest.NewRecorder()
	h.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health status: got=%d", healthRec.Code)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	h.ServeHTTP(metricsRec, metricsReq)
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status: got=%d", metricsRec.Code)
	}
	body, err := io.ReadAll(metricsRec.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	s := string(body)
	if !strings.Contains(s, "streamflow_management_http_requests_total") {
		t.Fatalf("missing custom request metric")
	}
	if !strings.Contains(s, "streamflow_management_http_request_duration_seconds") {
		t.Fatalf("missing custom duration metric")
	}
}

func TestAuthMiddleware(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, err := broker.NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	h := NewServerWithAPIKey(b, "secret").Handler()

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRec := httptest.NewRecorder()
	h.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health should be public, got=%d", healthRec.Code)
	}

	unauthReq := httptest.NewRequest(http.MethodGet, "/topics", nil)
	unauthRec := httptest.NewRecorder()
	h.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got=%d", unauthRec.Code)
	}

	authReq := httptest.NewRequest(http.MethodGet, "/topics", nil)
	authReq.Header.Set("X-API-Key", "secret")
	authRec := httptest.NewRecorder()
	h.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("expected authorized topics response, got=%d", authRec.Code)
	}
}

func TestDrainEndpointBlocksProduce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, err := broker.NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	h := NewServerWithAPIKey(b, "secret").Handler()

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/drain", bytes.NewBufferString(`{"duration_seconds":1}`))
	drainReq.Header.Set("X-API-Key", "secret")
	drainRec := httptest.NewRecorder()
	h.ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK {
		t.Fatalf("drain status: got=%d body=%s", drainRec.Code, drainRec.Body.String())
	}

	produceReq := httptest.NewRequest(http.MethodPost, "/produce", bytes.NewBufferString(`{"topic":"orders","key":"k1","value":"v1"}`))
	produceReq.Header.Set("X-API-Key", "secret")
	produceRec := httptest.NewRecorder()
	h.ServeHTTP(produceRec, produceReq)
	if produceRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 while draining, got=%d body=%s", produceRec.Code, produceRec.Body.String())
	}

	time.Sleep(1100 * time.Millisecond)

	produceReq2 := httptest.NewRequest(http.MethodPost, "/produce", bytes.NewBufferString(`{"topic":"orders","key":"k1","value":"v1"}`))
	produceReq2.Header.Set("X-API-Key", "secret")
	produceRec2 := httptest.NewRecorder()
	h.ServeHTTP(produceRec2, produceReq2)
	if produceRec2.Code != http.StatusOK {
		t.Fatalf("expected produce to recover after drain, got=%d body=%s", produceRec2.Code, produceRec2.Body.String())
	}
}
