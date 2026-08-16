package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
