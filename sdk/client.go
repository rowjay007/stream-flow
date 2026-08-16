package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client is a lightweight HTTP SDK client for StreamFlow broker/management APIs.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
}

func NewClient(brokerAddr string) *Client {
	base := strings.TrimRight(brokerAddr, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return &Client{baseURL: base, httpClient: &http.Client{}}
}

func (c *Client) WithAPIKey(apiKey string) *Client {
	c.apiKey = apiKey
	return c
}

func (c *Client) Produce(ctx context.Context, topic string, key, value []byte) error {
	payload := map[string]string{"topic": topic, "key": string(key), "value": string(value)}
	_, err := c.postJSON(ctx, "/produce", payload)
	return err
}

func (c *Client) ProduceIdempotent(ctx context.Context, topic string, key, value []byte, producerID string, sequence int64) error {
	payload := map[string]interface{}{
		"topic":       topic,
		"key":         string(key),
		"value":       string(value),
		"producer_id": producerID,
		"sequence":    sequence,
	}
	_, err := c.postJSON(ctx, "/produce/idempotent", payload)
	return err
}

func (c *Client) BeginTransaction(ctx context.Context, producerID string, epoch int64) (string, error) {
	resp, err := c.postJSON(ctx, "/tx/begin", map[string]interface{}{"producer_id": producerID, "epoch": epoch})
	if err != nil {
		return "", err
	}
	return fmt.Sprint(resp["tx_id"]), nil
}

func (c *Client) TxProduce(ctx context.Context, txID, topic string, key, value []byte) error {
	_, err := c.postJSON(ctx, "/tx/produce", map[string]interface{}{"tx_id": txID, "topic": topic, "key": string(key), "value": string(value)})
	return err
}

func (c *Client) CommitTransaction(ctx context.Context, txID string) error {
	_, err := c.postJSON(ctx, "/tx/commit", map[string]string{"tx_id": txID})
	return err
}

func (c *Client) AbortTransaction(ctx context.Context, txID string) error {
	_, err := c.postJSON(ctx, "/tx/abort", map[string]string{"tx_id": txID})
	return err
}

func (c *Client) Consume(ctx context.Context, topic string) (<-chan []byte, error) {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		recs, err := c.getRecords(ctx, topic)
		if err != nil {
			return
		}
		for _, rec := range recs {
			select {
			case <-ctx.Done():
				return
			case ch <- []byte(rec.Value):
			}
		}
	}()
	return ch, nil
}

type consumedRecord struct {
	Value string `json:"value"`
}

func (c *Client) getRecords(ctx context.Context, topic string) ([]consumedRecord, error) {
	u, err := url.Parse(c.baseURL + "/consume")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("topic", topic)
	q.Set("offset", "0")
	q.Set("max", "100")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("consume failed: %s", strings.TrimSpace(string(body)))
	}
	var recs []consumedRecord
	if err := json.NewDecoder(resp.Body).Decode(&recs); err != nil {
		return nil, err
	}
	return recs, nil
}

func (c *Client) postJSON(ctx context.Context, path string, payload interface{}) (map[string]interface{}, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && err != io.EOF {
		return nil, err
	}
	return out, nil
}
