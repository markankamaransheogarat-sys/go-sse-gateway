package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/markankamaransheogarat-sys/go-sse-gateway/internal/ratelimit"
)

func TestSSEGatewayFailoverAndStreaming(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream down", http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	healthySSE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for i := range 3 {
			_, _ = fmt.Fprintf(w, "data: chunk-%d\n\n", i)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer healthySSE.Close()

	limiter := ratelimit.NewShardedLimiter(100, 10)
	gw, err := NewSSEGateway([]string{failing.URL, healthySSE.URL}, limiter)
	if err != nil {
		t.Fatalf("NewSSEGateway failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"prompt":"hi"}`))
	req.Header.Set("X-Tenant-Key", "t1")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 after failover, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "data: chunk-2") {
		t.Fatalf("unexpected SSE body: %s", string(body))
	}
	if gw.Metrics.Failovers.Load() != 1 {
		t.Fatalf("expected 1 failover, got %d", gw.Metrics.Failovers.Load())
	}
}

func TestSSEGatewayRateLimit(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer ok.Close()

	gw, _ := NewSSEGateway([]string{ok.URL}, ratelimit.NewShardedLimiter(1, 1))

	for i := range 2 {
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.Header.Set("X-Tenant-Key", "burst-1")
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		want := http.StatusOK
		if i == 1 {
			want = http.StatusTooManyRequests
		}
		if rec.Code != want {
			t.Fatalf("request %d: expected %d, got %d", i, want, rec.Code)
		}
	}
}
