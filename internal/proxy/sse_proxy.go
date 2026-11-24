package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/markankamaransheogarat-sys/go-sse-gateway/internal/circuit"
	"github.com/markankamaransheogarat-sys/go-sse-gateway/internal/ratelimit"
)

const copyBufSize = 32 * 1024

type Upstream struct {
	URL     *url.URL
	Breaker *circuit.Breaker
}

type Metrics struct {
	RequestsTotal  atomic.Int64
	RateLimited    atomic.Int64
	Failovers      atomic.Int64
	UpstreamErrors atomic.Int64
	BytesStreamed  atomic.Int64
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w,
		"# TYPE gateway_requests_total counter\ngateway_requests_total %d\n"+
			"# TYPE gateway_rate_limited_total counter\ngateway_rate_limited_total %d\n"+
			"# TYPE gateway_failovers_total counter\ngateway_failovers_total %d\n"+
			"# TYPE gateway_upstream_errors_total counter\ngateway_upstream_errors_total %d\n"+
			"# TYPE gateway_bytes_streamed_total counter\ngateway_bytes_streamed_total %d\n",
		m.RequestsTotal.Load(),
		m.RateLimited.Load(),
		m.Failovers.Load(),
		m.UpstreamErrors.Load(),
		m.BytesStreamed.Load(),
	)
}

// SSEGateway streams HTTP and Server-Sent Events responses with zero-allocation
// buffer pooling, sharded rate limiting, and circuit-broken upstream failover.
type SSEGateway struct {
	upstreams []*Upstream
	limiter   *ratelimit.ShardedLimiter
	client    *http.Client
	bufPool   sync.Pool
	Metrics   *Metrics
	rr        atomic.Uint64
}

func NewSSEGateway(upstreamURLs []string, limiter *ratelimit.ShardedLimiter) (*SSEGateway, error) {
	if len(upstreamURLs) == 0 {
		return nil, errors.New("at least one upstream URL required")
	}

	ups := make([]*Upstream, 0, len(upstreamURLs))
	for _, raw := range upstreamURLs {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		ups = append(ups, &Upstream{
			URL:     u,
			Breaker: circuit.NewBreaker(3, 15*time.Second),
		})
	}

	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}

	return &SSEGateway{
		upstreams: ups,
		limiter:   limiter,
		client:    &http.Client{Transport: transport},
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, copyBufSize)
				return &b
			},
		},
		Metrics: &Metrics{},
	}, nil
}

func (g *SSEGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.Metrics.RequestsTotal.Add(1)

	tenantKey := r.Header.Get("X-Tenant-Key")
	if tenantKey == "" {
		tenantKey = r.RemoteAddr
	}
	if !g.limiter.Allow(tenantKey) {
		g.Metrics.RateLimited.Add(1)
		http.Error(w, `{"error":"rate_limited"}`, http.StatusTooManyRequests)
		return
	}

	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			http.Error(w, `{"error":"bad_request_body"}`, http.StatusBadRequest)
			return
		}
	}

	startIdx := int(g.rr.Add(1)-1) % len(g.upstreams)
	tried := 0

	for i := range len(g.upstreams) {
		up := g.upstreams[(startIdx+i)%len(g.upstreams)]
		if !up.Breaker.Allow() {
			continue
		}
		tried++
		if i > 0 {
			g.Metrics.Failovers.Add(1)
		}

		resp, err := g.doUpstream(r, up.URL, bodyBytes)
		if err != nil || resp.StatusCode >= 500 {
			up.Breaker.RecordFailure()
			g.Metrics.UpstreamErrors.Add(1)
			if resp != nil {
				_ = resp.Body.Close()
			}
			continue
		}

		up.Breaker.RecordSuccess()
		g.streamResponse(w, resp)
		return
	}

	if tried == 0 {
		http.Error(w, `{"error":"all_circuits_open"}`, http.StatusServiceUnavailable)
		return
	}
	http.Error(w, `{"error":"all_upstreams_failed"}`, http.StatusBadGateway)
}

func (g *SSEGateway) doUpstream(r *http.Request, base *url.URL, bodyBytes []byte) (*http.Response, error) {
	target := *base
	target.Path = strings.TrimRight(base.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery

	var body io.Reader
	if len(bodyBytes) > 0 {
		body = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), body)
	if err != nil {
		return nil, err
	}
	copyHeaders(req.Header, r.Header)
	return g.client.Do(req)
}

func (g *SSEGateway) streamResponse(w http.ResponseWriter, resp *http.Response) {
	defer func() { _ = resp.Body.Close() }()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	flusher, canFlush := w.(http.Flusher)
	isSSE := strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")

	bufPtr := g.bufPool.Get().(*[]byte)
	defer g.bufPool.Put(bufPtr)
	buf := *bufPtr

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			written, wErr := w.Write(buf[:n])
			g.Metrics.BytesStreamed.Add(int64(written))
			if wErr != nil {
				return
			}
			if canFlush && isSSE {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vals := range src {
		if strings.EqualFold(k, "Connection") || strings.EqualFold(k, "Keep-Alive") || strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}
