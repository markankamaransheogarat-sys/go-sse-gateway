# go-sse-gateway

High-throughput HTTP & Server-Sent Events (SSE) reverse proxy in **Go 1.26** (zero external dependencies), inspired by `llm-cache-edge` and `tensor-gateway-rs`.

## Architecture
- **64-Shard Token Bucket Rate Limiter (`internal/ratelimit`)**: FNV-1a sharded mutexes per tenant key to eliminate global lock contention.
- **Lock-Free Atomic Circuit Breaker (`internal/circuit`)**: `Closed -> Open -> HalfOpen` state machine using `atomic.Int32` / `CompareAndSwap`.
- **Zero-Allocation Streaming Proxy (`internal/proxy`)**: `sync.Pool` 32KB copy buffers, immediate `http.Flusher` SSE chunk flushing, round-robin load balancing, and automatic failover on 5xx/network errors.
- **Prometheus `/metrics` & Graceful Shutdown**: Lock-free counters and `http.Server.Shutdown` drain on `SIGINT`/`SIGTERM`.

## Run & Benchmark
```bash
go test -race -v ./...
go test -bench=. -benchmem ./...
```
