package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestShardedLimiterRefillAndConcurrency(t *testing.T) {
	now := time.Now()
	l := NewShardedLimiter(10, 5)
	l.nowFn = func() time.Time { return now }

	for range 5 {
		if !l.Allow("tenant-a") {
			t.Fatal("expected burst token to be allowed")
		}
	}
	if l.Allow("tenant-a") {
		t.Fatal("expected 6th immediate request to be rate-limited")
	}

	now = now.Add(200 * time.Millisecond) // +2 tokens
	if !l.Allow("tenant-a") || !l.Allow("tenant-a") {
		t.Fatal("expected 2 refilled tokens")
	}
	if l.Allow("tenant-a") {
		t.Fatal("expected exhaustion after 2 tokens")
	}

	// Race test across 32 goroutines
	l2 := NewShardedLimiter(10_000, 1000)
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", id%8)
			for range 100 {
				l2.Allow(key)
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkShardedLimiterAllow(b *testing.B) {
	l := NewShardedLimiter(1_000_000, 1_000_000)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Allow(fmt.Sprintf("tenant-%d", i&63))
			i++
		}
	})
}
