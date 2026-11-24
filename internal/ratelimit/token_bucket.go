package ratelimit

import (
	"hash/fnv"
	"sync"
	"time"
)

const numShards = 64

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

// ShardedLimiter implements a 64-shard Token Bucket rate limiter to minimize
// lock contention across concurrent requests.
type ShardedLimiter struct {
	shards     [numShards]shard
	ratePerSec float64
	burst      float64
	nowFn      func() time.Time
}

func NewShardedLimiter(ratePerSec, burst float64) *ShardedLimiter {
	l := &ShardedLimiter{
		ratePerSec: ratePerSec,
		burst:      burst,
		nowFn:      time.Now,
	}
	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket)
	}
	return l
}

func (l *ShardedLimiter) Allow(key string) bool {
	s := &l.shards[shardIndex(key)]
	now := l.nowFn()

	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		s.buckets[key] = &bucket{tokens: l.burst - 1.0, lastRefill: now}
		return l.burst >= 1.0
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens = min(l.burst, b.tokens+elapsed*l.ratePerSec)
		b.lastRefill = now
	}

	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
}

func shardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % numShards
}
