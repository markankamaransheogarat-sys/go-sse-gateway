package circuit

import (
	"sync/atomic"
	"time"
)

const (
	StateClosed int32 = iota
	StateOpen
	StateHalfOpen
)

// Breaker is a lock-free atomic circuit breaker (Closed -> Open -> HalfOpen).
type Breaker struct {
	state        atomic.Int32
	failures     atomic.Int64
	openedAtNano atomic.Int64
	threshold    int64
	cooldown     time.Duration
}

func NewBreaker(threshold int64, cooldown time.Duration) *Breaker {
	return &Breaker{
		threshold: threshold,
		cooldown:  cooldown,
	}
}

func (b *Breaker) Allow() bool {
	st := b.state.Load()
	if st == StateClosed {
		return true
	}
	if st == StateOpen {
		opened := time.Unix(0, b.openedAtNano.Load())
		if time.Since(opened) >= b.cooldown {
			return b.state.CompareAndSwap(StateOpen, StateHalfOpen)
		}
		return false
	}
	return false // HalfOpen already has a probe in flight
}

func (b *Breaker) RecordSuccess() {
	b.failures.Store(0)
	b.state.Store(StateClosed)
}

func (b *Breaker) RecordFailure() {
	if b.state.Load() == StateHalfOpen {
		b.openedAtNano.Store(time.Now().UnixNano())
		b.state.Store(StateOpen)
		return
	}
	if b.failures.Add(1) >= b.threshold {
		b.openedAtNano.Store(time.Now().UnixNano())
		b.state.Store(StateOpen)
	}
}

func (b *Breaker) State() int32 {
	return b.state.Load()
}
