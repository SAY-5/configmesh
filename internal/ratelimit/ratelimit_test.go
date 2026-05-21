//go:build integration

package ratelimit_test

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SAY-5/configmesh/internal/ratelimit"
	"github.com/SAY-5/configmesh/internal/testutil"
)

func TestTryConsume_StartsAtFullCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 10, RefillPerSecond: 1}).
		WithClock(func() int64 { return 0 })

	for i := 0; i < 10; i++ {
		r, err := l.TryConsume(ctx, "c", 1)
		if err != nil {
			t.Fatalf("TryConsume: %v", err)
		}
		if !r.Allowed {
			t.Fatalf("call %d: expected allowed", i)
		}
	}
}

func TestTryConsume_RejectsWhenEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)
	clock := int64(0)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 2, RefillPerSecond: 1}).
		WithClock(func() int64 { return clock })

	for i := 0; i < 2; i++ {
		r, _ := l.TryConsume(ctx, "c", 1)
		if !r.Allowed {
			t.Fatalf("call %d should be allowed", i)
		}
	}
	r, err := l.TryConsume(ctx, "c", 1)
	if err != nil {
		t.Fatalf("TryConsume: %v", err)
	}
	if r.Allowed {
		t.Fatalf("expected rejection when bucket empty")
	}
	if r.RetryAfterMillis <= 0 {
		t.Fatalf("expected positive retry-after, got %d", r.RetryAfterMillis)
	}
}

func TestTryConsume_RefillsOverTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)
	clock := int64(0)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 2, RefillPerSecond: 10}).
		WithClock(func() int64 { return clock })

	// drain
	for i := 0; i < 2; i++ {
		l.TryConsume(ctx, "c", 1)
	}
	// 200ms passes -> 2 tokens refilled
	clock = 200
	r, err := l.TryConsume(ctx, "c", 1)
	if err != nil {
		t.Fatalf("TryConsume: %v", err)
	}
	if !r.Allowed {
		t.Fatalf("expected allowed after refill")
	}
}

func TestTryConsume_PropertyNeverNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)

	const cap_ = 50.0
	const refill = 25.0
	clock := int64(0)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: cap_, RefillPerSecond: refill}).
		WithClock(func() int64 { return clock })

	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test seed
	for i := 0; i < 2000; i++ {
		clock += int64(rng.Intn(50)) // 0..50ms jitter
		cost := float64(rng.Intn(3) + 1)
		r, err := l.TryConsume(ctx, "c", cost)
		if err != nil {
			t.Fatalf("TryConsume: %v", err)
		}
		if r.RemainingTokens < 0 {
			t.Fatalf("remaining went negative: %f (i=%d)", r.RemainingTokens, i)
		}
		if r.RemainingTokens > cap_+1e-6 {
			t.Fatalf("remaining exceeded capacity: %f (cap %f)", r.RemainingTokens, cap_)
		}
	}
}

func TestTryConsume_RefillMathMatchesContinuousTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)
	clock := int64(0)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 100, RefillPerSecond: 10}).
		WithClock(func() int64 { return clock })

	l.TryConsume(ctx, "c", 100) // drain
	// after 5000ms at 10/s we expect 50 tokens accrued.
	clock = 5000
	r, err := l.TryConsume(ctx, "c", 0) // probe
	if err != nil {
		t.Fatalf("TryConsume: %v", err)
	}
	if r.RemainingTokens < 49.9 || r.RemainingTokens > 50.1 {
		t.Fatalf("expected ~50 tokens after 5s, got %f", r.RemainingTokens)
	}
}

func TestTryConsume_AtomicityUnderConcurrency(t *testing.T) {
	// Multiple goroutines hammer one bucket. The bucket starts with N
	// tokens; total allowed grants across all goroutines must be <= N +
	// (elapsed_seconds * refill_rate). Run with tiny refill so the
	// inequality is tight.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := testutil.RedisClient(t)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 100, RefillPerSecond: 0.001})

	const goroutines = 16
	const each = 200

	var allowed atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wgDone int
		for wgDone < goroutines {
			<-time.After(time.Millisecond)
			wgDone++
		}
	}()

	t0 := time.Now()
	finished := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < each; i++ {
				r, err := l.TryConsume(ctx, "c", 1)
				if err != nil {
					t.Errorf("TryConsume: %v", err)
					finished <- struct{}{}
					return
				}
				if r.Allowed {
					allowed.Add(1)
				}
			}
			finished <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-finished
	}
	elapsed := time.Since(t0).Seconds()
	upper := 100 + int64(elapsed*0.001) + 2 // +2 floor slack
	if allowed.Load() > upper {
		t.Fatalf("over-consumed: allowed=%d upper=%d (elapsed=%.3fs)",
			allowed.Load(), upper, elapsed)
	}
}
