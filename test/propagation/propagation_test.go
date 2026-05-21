//go:build integration

package propagation_test

import (
	"context"
	"testing"
	"time"

	"github.com/SAY-5/configmesh/internal/testutil"
	"github.com/SAY-5/configmesh/test/propagation"
)

// TestPropagation_50Clients_Smoke is the CI canary. It runs the full
// 50-client topology with the standard 100 writes and asserts the
// median < 100ms and p95 < 200ms SLOs from the README.
func TestPropagation_50Clients_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("propagation test requires docker (testcontainers)")
	}
	addr := testutil.SharedRedisAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := propagation.DefaultConfig(addr)
	r, err := propagation.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("propagation: clients=%d writes=%d pairs=%d median=%dus p95=%dus p99=%dus dropped=%d wall=%dms",
		r.Clients, r.Writes, r.Pairs, r.MedianMicros, r.P95Micros, r.P99Micros, r.DroppedPairs, r.WallTimeMillis)

	if r.DroppedPairs > r.Pairs/100 { // allow <1% drop
		t.Fatalf("too many dropped pairs: %d / %d", r.DroppedPairs, r.Pairs+r.DroppedPairs)
	}
	const medianBudgetUs = 100_000 // 100ms
	const p95BudgetUs = 200_000    // 200ms
	if r.MedianMicros > medianBudgetUs {
		t.Fatalf("median %dus exceeds %dus budget", r.MedianMicros, medianBudgetUs)
	}
	if r.P95Micros > p95BudgetUs {
		t.Fatalf("p95 %dus exceeds %dus budget", r.P95Micros, p95BudgetUs)
	}
}
