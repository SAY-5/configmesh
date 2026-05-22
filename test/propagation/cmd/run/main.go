// Command run executes the propagation harness and writes its result
// to JSON. Brings up its own Redis via testcontainers so it's a single
// `go run` invocation with no external setup.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/SAY-5/configmesh/internal/testutil"
	"github.com/SAY-5/configmesh/test/propagation"
)

func main() {
	clients := flag.Int("clients", 50, "number of subscriber clients")
	writes := flag.Int("writes", 100, "number of writes to issue")
	out := flag.String("out", "propagation-result.json", "output JSON path")
	flag.Parse()

	// Use the testutil helper, which is `*testing.T` based.
	// We adapt to it via a fake T -- this is a runtime binary, so we
	// implement just enough of the surface to call testutil.SharedRedisAddr.
	addr := startRedis()

	cfg := propagation.DefaultConfig(addr)
	cfg.Clients = *clients
	cfg.Writes = *writes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := runOnce(ctx, cfg, *out); err != nil {
		// Allow deferred cancel() above to run before exiting.
		fmt.Fprintln(os.Stderr, err)
		_ = testutil.CloseShared
		os.Exit(1)
	}
	_ = testutil.CloseShared
}

func runOnce(ctx context.Context, cfg propagation.Config, out string) error {
	r, err := propagation.Run(ctx, cfg)
	if err != nil {
		return fmt.Errorf("propagation: %w", err)
	}
	if err := propagation.WriteResultFile(out, r); err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	fmt.Printf("clients=%d writes=%d pairs=%d median=%.2fms p95=%.2fms p99=%.2fms dropped=%d wall=%dms\n",
		r.Clients, r.Writes, r.Pairs,
		float64(r.MedianMicros)/1000,
		float64(r.P95Micros)/1000,
		float64(r.P99Micros)/1000,
		r.DroppedPairs, r.WallTimeMillis)
	fmt.Printf("wrote %s\n", out)
	return nil
}
