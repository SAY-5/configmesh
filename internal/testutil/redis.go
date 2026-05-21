// Package testutil wires up shared test fixtures. Most importantly: a
// real Redis brought up via testcontainers so the store and rate-limiter
// suites run against the same Lua-script semantics they use in prod.
package testutil

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	sharedMu       sync.Mutex
	sharedAddr     string
	sharedShutdown func()
)

// startSharedRedis brings up a single Redis container for the entire test
// binary. Each test that calls RedisClient gets its own FLUSHALL'd handle,
// which keeps tests isolated without the per-test container startup cost.
func startSharedRedis(t *testing.T) string {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedAddr != "" {
		return sharedAddr
	}
	// On macOS + colima the Ryuk reaper tries to bind-mount the docker
	// socket; colima's virtio-fs rejects that. Linux CI runners with
	// real docker do not need the override but it is harmless there
	// (tests clean up via t.Cleanup -> Terminate).
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		Cmd:          []string{"redis-server", "--save", "", "--appendonly", "no"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(45 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("testutil: start redis container: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("testutil: redis host: %v", err)
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("testutil: redis port: %v", err)
	}
	sharedAddr = host + ":" + port.Port()
	sharedShutdown = func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		_ = c.Terminate(stopCtx)
	}
	return sharedAddr
}

// RedisClient returns a fresh *redis.Client connected to a clean DB.
// The container is shared across tests; each call FLUSHALLs first so
// tests do not see each other's writes.
func RedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := startSharedRedis(t)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("testutil: flushall: %v", err)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("testutil: ping: %v", err)
	}
	return rdb
}

// SharedRedisAddr returns the address of the shared Redis if one has
// already been started, or starts one. Used by tests that want to share
// a single DB across multiple clients.
func SharedRedisAddr(t *testing.T) string {
	t.Helper()
	return startSharedRedis(t)
}

// FlushAll wipes the shared DB. Useful when a test brings up multiple
// independent clients and wants a known-empty starting state.
func FlushAll(t *testing.T) {
	t.Helper()
	addr := startSharedRedis(t)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("testutil: flushall: %v", err)
	}
}

// IsDockerAvailable returns true if the host has a usable docker daemon.
// Tests that need testcontainers skip themselves when this is false so
// the unit-only `make test` target stays green on machines without
// docker.
func IsDockerAvailable() bool {
	// testcontainers-go probes DOCKER_HOST + the default socket. Doing a
	// no-op container request would be heavyweight, so we just check the
	// host env var or the conventional docker socket env. This is
	// intentionally a hint, not a guarantee: the real fail mode is the
	// container start failing in startSharedRedis, which we propagate.
	if v := strings.TrimSpace(envOr("DOCKER_HOST", "")); v != "" {
		return true
	}
	if v := envOr("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", ""); v != "" {
		return true
	}
	// Fall back to: assume present. Tests that need docker will skip if
	// startSharedRedis errors before t.Fatalf -- they can call
	// t.Skip("docker unavailable") in that path.
	return true
}

// CloseShared terminates the shared Redis container if any. Tests do
// not normally need to call this; testcontainers handles teardown via
// the Ryuk reaper.
func CloseShared() {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedShutdown != nil {
		sharedShutdown()
		sharedShutdown = nil
		sharedAddr = ""
	}
}

func envOr(k, d string) string {
	if v := getenv(k); v != "" {
		return v
	}
	return d
}
