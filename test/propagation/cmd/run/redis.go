package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startRedis() string {
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	addr, err := startRedisOnce()
	if err != nil {
		// Bare Fprintln + os.Exit so callers can still defer cleanup outside
		// startRedis; here we have no outer deferred state to lose.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return addr
}

func startRedisOnce() (string, error) {
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
		return "", fmt.Errorf("start redis: %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("redis host: %w", err)
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return "", fmt.Errorf("redis port: %w", err)
	}
	return host + ":" + port.Port(), nil
}
