//go:build integration

package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/SAY-5/configmesh/internal/hub"
	"github.com/SAY-5/configmesh/internal/ratelimit"
	"github.com/SAY-5/configmesh/internal/server"
	"github.com/SAY-5/configmesh/internal/store"
	"github.com/SAY-5/configmesh/internal/testutil"
	configmeshv1 "github.com/SAY-5/configmesh/proto/configmeshv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newTestServer(t *testing.T) (configmeshv1.ConfigClient, func()) {
	t.Helper()
	rdb := testutil.RedisClient(t)
	st := store.New(rdb)
	h := hub.New(64)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 1000, RefillPerSecond: 1000})
	srv := server.New(st, h, l)

	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	configmeshv1.RegisterConfigServer(gs, srv)
	go func() {
		_ = gs.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	client := configmeshv1.NewConfigClient(conn)
	cleanup := func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestServer_PutGet(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pr, err := client.Put(ctx, &configmeshv1.PutRequest{ClientId: "c", Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	gr, err := client.Get(ctx, &configmeshv1.GetRequest{ClientId: "c", Key: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !gr.Exists || gr.Version != pr.Version || string(gr.Value) != "v" {
		t.Fatalf("Get got %+v", gr)
	}
}

func TestServer_Subscribe_LivePush(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := stream.Send(&configmeshv1.SubscribeRequest{ClientId: "sub1", Keys: []string{"flag.live"}}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Give the server a moment to register the subscriber. Without this
	// there's a race where Put fires before Subscribe finishes opening.
	time.Sleep(50 * time.Millisecond)

	if _, err := client.Put(ctx, &configmeshv1.PutRequest{ClientId: "writer", Key: "flag.live", Value: []byte("on")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	up, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if up.Key != "flag.live" || string(up.Value) != "on" {
		t.Fatalf("update: %+v", up)
	}
}

func TestServer_Subscribe_BackfillOnOpen(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-existing value.
	if _, err := client.Put(ctx, &configmeshv1.PutRequest{ClientId: "writer", Key: "flag.backfill", Value: []byte("v")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := stream.Send(&configmeshv1.SubscribeRequest{ClientId: "sub1", Keys: []string{"flag.backfill"}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if up.Key != "flag.backfill" || string(up.Value) != "v" {
		t.Fatalf("backfill update: %+v", up)
	}
}

func TestServer_Evaluate_Flag(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	flagJSON := []byte(`{"percentage":100}`)
	if _, err := client.Put(ctx, &configmeshv1.PutRequest{ClientId: "w", Key: "flag.eval", Value: flagJSON}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, err := client.Evaluate(ctx, &configmeshv1.EvaluateRequest{ClientId: "c", FlagKey: "flag.eval", UserId: "u"})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !r.Enabled || r.Reason != "rollout-full" {
		t.Fatalf("Evaluate: %+v", r)
	}
}

func TestServer_RateLimited(t *testing.T) {
	// Force a tight bucket: 2 capacity, near-zero refill.
	rdb := testutil.RedisClient(t)
	st := store.New(rdb)
	h := hub.New(8)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 2, RefillPerSecond: 0.001})
	srv := server.New(st, h, l)

	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	configmeshv1.RegisterConfigServer(gs, srv)
	go gs.Serve(lis) //nolint:errcheck
	defer gs.Stop()

	conn, _ := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close() //nolint:errcheck
	c := configmeshv1.NewConfigClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Two calls should succeed (bucket starts full at 2), the third
	// must return ResourceExhausted.
	for i := 0; i < 2; i++ {
		if _, err := c.Get(ctx, &configmeshv1.GetRequest{ClientId: "noisy", Key: "x"}); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}
	_, err := c.Get(ctx, &configmeshv1.GetRequest{ClientId: "noisy", Key: "x"})
	if err == nil {
		t.Fatal("expected rate-limit error")
	}
}
