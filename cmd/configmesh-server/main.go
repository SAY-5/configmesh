// Command configmesh-server runs the ConfigMesh gRPC service on the
// configured listen address backed by a Redis instance.
//
// Flags / env:
//
//	-listen / CONFIGMESH_LISTEN       (default ":9090")
//	-redis  / CONFIGMESH_REDIS_ADDR   (default "127.0.0.1:6379")
//	-mode                              "server" (default) or "sample-client"
//	-server                            sample-client target address
//
// The "sample-client" mode is used by docker-compose to demonstrate
// the bidi stream: it connects, subscribes to a flag, prints updates.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SAY-5/configmesh/internal/hub"
	"github.com/SAY-5/configmesh/internal/ratelimit"
	"github.com/SAY-5/configmesh/internal/server"
	"github.com/SAY-5/configmesh/internal/store"
	configmeshv1 "github.com/SAY-5/configmesh/proto/configmeshv1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	mode := flag.String("mode", "server", `"server" or "sample-client"`)
	listen := flag.String("listen", envOr("CONFIGMESH_LISTEN", ":9090"), "listen address")
	redisAddr := flag.String("redis", envOr("CONFIGMESH_REDIS_ADDR", "127.0.0.1:6379"), "redis address")
	target := flag.String("server", envOr("CONFIGMESH_SERVER", "127.0.0.1:9090"), "sample-client target")
	flag.Parse()

	switch *mode {
	case "server":
		if err := runServerOnce(*listen, *redisAddr); err != nil {
			log.Fatalf("%v", err)
		}
	case "sample-client":
		if err := runSampleClient(*target); err != nil {
			log.Fatalf("%v", err)
		}
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func runServerOnce(listen, redisAddr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis ping at %s: %w", redisAddr, err)
	}

	st := store.New(rdb)
	h := hub.New(256)
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 100, RefillPerSecond: 10})
	srv := server.New(st, h, l)

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listen, err)
	}
	gs := grpc.NewServer()
	configmeshv1.RegisterConfigServer(gs, srv)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("configmesh: shutdown signal")
		gs.GracefulStop()
	}()
	log.Printf("configmesh: listening on %s (redis %s)", listen, redisAddr)
	if err := gs.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func runSampleClient(target string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", target, err)
	}
	defer conn.Close() //nolint:errcheck
	c := configmeshv1.NewConfigClient(conn)

	stream, err := c.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	if err := stream.Send(&configmeshv1.SubscribeRequest{
		ClientId: "sample-client",
		Keys:     []string{"flag.demo"},
	}); err != nil {
		return fmt.Errorf("send watch: %w", err)
	}
	fmt.Println("sample-client: subscribed to flag.demo, waiting for updates...")
	for {
		up, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		fmt.Printf("update: key=%s version=%d deleted=%v value=%q\n",
			up.Key, up.Version, up.Deleted, up.Value)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
