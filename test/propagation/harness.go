// Package propagation is the 50-instance live-push harness.
//
// What it measures: the wall-clock interval between a server-side Put
// returning and every subscribed client receiving the corresponding
// ConfigUpdate over its gRPC bidi stream. It reports median and p95
// across all (write, client) pairs that match the write.
//
// What makes the measurement honest:
//   - The "issued at" timestamp is captured immediately *before* the Put
//     RPC, not from inside the server (server-side timestamps would hide
//     network and serialization).
//   - The "received at" timestamp is captured the moment the client's
//     stream.Recv returns the update. We deliberately do not include
//     the cost of decoding or downstream client work.
//   - All clients run in the same process as the server, connected over
//     bufconn. There is no network jitter and no kernel TCP queue. This
//     intentionally measures the server's fan-out path, the Redis round
//     trip for the write, and gRPC framing. Real-network deployments
//     will see higher numbers; the SLO here is for the in-process path.
//   - Writes are issued one at a time with a small random spacing. This
//     avoids saturating the rate limiter or batching unrealistically.
package propagation

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAY-5/configmesh/internal/hub"
	"github.com/SAY-5/configmesh/internal/ratelimit"
	"github.com/SAY-5/configmesh/internal/server"
	"github.com/SAY-5/configmesh/internal/store"
	configmeshv1 "github.com/SAY-5/configmesh/proto/configmeshv1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Config controls the harness.
type Config struct {
	Clients         int
	Writes          int
	KeysPerClient   int
	TotalKeys       int
	WriteSpacingMin time.Duration
	WriteSpacingMax time.Duration
	RecvTimeout     time.Duration
	Seed            int64
	RedisAddr       string // if empty, harness fails -- the runner provides it
	WarmupWrites    int
}

// DefaultConfig is the canonical 50-client topology.
func DefaultConfig(redisAddr string) Config {
	return Config{
		Clients:         50,
		Writes:          100,
		KeysPerClient:   4,
		TotalKeys:       20,
		WriteSpacingMin: 5 * time.Millisecond,
		WriteSpacingMax: 30 * time.Millisecond,
		RecvTimeout:     3 * time.Second,
		Seed:            1,
		RedisAddr:       redisAddr,
		WarmupWrites:    5,
	}
}

// Result is the JSON-serializable summary.
type Result struct {
	Generated      time.Time `json:"generated_at"`
	GoVersion      string    `json:"go_version"`
	Clients        int       `json:"clients"`
	Writes         int       `json:"writes"`
	TotalKeys      int       `json:"total_keys"`
	KeysPerClient  int       `json:"keys_per_client"`
	Pairs          int       `json:"pairs"`
	MedianMicros   int64     `json:"median_micros"`
	P95Micros      int64     `json:"p95_micros"`
	P99Micros      int64     `json:"p99_micros"`
	MinMicros      int64     `json:"min_micros"`
	MaxMicros      int64     `json:"max_micros"`
	DroppedPairs   int       `json:"dropped_pairs"`
	WallTimeMillis int64     `json:"wall_time_millis"`
	Note           string    `json:"note"`
	Mode           string    `json:"mode"`
}

// Run executes the harness against cfg.RedisAddr. The server, hub,
// ratelimiter, and clients all run in this same process; the only
// out-of-process dependency is Redis.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.RedisAddr == "" {
		return Result{}, fmt.Errorf("propagation: RedisAddr is required")
	}
	rng := rand.New(rand.NewSource(cfg.Seed)) //nolint:gosec // deterministic test seed

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close() //nolint:errcheck
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		return Result{}, fmt.Errorf("flushall: %w", err)
	}
	st := store.New(rdb)
	h := hub.New(1024)
	// Generous limits: we want to measure propagation, not rate-limit
	// throttling. The rate limiter is exercised in its own unit and
	// integration tests.
	l := ratelimit.New(rdb, ratelimit.Config{Capacity: 1_000_000, RefillPerSecond: 1_000_000})
	srv := server.New(st, h, l)
	srv.OpenCost = 1
	srv.UnaryCost = 1

	lis := bufconn.Listen(8 * 1024 * 1024)
	gs := grpc.NewServer(grpc.MaxConcurrentStreams(uint32(cfg.Clients * 4)))
	configmeshv1.RegisterConfigServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	// Build the key universe.
	keys := make([]string, cfg.TotalKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("flag.k%02d", i)
	}

	// Per-client watch sets. Each client watches KeysPerClient keys,
	// drawn so that the union of watch sets covers all keys.
	clientKeys := make([][]string, cfg.Clients)
	for c := 0; c < cfg.Clients; c++ {
		pick := make(map[string]struct{}, cfg.KeysPerClient)
		for len(pick) < cfg.KeysPerClient {
			pick[keys[rng.Intn(len(keys))]] = struct{}{}
		}
		ck := make([]string, 0, len(pick))
		for k := range pick {
			ck = append(ck, k)
		}
		sort.Strings(ck)
		clientKeys[c] = ck
	}
	// keyWatchers[k] = list of client indices watching k.
	keyWatchers := make(map[string][]int, len(keys))
	for c, ks := range clientKeys {
		for _, k := range ks {
			keyWatchers[k] = append(keyWatchers[k], c)
		}
	}

	// Per-client recv channels: each Update arriving on the stream is
	// pushed with the wall-clock recv time. The main goroutine pairs
	// these with write timestamps.
	type recvEvent struct {
		client  int
		key     string
		version uint64
		recvAt  time.Time
	}
	recvCh := make(chan recvEvent, cfg.Clients*cfg.Writes*4)

	// Open all client streams.
	var clientConns []*grpc.ClientConn
	defer func() {
		for _, c := range clientConns {
			_ = c.Close()
		}
	}()
	var openWG sync.WaitGroup
	startedSubs := make(chan struct{}, cfg.Clients)
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	for c := 0; c < cfg.Clients; c++ {
		c := c
		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return Result{}, fmt.Errorf("client %d: dial: %w", c, err)
		}
		clientConns = append(clientConns, conn)
		client := configmeshv1.NewConfigClient(conn)
		stream, err := client.Subscribe(clientCtx)
		if err != nil {
			return Result{}, fmt.Errorf("client %d: subscribe: %w", c, err)
		}
		clientID := fmt.Sprintf("client-%03d", c)
		if err := stream.Send(&configmeshv1.SubscribeRequest{
			ClientId: clientID,
			Keys:     clientKeys[c],
		}); err != nil {
			return Result{}, fmt.Errorf("client %d: send watch: %w", c, err)
		}
		openWG.Add(1)
		go func() {
			defer openWG.Done()
			startedSubs <- struct{}{}
			for {
				up, err := stream.Recv()
				if err != nil {
					return
				}
				recvCh <- recvEvent{
					client:  c,
					key:     up.Key,
					version: up.Version,
					recvAt:  time.Now(),
				}
			}
		}()
	}
	// Wait for every stream goroutine to start.
	for i := 0; i < cfg.Clients; i++ {
		<-startedSubs
	}
	// Sub-registration races the server-side Hub.Subscribe call inside
	// the stream handler. Sleep briefly so the hub has registered every
	// subscriber before we start measuring.
	time.Sleep(150 * time.Millisecond)

	// Warmup writes: not counted in the result. Lets gRPC connection
	// state, Redis Lua-script caching, and goroutine schedules settle.
	writerConn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return Result{}, fmt.Errorf("writer dial: %w", err)
	}
	defer writerConn.Close() //nolint:errcheck
	writerClient := configmeshv1.NewConfigClient(writerConn)

	for i := 0; i < cfg.WarmupWrites; i++ {
		k := keys[rng.Intn(len(keys))]
		_, err := writerClient.Put(clientCtx, &configmeshv1.PutRequest{
			ClientId: "writer",
			Key:      k,
			Value:    []byte(fmt.Sprintf("warmup-%d", i)),
		})
		if err != nil {
			return Result{}, fmt.Errorf("warmup put: %w", err)
		}
	}
	// Drain warmup recvs.
	drained := 0
	drainDeadline := time.After(500 * time.Millisecond)
DRAIN:
	for {
		select {
		case <-recvCh:
			drained++
		case <-drainDeadline:
			break DRAIN
		}
	}

	// Real measurement.
	type writeRecord struct {
		key      string
		version  uint64
		issuedAt time.Time
		watchers []int
	}
	writes := make([]writeRecord, 0, cfg.Writes)

	t0 := time.Now()
	for i := 0; i < cfg.Writes; i++ {
		k := keys[rng.Intn(len(keys))]
		val := []byte(fmt.Sprintf("v-%d", i))
		issuedAt := time.Now()
		resp, err := writerClient.Put(clientCtx, &configmeshv1.PutRequest{
			ClientId: "writer",
			Key:      k,
			Value:    val,
		})
		if err != nil {
			return Result{}, fmt.Errorf("put %d: %w", i, err)
		}
		writes = append(writes, writeRecord{
			key:      k,
			version:  resp.Version,
			issuedAt: issuedAt,
			watchers: keyWatchers[k],
		})
		gap := cfg.WriteSpacingMin
		if cfg.WriteSpacingMax > cfg.WriteSpacingMin {
			gap += time.Duration(rng.Int63n(int64(cfg.WriteSpacingMax - cfg.WriteSpacingMin)))
		}
		time.Sleep(gap)
	}

	// Collect recv events until we have one for every expected pair
	// or RecvTimeout elapses without progress.
	type pairKey struct {
		client  int
		key     string
		version uint64
	}
	want := make(map[pairKey]time.Time, 0)
	expectedPairs := 0
	for _, w := range writes {
		for _, c := range w.watchers {
			want[pairKey{client: c, key: w.key, version: w.version}] = w.issuedAt
			expectedPairs++
		}
	}
	if expectedPairs == 0 {
		return Result{}, fmt.Errorf("no watcher/write overlap; check key topology")
	}

	latencies := make([]int64, 0, expectedPairs)
	collected := atomic.Int64{}
	stopCollect := make(chan struct{})
	go func() {
		idle := time.NewTimer(cfg.RecvTimeout)
		defer idle.Stop()
		for {
			select {
			case <-stopCollect:
				return
			case ev := <-recvCh:
				pk := pairKey{client: ev.client, key: ev.key, version: ev.version}
				if issuedAt, ok := want[pk]; ok {
					latencies = append(latencies, ev.recvAt.Sub(issuedAt).Microseconds())
					delete(want, pk)
					collected.Add(1)
					if int(collected.Load()) >= expectedPairs {
						return
					}
				}
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
				idle.Reset(cfg.RecvTimeout)
			case <-idle.C:
				return
			}
		}
	}()

	// Active wait: poll collected count until we've got them all or
	// the collector closes.
	deadline := time.Now().Add(cfg.RecvTimeout + time.Second)
	for time.Now().Before(deadline) {
		if int(collected.Load()) >= expectedPairs {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stopCollect)
	// Brief drain to capture latencies appended by the collector after
	// the channel send below.
	time.Sleep(50 * time.Millisecond)
	wallEnd := time.Now()

	clientCancel() // stop client recv goroutines
	openWG.Wait()

	dropped := expectedPairs - len(latencies)
	if len(latencies) == 0 {
		return Result{}, fmt.Errorf("no propagations observed (expected %d)", expectedPairs)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) int64 {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx]
	}

	r := Result{
		Generated:      time.Now().UTC(),
		GoVersion:      runtimeGoVersion(),
		Clients:        cfg.Clients,
		Writes:         cfg.Writes,
		TotalKeys:      cfg.TotalKeys,
		KeysPerClient:  cfg.KeysPerClient,
		Pairs:          len(latencies),
		MedianMicros:   pct(0.50),
		P95Micros:      pct(0.95),
		P99Micros:      pct(0.99),
		MinMicros:      latencies[0],
		MaxMicros:      latencies[len(latencies)-1],
		DroppedPairs:   dropped,
		WallTimeMillis: wallEnd.Sub(t0).Milliseconds(),
		Note:           "All clients, server, hub, and rate limiter run in the same process over a bufconn transport. Redis (single instance) is the only out-of-process dependency. See ARCHITECTURE.md for methodology.",
		Mode:           "in-process bufconn + testcontainers redis",
	}
	return r, nil
}

// WriteResultFile serializes r to path as pretty-printed JSON.
func WriteResultFile(path string, r Result) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644) //nolint:gosec // committed test artifact
}
