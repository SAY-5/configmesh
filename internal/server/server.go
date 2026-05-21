// Package server is the gRPC service implementation. It composes the
// store, hub, rate limiter, and flag evaluator into the surface defined
// in proto/config.proto.
//
// Control flow per request type:
//
//	Put/Delete: rate-limit Put, write via Store, then Hub.Publish.
//	Get:        rate-limit, read from Store.
//	List:       rate-limit, scan the Store index.
//	Subscribe:  rate-limit the open, register a Subscriber with Hub,
//	            spawn a recv loop that updates the watch set, and a
//	            send loop that forwards Hub updates to the stream.
//	            On open and on watch update, replay missed versions
//	            using last_known_versions.
//	Evaluate:   rate-limit, read from Store, parse with flags.Parse,
//	            decide.
package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SAY-5/configmesh/internal/flags"
	"github.com/SAY-5/configmesh/internal/hub"
	"github.com/SAY-5/configmesh/internal/ratelimit"
	"github.com/SAY-5/configmesh/internal/store"
	configmeshv1 "github.com/SAY-5/configmesh/proto/configmeshv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server holds the wired dependencies.
type Server struct {
	configmeshv1.UnimplementedConfigServer
	Store     *store.Store
	Hub       *hub.Hub
	Limiter   *ratelimit.Limiter
	UnaryCost float64
	OpenCost  float64
}

// New returns a Server.
func New(s *store.Store, h *hub.Hub, l *ratelimit.Limiter) *Server {
	return &Server{
		Store:     s,
		Hub:       h,
		Limiter:   l,
		UnaryCost: 1,
		OpenCost:  5,
	}
}

func (s *Server) checkLimit(ctx context.Context, clientID string, cost float64) error {
	if clientID == "" {
		return status.Error(codes.InvalidArgument, "client_id is required")
	}
	r, err := s.Limiter.TryConsume(ctx, clientID, cost)
	if err != nil {
		return status.Errorf(codes.Internal, "rate-limit check failed: %v", err)
	}
	if !r.Allowed {
		return status.Errorf(codes.ResourceExhausted,
			"rate limit exceeded; retry after %d ms", r.RetryAfterMillis)
	}
	return nil
}

// Put writes a new version of req.Key.
func (s *Server) Put(ctx context.Context, req *configmeshv1.PutRequest) (*configmeshv1.PutResponse, error) {
	if err := s.checkLimit(ctx, req.GetClientId(), s.UnaryCost); err != nil {
		return nil, err
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	v, err := s.Store.Put(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	s.Hub.Publish(hub.Update{
		Key:     req.GetKey(),
		Version: v,
		Value:   req.GetValue(),
		Deleted: false,
	})
	return &configmeshv1.PutResponse{Version: v}, nil
}

// Delete tombstones a key.
func (s *Server) Delete(ctx context.Context, req *configmeshv1.DeleteRequest) (*configmeshv1.DeleteResponse, error) {
	if err := s.checkLimit(ctx, req.GetClientId(), s.UnaryCost); err != nil {
		return nil, err
	}
	v, err := s.Store.Delete(ctx, req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	s.Hub.Publish(hub.Update{Key: req.GetKey(), Version: v, Deleted: true})
	return &configmeshv1.DeleteResponse{Version: v}, nil
}

// Get reads the latest version of req.Key.
func (s *Server) Get(ctx context.Context, req *configmeshv1.GetRequest) (*configmeshv1.GetResponse, error) {
	if err := s.checkLimit(ctx, req.GetClientId(), s.UnaryCost); err != nil {
		return nil, err
	}
	e, err := s.Store.Get(ctx, req.GetKey())
	if errors.Is(err, store.ErrNotFound) {
		return &configmeshv1.GetResponse{Exists: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &configmeshv1.GetResponse{
		Exists:  true,
		Version: e.Version,
		Value:   e.Value,
		Deleted: e.Deleted,
	}, nil
}

// List returns matching keys.
func (s *Server) List(ctx context.Context, req *configmeshv1.ListRequest) (*configmeshv1.ListResponse, error) {
	if err := s.checkLimit(ctx, req.GetClientId(), s.UnaryCost); err != nil {
		return nil, err
	}
	keys, err := s.Store.List(ctx, req.GetPrefix())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	return &configmeshv1.ListResponse{Keys: keys}, nil
}

// Evaluate runs server-side flag evaluation.
func (s *Server) Evaluate(ctx context.Context, req *configmeshv1.EvaluateRequest) (*configmeshv1.EvaluateResponse, error) {
	if err := s.checkLimit(ctx, req.GetClientId(), s.UnaryCost); err != nil {
		return nil, err
	}
	e, err := s.Store.Get(ctx, req.GetFlagKey())
	if errors.Is(err, store.ErrNotFound) {
		return &configmeshv1.EvaluateResponse{Enabled: false, Reason: "flag-missing"}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "evaluate: get flag: %v", err)
	}
	if e.Deleted {
		return &configmeshv1.EvaluateResponse{Enabled: false, FlagVersion: e.Version, Reason: "flag-deleted"}, nil
	}
	f, err := flags.Parse(e.Value)
	if err != nil {
		return &configmeshv1.EvaluateResponse{Enabled: false, FlagVersion: e.Version, Reason: "flag-malformed"}, nil
	}
	d := flags.Evaluate(req.GetFlagKey(), f, req.GetUserId())
	return &configmeshv1.EvaluateResponse{
		Enabled:     d.Enabled,
		FlagVersion: e.Version,
		Reason:      d.Reason,
	}, nil
}

// Subscribe is the bidi stream.
//
// recv loop: reads SubscribeRequests; each one (re)sets the watch set
// and triggers a backfill from last_known_versions.
//
// send loop: forwards hub updates to the wire.
//
// Either loop's error tears down the stream cleanly via ctx cancel.
func (s *Server) Subscribe(stream configmeshv1.Config_SubscribeServer) error {
	ctx := stream.Context()
	// First message: opens the subscription. Block until it arrives.
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "subscribe: first message: %v", err)
	}
	if first.GetClientId() == "" {
		return status.Error(codes.InvalidArgument, "client_id is required")
	}
	if err := s.checkLimit(ctx, first.GetClientId(), s.OpenCost); err != nil {
		return err
	}
	sub := s.Hub.Subscribe(first.GetClientId(), first.GetKeys())
	defer sub.Close()

	if err := s.backfill(ctx, stream, first.GetKeys(), first.GetLastKnownVersions()); err != nil {
		return err
	}

	// recv loop
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if err := s.checkLimit(ctx, first.GetClientId(), s.UnaryCost); err != nil {
				recvErr <- err
				return
			}
			sub.Watch(msg.GetKeys())
			if err := s.backfill(ctx, stream, msg.GetKeys(), msg.GetLastKnownVersions()); err != nil {
				recvErr <- err
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-recvErr:
			if isClientGone(err) {
				return nil
			}
			return err
		case u, ok := <-sub.Updates():
			if !ok {
				return nil
			}
			if err := stream.Send(&configmeshv1.ConfigUpdate{
				Key:     u.Key,
				Version: u.Version,
				Value:   u.Value,
				Deleted: u.Deleted,
			}); err != nil {
				return err
			}
		}
	}
}

// backfill replays any versions the client doesn't have for the keys
// it just asked to watch.
func (s *Server) backfill(ctx context.Context, stream configmeshv1.Config_SubscribeServer, keys []string, lkv map[string]uint64) error {
	// dedupe + bounded concurrency. Without bounding, a stream that
	// watches 10k keys could open 10k goroutines on the server.
	uniq := make(map[string]uint64, len(keys))
	for _, k := range keys {
		uniq[k] = lkv[k]
	}
	type result struct {
		key string
		e   store.Entry
		err error
	}
	const conc = 16
	sem := make(chan struct{}, conc)
	resCh := make(chan result, len(uniq))
	var wg sync.WaitGroup
	for k, lastV := range uniq {
		k := k
		lastV := lastV
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			e, err := s.Store.Get(ctx, k)
			if errors.Is(err, store.ErrNotFound) {
				return
			}
			if err != nil {
				resCh <- result{key: k, err: err}
				return
			}
			if e.Version > lastV {
				resCh <- result{key: k, e: e}
			}
		}()
	}
	wg.Wait()
	close(resCh)
	for r := range resCh {
		if r.err != nil {
			return status.Errorf(codes.Internal, "backfill: %v", r.err)
		}
		if err := stream.Send(&configmeshv1.ConfigUpdate{
			Key:     r.e.Key,
			Version: r.e.Version,
			Value:   r.e.Value,
			Deleted: r.e.Deleted,
		}); err != nil {
			return err
		}
	}
	return nil
}

func isClientGone(err error) bool {
	if err == nil {
		return false
	}
	// io.EOF, ctx cancel, and grpc Canceled are the normal teardown
	// paths. Anything else is a real error worth surfacing.
	s, ok := status.FromError(err)
	if ok && (s.Code() == codes.Canceled || s.Code() == codes.Unavailable) {
		return true
	}
	return err.Error() == "EOF" || errors.Is(err, context.Canceled)
}

// Compile-time assertion that Server satisfies the generated interface.
var _ configmeshv1.ConfigServer = (*Server)(nil)

// Ensure no unused import: fmt is wired through status.Errorf above.
var _ = fmt.Sprintf
