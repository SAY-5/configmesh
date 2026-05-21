// Package hub is the in-process fan-out for ConfigUpdate events.
//
// Every Subscribe stream owns a Subscriber. The server registers it with
// the Hub for each key the client cares about. On every successful Put or
// Delete, the server calls hub.Publish, which copies the Entry into every
// matching subscriber's channel.
//
// Backpressure: each subscriber has a bounded channel. If a subscriber's
// channel is full (a slow consumer), Publish records the drop and moves
// on rather than blocking the writer. The subscriber's reader notices the
// gap on its next Watch refresh (it sends last_known_versions and the
// server replays anything missing).
package hub

import (
	"sync"
	"sync/atomic"
)

// Update is the value pushed to subscribers. It mirrors the proto
// ConfigUpdate without coupling the hub to the generated package.
type Update struct {
	Key     string
	Version uint64
	Value   []byte
	Deleted bool
}

// Subscriber is one client's view into the hub. Multiple keys map to
// the same Subscriber so the client's stream goroutine reads a single
// channel.
type Subscriber struct {
	ID    string
	ch    chan Update
	drops atomic.Uint64
	mu    sync.Mutex
	keys  map[string]struct{}
	hub   *Hub
	done  chan struct{}
}

// Updates returns the channel the stream goroutine reads from.
func (s *Subscriber) Updates() <-chan Update { return s.ch }

// Drops returns the count of updates dropped due to a full channel.
// Exposed for observability and for test assertions.
func (s *Subscriber) Drops() uint64 { return s.drops.Load() }

// Keys returns the watch set as a snapshot. The hub keeps its own index
// for fan-out; this is for replay/diagnostics.
func (s *Subscriber) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.keys))
	for k := range s.keys {
		out = append(out, k)
	}
	return out
}

// Watch atomically replaces this subscriber's interest set with keys.
// Adds new keys to the hub's index and removes any keys no longer in
// the set. This is the surface the bidi stream's recv loop uses.
func (s *Subscriber) Watch(keys []string) {
	want := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		want[k] = struct{}{}
	}
	s.mu.Lock()
	old := s.keys
	s.keys = want
	s.mu.Unlock()

	// Diff under the hub's index lock.
	s.hub.diff(s, old, want)
}

// Close unregisters the subscriber from all keys and closes its channel.
// Idempotent.
func (s *Subscriber) Close() {
	s.hub.remove(s)
}

// Hub is the in-process fan-out registry.
type Hub struct {
	mu      sync.RWMutex
	byKey   map[string]map[*Subscriber]struct{}
	bufSize int
}

// New returns a Hub. bufSize is the per-subscriber channel size; when
// the channel is full the hub drops rather than blocks.
func New(bufSize int) *Hub {
	if bufSize <= 0 {
		bufSize = 64
	}
	return &Hub{
		byKey:   make(map[string]map[*Subscriber]struct{}),
		bufSize: bufSize,
	}
}

// Subscribe creates and registers a Subscriber with the given client ID
// and initial watch keys. The caller is responsible for calling Close.
func (h *Hub) Subscribe(clientID string, keys []string) *Subscriber {
	s := &Subscriber{
		ID:   clientID,
		ch:   make(chan Update, h.bufSize),
		keys: make(map[string]struct{}),
		hub:  h,
		done: make(chan struct{}),
	}
	s.Watch(keys)
	return s
}

// Publish fans an update out to every subscriber interested in u.Key.
// Returns the number of subscribers that received the update (not
// including dropped sends).
func (h *Hub) Publish(u Update) int {
	h.mu.RLock()
	subs := h.byKey[u.Key]
	// Snapshot pointers under the read lock; sending is done lock-free.
	pending := make([]*Subscriber, 0, len(subs))
	for s := range subs {
		pending = append(pending, s)
	}
	h.mu.RUnlock()

	delivered := 0
	for _, s := range pending {
		select {
		case s.ch <- u:
			delivered++
		default:
			s.drops.Add(1)
		}
	}
	return delivered
}

// SubscriberCount returns the number of subscribers currently watching
// the given key. For tests.
func (h *Hub) SubscriberCount(key string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byKey[key])
}

func (h *Hub) diff(s *Subscriber, old, want map[string]struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k := range want {
		if _, had := old[k]; had {
			continue
		}
		set, ok := h.byKey[k]
		if !ok {
			set = make(map[*Subscriber]struct{})
			h.byKey[k] = set
		}
		set[s] = struct{}{}
	}
	for k := range old {
		if _, still := want[k]; still {
			continue
		}
		if set, ok := h.byKey[k]; ok {
			delete(set, s)
			if len(set) == 0 {
				delete(h.byKey, k)
			}
		}
	}
}

func (h *Hub) remove(s *Subscriber) {
	h.mu.Lock()
	for k, set := range h.byKey {
		delete(set, s)
		if len(set) == 0 {
			delete(h.byKey, k)
		}
	}
	h.mu.Unlock()

	s.mu.Lock()
	select {
	case <-s.done:
		// already closed
	default:
		close(s.done)
		close(s.ch)
	}
	s.mu.Unlock()
}
