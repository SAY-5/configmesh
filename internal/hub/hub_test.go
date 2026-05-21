package hub_test

import (
	"sync"
	"testing"
	"time"

	"github.com/SAY-5/configmesh/internal/hub"
)

func TestPublish_FanOutToAllSubscribers(t *testing.T) {
	h := hub.New(8)
	const n = 5
	subs := make([]*hub.Subscriber, n)
	for i := 0; i < n; i++ {
		subs[i] = h.Subscribe("c", []string{"k"})
	}
	if got := h.SubscriberCount("k"); got != n {
		t.Fatalf("SubscriberCount: want %d got %d", n, got)
	}
	delivered := h.Publish(hub.Update{Key: "k", Version: 1, Value: []byte("v")})
	if delivered != n {
		t.Fatalf("delivered: want %d got %d", n, delivered)
	}
	for i, s := range subs {
		select {
		case u := <-s.Updates():
			if u.Version != 1 || string(u.Value) != "v" {
				t.Fatalf("sub %d wrong update: %+v", i, u)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %d did not receive update", i)
		}
	}
}

func TestPublish_OnlyMatchingKeys(t *testing.T) {
	h := hub.New(4)
	a := h.Subscribe("a", []string{"k1"})
	b := h.Subscribe("b", []string{"k2"})
	h.Publish(hub.Update{Key: "k1", Version: 1})
	select {
	case u := <-a.Updates():
		if u.Key != "k1" {
			t.Fatalf("a got %s", u.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("a did not receive")
	}
	select {
	case u := <-b.Updates():
		t.Fatalf("b received unexpected update: %+v", u)
	case <-time.After(100 * time.Millisecond):
		// good: b should not see k1
	}
}

func TestWatch_AddRemoveKeys(t *testing.T) {
	h := hub.New(4)
	s := h.Subscribe("c", []string{"k1"})
	if h.SubscriberCount("k1") != 1 {
		t.Fatal("expected 1 on k1")
	}
	s.Watch([]string{"k2"})
	if h.SubscriberCount("k1") != 0 {
		t.Fatal("expected 0 on k1 after Watch swap")
	}
	if h.SubscriberCount("k2") != 1 {
		t.Fatal("expected 1 on k2 after Watch swap")
	}
}

func TestClose_RemovesSubscriber(t *testing.T) {
	h := hub.New(4)
	s := h.Subscribe("c", []string{"k"})
	s.Close()
	if h.SubscriberCount("k") != 0 {
		t.Fatal("expected 0 after Close")
	}
	// Calling Close again is fine.
	s.Close()
}

func TestPublish_BackpressureDrops(t *testing.T) {
	h := hub.New(1) // size 1
	s := h.Subscribe("c", []string{"k"})
	// First send fills the channel. Second send drops.
	h.Publish(hub.Update{Key: "k", Version: 1})
	h.Publish(hub.Update{Key: "k", Version: 2})
	if s.Drops() != 1 {
		t.Fatalf("drops: want 1 got %d", s.Drops())
	}
}

func TestConcurrentPublishAndSubscribe(_ *testing.T) {
	h := hub.New(256)
	const writers = 4
	const writes = 200
	const subs = 16

	subList := make([]*hub.Subscriber, subs)
	for i := range subList {
		subList[i] = h.Subscribe("c", []string{"k"})
	}

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				h.Publish(hub.Update{Key: "k", Version: uint64(i)})
			}
		}()
	}
	// Drain subscribers in parallel so the buffered channels don't stall.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	drainWG := sync.WaitGroup{}
	drainWG.Add(subs)
	for _, s := range subList {
		go func(s *hub.Subscriber) {
			defer drainWG.Done()
			for {
				select {
				case <-s.Updates():
				case <-time.After(200 * time.Millisecond):
					return
				}
			}
		}(s)
	}
	<-done
	drainWG.Wait()
}
