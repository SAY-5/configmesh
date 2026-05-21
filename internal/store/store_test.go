//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/SAY-5/configmesh/internal/store"
	"github.com/SAY-5/configmesh/internal/testutil"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	rdb := testutil.RedisClient(t)
	return store.New(rdb)
}

func TestPut_AssignsMonotonicVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	var prev uint64
	for i := 0; i < 10; i++ {
		v, err := s.Put(ctx, "feature.x", []byte("v"))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if v <= prev {
			t.Fatalf("non-monotonic version: prev=%d new=%d", prev, v)
		}
		prev = v
	}
}

func TestGet_ReadYourWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	v, err := s.Put(ctx, "feature.read", []byte("hello"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "feature.read")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != v {
		t.Fatalf("version mismatch: want %d got %d", v, got.Version)
	}
	if string(got.Value) != "hello" {
		t.Fatalf("value mismatch: got %q", got.Value)
	}
	if got.Deleted {
		t.Fatalf("unexpected deleted=true")
	}
}

func TestDelete_AsTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	if _, err := s.Put(ctx, "feature.del", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	delV, err := s.Delete(ctx, "feature.del")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Get(ctx, "feature.del")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Deleted {
		t.Fatalf("expected deleted=true")
	}
	if got.Version != delV {
		t.Fatalf("version: want %d got %d", delV, got.Version)
	}
}

func TestGet_NotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	_, err := s.Get(ctx, "feature.never")
	if err == nil {
		t.Fatalf("expected ErrNotFound")
	}
}

func TestList_PrefixFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	keys := []string{"flag.a", "flag.b", "config.c"}
	for _, k := range keys {
		if _, err := s.Put(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	got, err := s.List(ctx, "flag.")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List(flag.): want 2 got %d (%v)", len(got), got)
	}
}

func TestGetAtVersion_OlderVersionReadable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newStore(t)

	v1, err := s.Put(ctx, "feature.ver", []byte("first"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Put(ctx, "feature.ver", []byte("second")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	old, err := s.GetAtVersion(ctx, "feature.ver", v1)
	if err != nil {
		t.Fatalf("GetAtVersion: %v", err)
	}
	if string(old.Value) != "first" {
		t.Fatalf("old version value: got %q", old.Value)
	}
}
