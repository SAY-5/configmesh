// Package store implements the versioned key/value layer that backs
// ConfigMesh. Keys are versioned with a monotonic per-key counter, and
// values are stored at the (key, version) pair so reads can be made
// version-aware. Deletes are tombstones at a new version, not erasures.
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Get when the key has never been written.
var ErrNotFound = errors.New("store: not found")

// Entry is the value-at-a-version row returned by Get and pushed to subscribers.
type Entry struct {
	Key     string
	Version uint64
	Value   []byte
	Deleted bool
}

// Store is a versioned KV layer backed by Redis.
//
// Key layout in Redis:
//
//	cm:version:<key>            -> uint64 monotonic counter (INCR)
//	cm:value:<key>:<version>    -> bytes (latest value at that version)
//	cm:tomb:<key>:<version>     -> "1" marker if this version is a delete
//	cm:index                    -> SET of all known keys (for List)
//
// All writes go through a small Lua script so the version bump and the
// value/tombstone write happen atomically; without that, a crash between
// INCR and SET would create a version with no value (a read-your-write hole).
type Store struct {
	rdb *redis.Client
}

// New returns a Store wired to the given Redis client.
func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

const (
	versionKeyPrefix = "cm:version:"
	valueKeyPrefix   = "cm:value:"
	tombKeyPrefix    = "cm:tomb:"
	indexSetKey      = "cm:index"
)

// putScript bumps the version counter and writes the value at that version
// atomically. Returns the new version.
//
// KEYS[1] = cm:version:<key>
// KEYS[2] = cm:value:<key>:NEW   (built in Go because the new version is unknown until INCR; see below)
// We use a single key + ARGV to encode the value-key template and let the
// script splice the version onto it. This keeps the operation atomic.
//
//	KEYS[1] = version counter key
//	KEYS[2] = value key prefix (e.g. "cm:value:<key>:" -- script appends version)
//	KEYS[3] = tomb key prefix
//	KEYS[4] = index set key
//	ARGV[1] = key name (for the index set)
//	ARGV[2] = value bytes
//	ARGV[3] = "1" if this write is a delete, "0" otherwise
const putScript = `
local v = redis.call('INCR', KEYS[1])
local value_key = KEYS[2] .. v
if ARGV[3] == "1" then
  redis.call('SET', KEYS[3] .. v, "1")
  redis.call('SET', value_key, "")
else
  redis.call('SET', value_key, ARGV[2])
end
redis.call('SADD', KEYS[4], ARGV[1])
return v
`

func (s *Store) put(ctx context.Context, key string, value []byte, tombstone bool) (uint64, error) {
	if key == "" {
		return 0, errors.New("store: empty key")
	}
	keys := []string{
		versionKeyPrefix + key,
		valueKeyPrefix + key + ":",
		tombKeyPrefix + key + ":",
		indexSetKey,
	}
	tombFlag := "0"
	if tombstone {
		tombFlag = "1"
	}
	res, err := s.rdb.Eval(ctx, putScript, keys, key, value, tombFlag).Result()
	if err != nil {
		return 0, fmt.Errorf("store: put eval: %w", err)
	}
	v, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("store: put eval: unexpected result type %T", res)
	}
	if v <= 0 {
		return 0, fmt.Errorf("store: put returned non-positive version %d", v)
	}
	return uint64(v), nil
}

// Put writes value at a new version for key and returns that version.
func (s *Store) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return s.put(ctx, key, value, false)
}

// Delete writes a tombstone at a new version. Reads after this point
// return Entry{Deleted: true}.
func (s *Store) Delete(ctx context.Context, key string) (uint64, error) {
	return s.put(ctx, key, nil, true)
}

// Get returns the latest entry for key. If the key has never been written
// it returns ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (Entry, error) {
	if key == "" {
		return Entry{}, errors.New("store: empty key")
	}
	v, err := s.rdb.Get(ctx, versionKeyPrefix+key).Int64()
	if errors.Is(err, redis.Nil) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("store: get version: %w", err)
	}
	return s.getAtVersion(ctx, key, uint64(v))
}

// GetAtVersion returns the entry for key at the specified version. This is
// used by the subscriber hub to replay missed updates without races against
// later writes.
func (s *Store) GetAtVersion(ctx context.Context, key string, version uint64) (Entry, error) {
	return s.getAtVersion(ctx, key, version)
}

func (s *Store) getAtVersion(ctx context.Context, key string, version uint64) (Entry, error) {
	valueKey := valueKeyPrefix + key + ":" + strconv.FormatUint(version, 10)
	tombKey := tombKeyPrefix + key + ":" + strconv.FormatUint(version, 10)
	val, err := s.rdb.Get(ctx, valueKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("store: get value: %w", err)
	}
	tomb, err := s.rdb.Exists(ctx, tombKey).Result()
	if err != nil {
		return Entry{}, fmt.Errorf("store: get tomb: %w", err)
	}
	return Entry{
		Key:     key,
		Version: version,
		Value:   val,
		Deleted: tomb == 1,
	}, nil
}

// LatestVersion returns the latest version for key, or 0 if the key has
// never been written.
func (s *Store) LatestVersion(ctx context.Context, key string) (uint64, error) {
	v, err := s.rdb.Get(ctx, versionKeyPrefix+key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: latest version: %w", err)
	}
	if v < 0 {
		return 0, fmt.Errorf("store: negative version %d", v)
	}
	return uint64(v), nil
}

// List returns all keys whose name starts with prefix. Used for the unary
// List RPC; for large key counts a real deployment would paginate. The
// scan iterates the index set, so the cost is proportional to the number
// of distinct keys, not to the number of versions.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, indexSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("store: smembers: %w", err)
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		if prefix == "" || strings.HasPrefix(m, prefix) {
			out = append(out, m)
		}
	}
	return out, nil
}
