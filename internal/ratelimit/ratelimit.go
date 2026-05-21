// Package ratelimit is a Redis-backed per-client token bucket.
//
// The bucket has a capacity (burst) and a refill rate (tokens per
// second). Every TryConsume(clientID, cost) call deducts cost tokens
// after refilling for the elapsed time since the previous call. The
// operation is implemented as a single Lua script so the read-refill-
// write is atomic; otherwise two concurrent consumers could both see
// "enough tokens" and over-consume.
//
// Bucket state is { tokens float64, last_ms int64 } encoded as two
// Redis hash fields. The TTL is set to a few multiples of the refill
// window so idle clients eventually expire from Redis instead of
// accumulating forever.
package ratelimit

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Config describes a bucket policy.
type Config struct {
	// Capacity is the maximum number of tokens the bucket holds.
	Capacity float64
	// RefillPerSecond is the steady-state refill rate.
	RefillPerSecond float64
	// TTLSeconds is how long an idle bucket lives in Redis. Defaults
	// to max(60, ceil(2 * Capacity/RefillPerSecond)) if zero.
	TTLSeconds int64
}

// Limiter is the per-client token bucket service.
type Limiter struct {
	rdb *redis.Client
	cfg Config
	now nowFunc
}

type nowFunc func() int64 // unix milliseconds

// New returns a Limiter backed by rdb. clock is optional; nil uses the
// real wall clock (used only by tests).
func New(rdb *redis.Client, cfg Config) *Limiter {
	if cfg.TTLSeconds == 0 {
		cfg.TTLSeconds = int64(2 * cfg.Capacity / max1(cfg.RefillPerSecond))
		if cfg.TTLSeconds < 60 {
			cfg.TTLSeconds = 60
		}
	}
	return &Limiter{rdb: rdb, cfg: cfg, now: nowMillisReal}
}

// WithClock returns a copy of l using clock as the time source. For
// tests only.
func (l *Limiter) WithClock(clock func() int64) *Limiter {
	cp := *l
	cp.now = clock
	return &cp
}

// Result is the outcome of TryConsume.
type Result struct {
	Allowed         bool
	RemainingTokens float64
	// RetryAfterMillis is set when Allowed is false: how many ms until
	// enough tokens accrue to satisfy this exact request.
	RetryAfterMillis int64
}

// luaTryConsume is the atomic refill-and-deduct script.
//
//	KEYS[1] = bucket hash key
//	ARGV[1] = capacity        (float)
//	ARGV[2] = refill_per_sec  (float)
//	ARGV[3] = cost            (float)
//	ARGV[4] = now_ms          (int)
//	ARGV[5] = ttl_seconds     (int)
//
// Returns [allowed (0/1), remaining_tokens (string), retry_after_ms (int)]
const luaTryConsume = `
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'last_ms')
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  last_ms = now_ms
end
local elapsed_ms = now_ms - last_ms
if elapsed_ms < 0 then elapsed_ms = 0 end
local refill_amount = (elapsed_ms / 1000.0) * refill
tokens = tokens + refill_amount
if tokens > capacity then tokens = capacity end

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  local deficit = cost - tokens
  retry_after = math.ceil((deficit / refill) * 1000.0)
end

redis.call('HMSET', KEYS[1], 'tokens', tostring(tokens), 'last_ms', tostring(now_ms))
redis.call('EXPIRE', KEYS[1], ttl)
return {allowed, tostring(tokens), retry_after}
`

// TryConsume attempts to deduct cost tokens from the bucket for
// clientID. If the bucket has enough tokens (after refill) the call
// returns Result{Allowed: true}. Otherwise Allowed is false and
// RetryAfterMillis tells the caller when to retry.
func (l *Limiter) TryConsume(ctx context.Context, clientID string, cost float64) (Result, error) {
	if clientID == "" {
		return Result{}, errors.New("ratelimit: empty client id")
	}
	if cost < 0 {
		return Result{}, errors.New("ratelimit: negative cost")
	}
	key := "cm:bucket:" + clientID
	res, err := l.rdb.Eval(ctx, luaTryConsume, []string{key},
		l.cfg.Capacity, l.cfg.RefillPerSecond, cost, l.now(), l.cfg.TTLSeconds,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: eval: %w", err)
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return Result{}, fmt.Errorf("ratelimit: bad eval result %#v", res)
	}
	allowedI, _ := arr[0].(int64)
	tokensS, _ := arr[1].(string)
	retryI, _ := arr[2].(int64)
	var tokens float64
	if _, err := fmt.Sscanf(tokensS, "%f", &tokens); err != nil {
		return Result{}, fmt.Errorf("ratelimit: parse tokens %q: %w", tokensS, err)
	}
	return Result{
		Allowed:          allowedI == 1,
		RemainingTokens:  tokens,
		RetryAfterMillis: retryI,
	}, nil
}

func max1(x float64) float64 {
	if x < 1 {
		return 1
	}
	return x
}
