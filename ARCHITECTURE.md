# Architecture

## Shape

```
   client                       server                       Redis
   ----------------+            -----------+                 ---------
    Subscribe(stream) --gRPC--> Hub        |
                                  |        |
                                  |---fan-out
                                  v
                              ConfigStore  ---INCR/SET--->   versioned
                                  ^                          keys
   Put(k,v) ----------gRPC------> |
   Get(k)   ----------gRPC------> |
   Evaluate(flag, ctx) -gRPC----> Flag evaluator
```

## Versioned storage

Every config key has a monotonic version. A `Put` does:

```
INCR cm:version:<key>            # atomic version bump
SET  cm:value:<key>:<version> v  # write the new value
SET  cm:latest:<key> <version>   # pointer for reads
```

A `Get` reads `cm:latest:<key>` then the matching `cm:value:<key>:N`.
Clients can read by version directly for idempotent caching.

## Bidi-stream Subscribe

On `Subscribe` the client sends `{client_id, keys_of_interest,
last_known_versions{}}`. The server scans the keys, sends a backfill of
any value where the client's last_known version is below the latest,
then registers the stream in the in-memory hub. On every `Put` to a
subscribed key the hub fans out a `ConfigUpdate` event.

## Rate limiting

Token bucket per `client_id`: 100/min burst, 10/sec sustained. State
lives in Redis; an atomic Lua script `try_consume(client_id, tokens)`
runs the math. Excess calls return `RESOURCE_EXHAUSTED`. The rate limit
applies to `Get` and to `Subscribe` connects/reconnects, NOT to
server-initiated streamed updates.

## Feature-flag evaluation

A flag is shaped `{percentage: 0..100, on_for: [user_id], off_for:
[user_id]}`. Server-side eval uses stable hashing:

```
bucket = FNV(flag_key + user_id) % 100
on     = (user_id in on_for) OR (user_id not in off_for AND bucket < percentage)
```

Property test: same `(flag, user)` always returns the same `on`. Uniform
distribution across 100k random user_ids stays within 2 percentage
points of the configured `percentage`.

## The 50-client propagation test

50 simulated clients connect over bufconn-backed gRPC; each subscribes
to a small set of overlapping keys. The harness writes 100 random key
mutations and measures wall-clock from `Put` return to every interested
client receiving the matching `ConfigUpdate`. The committed
`propagation-result.json` records the median + p95 per write-client
pair.

## Concurrency

One Go process. A single `Hub` goroutine owns the subscriber map under a
mutex. Each subscriber has its own send channel; the hub drops a slow
subscriber rather than blocking ingestion. Redis is the only external
dependency.

## What is deliberately not here

- Multi-region replicated config (single Redis; document the swap)
- Open Policy Agent or any general policy language (explicit flag eval
  only)
- Kubernetes ConfigMaps integration (those are static; this is
  push-on-write)
- Per-environment promotion workflows
- Audit-log UI (the `audit_log` table is the source; no UI on top yet)
- Multi-tenant auth (single-team assumed)
