# configmesh

A distributed configuration and feature-flag service in Go. Clients
subscribe over a long-lived gRPC bidi stream; the server pushes config
changes within milliseconds of a write. Storage is Redis with monotonic
per-key versions. Per-client rate limiting on the streaming layer so a
misbehaving client (reconnect storm, runaway poll) cannot starve the
server.

## What this studies

- Versioned key-value storage with monotonic version numbers (atomic
  `INCR + SET`)
- gRPC bidi streaming as the push primitive (vs polling)
- Token-bucket rate limiting on a streaming protocol (Lua script in
  Redis for atomic try-consume)
- Stable-hash percentage rollouts for feature flags

## How this differs from sibling repos

| Axis | api-platform | edgemesh | configmesh |
| --- | --- | --- | --- |
| Layer | API gateway | service mesh data plane | config/flag distribution |
| Wire | HTTP REST | gRPC s2s | gRPC bidi streaming |
| Push semantics | poll | poll | server-initiated push |
| Domain | API keys + rate limits | retries + health | config + flags |

## Quickstart

```
make up                                 # docker-compose: configmesh + redis
go run ./cmd/configmesh-server &        # server on :7050
go run ./cmd/configmesh-server -mode=sample-client    # subscribe + observe pushes
```

## The 50-client propagation result

`test/propagation/cmd/run/` boots Redis via testcontainers, spins 50
concurrent gRPC subscribers, fires 100 random key mutations, and writes
`propagation-result.json` with the per-pair propagation distribution.
Committed result on local laptop hardware: see
`propagation-result.json`.

## What this is *not*

- Multi-region replicated config (single Redis; the swap path is in
  `ARCHITECTURE.md`)
- Open Policy Agent or general policy language (explicit flag eval only)
- Kubernetes ConfigMaps (static + reload-on-restart; this is
  push-on-write)
- LaunchDarkly (no audit log UI, no SDK ecosystem; the shape is
  comparable, the surface is intentionally smaller)
- Authenticated (mTLS hook present, not in scope today)

## License

MIT.
