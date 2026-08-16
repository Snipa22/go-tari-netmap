# go-tari-netmap

Tari network peer-topology + node-health mapping tool for mining-pool operators picking peers.

## Status

Scaffold only. Repo structure, CI, and placeholder code are in place; the actual
collector/discovery/storage/API/UI implementation is a separate, not-yet-started follow-up.

## Architecture

The planned pieces:

- **Collector service** — polls Tari nodes via `go-tari-grpc-lib`, discovering peers by walking
  the peer graph starting from a set of seed nodes.
- **Poll cadence / politeness** — discovered/generic nodes are polled at most once per hour, to
  avoid looking like abuse to the wider Tari network. Nodes explicitly tagged as pool-owned are
  polled more frequently (exact interval TBD — see the placeholder constant in
  `internal/collector`).
- **Storage** — Postgres + TimescaleDB, on a dedicated instance (not shared infra).
- **API** — an HTTP API exposing topology/health data.
- **Web UI** — a server-rendered htmx + Go `html/template` dashboard (the fleet UI standard as
  of 2026-08-17 — no separate JS framework, no frontend build step). Public, no auth gate in v1.

## Development

```
go build ./...
go vet ./...
go test ./...
gofmt -l .
go mod tidy
```

See `AGENTS.md` for full conventions.

## License

MIT, see [LICENSE](LICENSE).
