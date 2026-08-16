# go-tari-netmap

Tari network peer-topology + node-health mapping tool for mining-pool operators picking peers.

## Status

Collector, storage, API, and web dashboard are implemented (see `AGENTS.md` for the pass that
built them). `go-tari-grpc-lib` is not wired in as a real dependency yet — the collector's
`NodeClient` interface stubs out the real gRPC calls behind a no-op placeholder client
(`collector.NewStubClient`) until that dependency is available; swapping in a real
implementation is a one-function change.

## Architecture

- **Collector service** (`internal/collector`) — polls Tari nodes via a `NodeClient` interface
  (a stand-in for `go-tari-grpc-lib`'s base-node client), discovering peers by walking the peer
  graph starting from a set of seed nodes.
- **Poll cadence / politeness** — discovered/generic nodes are polled at most once per hour, to
  avoid looking like abuse to the wider Tari network. Nodes explicitly tagged as pool-owned
  (`tags["pool_owned"] == true`) are polled every 5 minutes (see the constants in
  `internal/collector`).
- **Storage** (`internal/storage`) — Postgres, with an optional TimescaleDB hypertable for the
  `node_health` time-series table. Migrations live in `internal/storage/migrations` and are
  applied by a small embedded-`.sql`-file runner (`internal/storage/migrate.go`), tracked in a
  `schema_migrations` table.
  - **TimescaleDB is optional.** `0002_timescale_hypertable_optional.sql` converts
    `node_health` into a TimescaleDB hypertable, but not every target Postgres instance has a
    working TimescaleDB extension installed. The migration runner treats any migration file
    with `_optional` in its name as best-effort: if it fails, the error is logged and the
    migration is left unrecorded (so it's retried on the next startup) instead of aborting
    startup. The base `node_health` table itself is created unconditionally in
    `0001_init.sql` and must always succeed. (This project's own dev sandbox is a case where
    the optional step currently fails: Postgres 17 without a TimescaleDB binary compatible
    with that build, and no root access to install a matching one — the app still starts and
    works fine without the hypertable conversion.)
- **API** (`internal/api`) — an HTTP API (mounted at `/api/`) exposing node registry
  submission/listing, per-node health history, and the topology graph as JSON.
- **Web UI** (`internal/web`) — a server-rendered htmx + Go `html/template` dashboard (no
  separate JS framework, no frontend build step): a homepage with summary counts + a node
  table + a registry-submission form, and a per-node detail/history page. Public, no auth gate.

## Configuration

- `NETMAP_DATABASE_URL` — Postgres DSN. Defaults to `postgres://localhost:5432/netmap` if unset.
- `NETMAP_SEED_NODES` — comma-separated list of seed node addresses for peer-graph discovery.
  Defaults to empty (no discovery until seeds are configured).

## Development

```
go build ./...
go vet ./...
go test ./...
gofmt -l .
go mod tidy
```

Storage/API/collector tests exercise a real Postgres database via `TEST_DATABASE_URL` (falling
back to a sandbox-specific default DSN if unset) and `t.Skip` gracefully if it can't be reached.

See `AGENTS.md` for full conventions.

## License

MIT, see [LICENSE](LICENSE).

