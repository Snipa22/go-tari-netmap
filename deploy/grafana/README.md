# Grafana Dashboards

Dashboard JSON export(s) for this repo's metrics-emitting binary. Uses
Grafana's exportable-dashboard shape (a `DS_PROMETHEUS` datasource input via
`__inputs` + `templating`), so it imports into any Grafana instance without
editing — no hardcoded datasource UID.

## Importing

1. Grafana UI → Dashboards → **New** → **Import**.
2. Paste the JSON (or upload the file).
3. When prompted for the `DS_PROMETHEUS` input, select your Prometheus
   datasource.
4. Import.

## Dashboards

| File | Paired binary | Notes |
|---|---|---|
| `netmap-collector-dashboard.json` | `cmd/netmap` | Known-nodes-by-source, poll queue backlog, poll result rate, collector/DB liveness, dashboard-API request rate + latency. Templated by `network` (mainnet/testnet). |

**Caveat (as of this branch, based off `main`):** `cmd/netmap` on `main` does
not yet expose these Prometheus metrics — no `metrics.go` / Prometheus
wiring exists on `main` at all. That wiring (plus a separate
`cmd/netmap-p2p-responder` binary) currently only exists on the unmerged
branch `feat/p2p-responder-db-wiring`
(commits `feat(observability): network-scope metric names and add metrics to
netmap binary` and `feat(p2p): add Prometheus metrics + /healthz to
netmap-p2p-responder`). The dashboard was built and verified against the
live fleet-monitor deployment (which is evidently running that
not-yet-merged code), but it will render "no data" against a `cmd/netmap`
built straight from current `main` until `feat/p2p-responder-db-wiring`
(or equivalent metrics wiring) lands.
