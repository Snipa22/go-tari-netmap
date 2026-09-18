-- 0011_reported_by_collector.sql
--
-- Adds a reported_by_collector column to node_health and peer_edge_observations, recording
-- which trusted remote collector satellite (see internal/api/collector_auth.go's
-- X-Collector-Key convention) reported a given row via POST /internal/collectors/report --
-- see internal/api/collector_report.go's applyCollectorReport.
--
-- Before this migration, a satellite-reported health check or peer-edge observation was
-- indistinguishable from a centrally-observed one: if a satellite ever misbehaves (bad data,
-- a compromised API key), there was no way to identify or roll back what it fed in. This
-- column is set to the authenticated collector's configured name (see
-- NETMAP_COLLECTOR_KEYS'/parseCollectorKeys' collector_name) on every row applyCollectorReport
-- writes, and left NULL for every row written any other way (the local collector's own direct
-- Postgres writes, a central-side probe, etc.) -- NULL unambiguously means "not reported by a
-- remote collector satellite", never "reported by an unknown one".
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0005/0006/0009/0010 (no "_optional" marker) -- see
-- internal/storage/migrate.go for how that distinction is enforced.

ALTER TABLE node_health ADD COLUMN IF NOT EXISTS reported_by_collector text NULL;
ALTER TABLE peer_edge_observations ADD COLUMN IF NOT EXISTS reported_by_collector text NULL;
