-- 0012_report_batch_idempotency.sql
--
-- Adds a report_batch_id column to node_health and peer_edge_observations, used by
-- internal/api/collector_report.go's applyCollectorReport to make a retried collector report
-- batch idempotent (see this repo's readiness-review follow-up, Fix 3).
--
-- Why: internal/remotestore's flushOnce POSTs a bounded slice of its pending buffer to
-- POST /internal/collectors/report (see Config.FlushBatchSize). If the client sees a
-- network-level failure (e.g. a timeout) for a POST the server actually finished applying,
-- flushOnce restores that slice's items into the pending buffer for a later retry -- so the
-- exact same batch content can be resent and re-applied a second time. node_health and
-- peer_edge_observations are both append-only (no natural primary key to upsert against), so
-- without something to dedupe on, a retried batch duplicates rows.
--
-- report_batch_id is a deterministic UUID internal/remotestore derives from the exact byte
-- content of a given sub-batch POST (see flush.go's reportBatchID) -- a byte-identical retry
-- of the same buffered items always produces the same report_batch_id, while genuinely new
-- content (a different set of buffered items, e.g. after a partial success or new data
-- accumulating) produces a different one. NULL for every row written any other way (the local
-- collector's own direct writes, a central-side probe, a report batch with no report_id set
-- at all e.g. from an older/different client) -- dedup is opt-in per row, never applied
-- unless the writer explicitly supplied a batch id.
--
-- peer_edge_observations gets a real partial UNIQUE index (from_node_id, to_node_id,
-- report_batch_id) WHERE report_batch_id IS NOT NULL -- applyCollectorReport's
-- RecordPeerEdgeObservation call relies on `ON CONFLICT ... DO NOTHING` against this index for
-- atomic, race-safe dedup. peer_edge_observations is a plain table (no TimescaleDB
-- partitioning concerns).
--
-- node_health does NOT get a unique index here: it's a TimescaleDB hypertable candidate (see
-- 0002_timescale_hypertable_optional.sql/0005_node_health_hypertable_pk_fix.sql), and
-- TimescaleDB requires every UNIQUE constraint on a hypertable to include the partitioning
-- column (ts) -- which varies per insert attempt (assigned via now()), so a
-- (node_id, report_batch_id, ts) unique constraint would never actually catch a duplicate
-- retry (every retry gets a fresh ts). Instead, applyCollectorReport's RecordHealthCheck call
-- does a plain existence check (SELECT ... WHERE node_id = $1 AND report_batch_id = $2) before
-- inserting -- not fully race-safe under true concurrent retries of the identical batch, but
-- sufficient for the realistic scenario here (a single satellite's own flush loop only ever
-- has one flush in flight at a time, so retries are sequential, never concurrent with
-- themselves). A plain (non-unique) index on (node_id, report_batch_id) keeps that existence
-- check cheap.
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0005/0006/0009/0010/0011 (no "_optional" marker) -- see
-- internal/storage/migrate.go for how that distinction is enforced.

ALTER TABLE node_health ADD COLUMN IF NOT EXISTS report_batch_id uuid NULL;
CREATE INDEX IF NOT EXISTS idx_node_health_report_batch_id
    ON node_health (node_id, report_batch_id) WHERE report_batch_id IS NOT NULL;

ALTER TABLE peer_edge_observations ADD COLUMN IF NOT EXISTS report_batch_id uuid NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_peer_edge_observations_report_batch_id
    ON peer_edge_observations (from_node_id, to_node_id, report_batch_id) WHERE report_batch_id IS NOT NULL;
