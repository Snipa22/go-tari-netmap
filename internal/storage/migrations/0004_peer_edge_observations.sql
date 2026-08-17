-- 0004_peer_edge_observations.sql
--
-- Converts peer-topology edges from a single upserted-in-place row per
-- (from, to) pair into an append-only time series of observations.
--
-- Why: the previous peer_edges table did
--   INSERT ... ON CONFLICT (from_node_id, to_node_id) DO UPDATE SET last_seen = now()
-- which overwrites history on every re-discovery of the same edge. That
-- makes it impossible to later answer questions like "how did node X's
-- peer count change over time" or "which nodes have the most
-- connections" (as measured over some rolling window), since only the
-- most recent first_seen/last_seen pair per edge ever existed.
--
-- peer_edge_observations is now the source of truth: every discovery-walk
-- observation of a directed edge is inserted as its own row, never
-- updated or deduplicated at the storage layer. peer_edges becomes a
-- derived SQL VIEW over peer_edge_observations, rolling many
-- observations of the same (from, to) pair back into the old row shape
-- (id, from_node_id, to_node_id, first_seen, last_seen) so that
-- ListTopology's existing query against "peer_edges" keeps working
-- unmodified. This is pre-production with no existing peer_edges data
-- worth preserving, so this migration simply drops the old base table
-- and creates the observations table + view fresh — no backfill logic.
--
-- The view's synthesized "id" is the id of the earliest observation for
-- each (from_node_id, to_node_id) pair, picked via
-- (array_agg(id ORDER BY observed_at))[1] rather than min(id) — Postgres
-- has no min()/max() aggregate for the uuid type (only for types with a
-- "smallest common" ordering built in, which uuid isn't among), so
-- min(id) does not type-check. array_agg(... ORDER BY ...)[1] works for
-- any type and gives a stable, deterministic id (the observation that
-- produced first_seen).

DROP TABLE IF EXISTS peer_edges;

CREATE TABLE IF NOT EXISTS peer_edge_observations (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    from_node_id  uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    to_node_id    uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    observed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_peer_edge_observations_from_to_observed_at
    ON peer_edge_observations (from_node_id, to_node_id, observed_at);

CREATE INDEX IF NOT EXISTS idx_peer_edge_observations_observed_at
    ON peer_edge_observations (observed_at);

CREATE VIEW peer_edges AS
    SELECT
        (array_agg(id ORDER BY observed_at))[1] AS id,
        from_node_id,
        to_node_id,
        min(observed_at) AS first_seen,
        max(observed_at) AS last_seen
    FROM peer_edge_observations
    GROUP BY from_node_id, to_node_id;
