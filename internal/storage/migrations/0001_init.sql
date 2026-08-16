-- 0001_init.sql
--
-- Base schema for go-tari-netmap: nodes, peer topology edges, and the
-- node-health time-series table.
--
-- This migration must always succeed for the binary to start. TimescaleDB
-- hypertable conversion for node_health is handled separately in
-- 0002_timescale_hypertable_optional.sql, which is allowed to fail (see the
-- comment in that file and internal/storage/migrate.go for why).
--
-- gen_random_uuid() is built into Postgres core as of PG13+, no extension
-- required.

CREATE TABLE IF NOT EXISTS nodes (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    address          text NOT NULL UNIQUE,
    discovery_source text NOT NULL CHECK (discovery_source IN ('p2p_discovered', 'registry_submitted', 'both')),
    tags             jsonb NOT NULL DEFAULT '{}'::jsonb,
    label            text,
    first_seen       timestamptz NOT NULL DEFAULT now(),
    last_seen        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS peer_edges (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    from_node_id uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    to_node_id   uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    first_seen   timestamptz NOT NULL DEFAULT now(),
    last_seen    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (from_node_id, to_node_id)
);

-- node_health is the time-series table of per-node health-check results.
-- Hashrate is modeled as a few nullable numeric columns, one per algo,
-- rather than JSONB, so it stays directly queryable/aggregatable.
CREATE TABLE IF NOT EXISTS node_health (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    node_id          uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    ts               timestamptz NOT NULL DEFAULT now(),
    reachable        boolean NOT NULL,
    height           bigint,
    chain_tip_height bigint,
    version          text,
    latency_ms       integer,
    rxt_hashrate     numeric,
    c29_hashrate     numeric,
    sha3x_hashrate   numeric
);

CREATE INDEX IF NOT EXISTS idx_node_health_node_id_ts ON node_health (node_id, ts DESC);
