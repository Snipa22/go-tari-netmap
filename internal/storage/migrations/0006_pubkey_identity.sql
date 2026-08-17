-- 0006_pubkey_identity.sql
--
-- Reworks node identity from address-keyed to pubkey-keyed.
--
-- Why: real Tari identity is the node's Ristretto255 public key, not its
-- address. A single node can have multiple simultaneously-valid addresses
-- (onion, clearnet, IPv4/IPv6). Addresses discovered via peer-walk
-- (GetPeers) before we have directly, successfully probed that node
-- ourselves and learned its real pubkey are recorded as placeholder rows
-- (public_key NULL) that get merged/promoted once we do learn the pubkey
-- — see storage.UpsertDiscoveredNode/UpsertConfirmedNode.
--
-- This migration MUST be safe to run against a database that already has
-- real accumulated production data (existing nodes/node_health/
-- peer_edge_observations rows) — no data loss:
--   * nodes.public_key is added as a nullable column — every existing row
--     gets NULL, i.e. becomes an (address-only) placeholder under the new
--     model, which is exactly what it always was (no pubkey was ever
--     recorded before this migration).
--   * nodes.address's old UNIQUE constraint is dropped (a node can now
--     have multiple addresses, tracked in the new node_addresses table),
--     but the column itself and its data are left untouched.
--   * node_addresses is backfilled from every existing nodes row, so
--     existing address data isn't lost/orphaned by the model change.
--
-- It is idempotent (safe to re-run), consistent with this repo's other
-- migrations (0001-0005): IF NOT EXISTS / IF EXISTS guards throughout, and
-- the backfill INSERT uses ON CONFLICT DO NOTHING against node_addresses'
-- own UNIQUE(node_id, address) constraint.
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0005 (no "_optional" marker) — see
-- internal/storage/migrate.go for how that distinction is enforced.

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS public_key bytea;

-- A node's pubkey, once known, must be globally unique — but many rows
-- legitimately have no pubkey yet (placeholders), so this can't be a
-- plain UNIQUE constraint on the column: a partial index scoped to
-- non-NULL values is the standard Postgres pattern for "unique when
-- present, otherwise unconstrained".
CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_public_key_unique ON nodes (public_key) WHERE public_key IS NOT NULL;

-- nodes.address is no longer guaranteed unique: a node can have multiple
-- simultaneously-valid addresses (see node_addresses below), and two
-- different placeholder rows (each pubkey NULL, each representing a
-- not-yet-confirmed identity) may reference addresses that later turn out
-- to belong to the same real node. The real constraint name comes from
-- 0001_init.sql's `address text NOT NULL UNIQUE` column definition, which
-- Postgres names "nodes_address_key" by its standard
-- <table>_<column>_key convention for an inline UNIQUE column constraint.
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_address_key;

-- node_addresses tracks every address a node has ever been seen at,
-- independent of nodes.address (which remains populated as the
-- first-known/primary address for backward compatibility with existing
-- code paths, but is no longer the unique key for node identity).
CREATE TABLE IF NOT EXISTS node_addresses (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    node_id    uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    address    text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (node_id, address)
);

CREATE INDEX IF NOT EXISTS idx_node_addresses_address ON node_addresses (address);

-- Backfill: every existing node's primary address becomes its first
-- node_addresses row, preserving its original first_seen/last_seen rather
-- than resetting to now(). ON CONFLICT DO NOTHING makes this safe to
-- re-run (e.g. if the migration is somehow re-applied outside the normal
-- schema_migrations tracking).
INSERT INTO node_addresses (node_id, address, first_seen, last_seen)
    SELECT id, address, first_seen, last_seen FROM nodes
    ON CONFLICT (node_id, address) DO NOTHING;
