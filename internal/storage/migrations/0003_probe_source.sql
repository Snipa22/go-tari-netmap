-- 0003_probe_source.sql
--
-- Adds a probe_source column to node_health, recording which transport a
-- given health check was collected over: 'grpc' (go-tari-grpc-lib, the
-- Tari base node's gRPC BaseNode service) or 'p2p' (go-tari-lib/p2p, a
-- direct Tari comms/RPC-over-P2P probe). This lets the same node accrue
-- independent health-check history per probe transport, since a node may
-- be reachable over one and not the other.
--
-- DEFAULT 'grpc' keeps existing rows (all of which predate this column and
-- were all collected via the gRPC path) and any INSERT that doesn't set it
-- explicitly valid. Going forward, every INSERT from the Go code sets this
-- explicitly (see storage.RecordHealthCheck) — the default exists purely
-- for backward compatibility, not as an invitation to omit it.
--
-- This migration must always succeed for the binary to start, same as
-- 0001_init.sql (no "_optional" marker) — see internal/storage/migrate.go
-- for how that distinction is enforced.

ALTER TABLE node_health ADD COLUMN IF NOT EXISTS probe_source text NOT NULL DEFAULT 'grpc' CHECK (probe_source IN ('grpc', 'p2p'));
