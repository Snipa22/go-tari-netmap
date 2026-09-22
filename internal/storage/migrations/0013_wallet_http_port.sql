-- 0013_wallet_http_port.sql
--
-- Adds wallet_http_port (nullable int) to both nodes and pending_submissions, backing the
-- wallet-sync HTTP service ("wallet query service") tracking feature -- see
-- Netmap-Wallet-HTTP-Port-Design-2026-09-19.md for the full design and the HARD safety rules
-- this column's population is bound by everywhere else in the codebase (storage.Node's own
-- doc comment on this field has the short version). In one sentence: minotari_node can
-- optionally run a second, unauthenticated Axum HTTP/REST server (config section
-- [base_node.http_wallet_query_service]) separate from its gRPC/P2P surface, used by
-- third-party wallets to sync against a public node -- this column records, for a node netmap
-- already knows about, which port (if any) that service listens on.
--
-- wallet_http_port is added directly to nodes (not a joined side table) -- it always pairs
-- with that SAME node's own address (never a different host, see the design doc), so a plain
-- nullable column is sufficient and avoids an unnecessary join for the one dashboard page that
-- reads it.
--
-- CRITICALLY: this migration only adds the column. It does NOT populate it for any existing
-- row, and no code path anywhere in this codebase may ever populate it except (a) the
-- NETMAP_OWNED_WALLET_HTTP_ADDRESSES env var (owned nodes) or (b) an explicit
-- wallet_http_port field on a public POST /nodes submission, carried through at admin
-- approval -- see internal/api/api.go's handleApproveSubmission and
-- internal/collector/collector.go's syncOwnedWalletHTTPPorts. There is deliberately no
-- passive/automatic discovery of this port from any other source (P2P handshake, peer info,
-- RPC calls to the node, port-scanning, per-network default-port tables, etc) -- see the
-- design doc's "HARD RULES" section, rule 1.
--
-- The CHECK constraint mirrors req/port range validation already enforced at the Go layer
-- (internal/api's validateSubmittedHost/handleCreateNode-style 1-65535 range checks) as a
-- defense-in-depth database-level invariant, same spirit as node_health.probe_source's CHECK
-- constraint below.
--
-- probe_source's existing CHECK constraint (added by 0003_probe_source.sql, values 'grpc'/
-- 'p2p') is widened here to also allow 'wallet_http' -- the new probe transport this feature
-- adds (see internal/collector's new wallet-sync HTTP client, which calls ONLY
-- /get_tip_info -- never /fetch_utxo, /transactions, /sync_utxos_by_block,
-- /get_utxos_by_block, or /json_rpc, all of which can return real UTXO/transaction data; see
-- that client's own doc comment for the full rationale). Postgres auto-names an unnamed
-- inline CHECK constraint '<table>_<column>_check', which is what 0003 relied on implicitly --
-- DROP/ADD by that same generated name here rather than introducing an explicit name only
-- now, to keep this a minimal, mechanical widening of the existing constraint.
--
-- This migration must always succeed for the binary to start, same as every other
-- non-"_optional" migration in this directory -- see internal/storage/migrate.go for how
-- that distinction is enforced.

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS wallet_http_port integer
    CHECK (wallet_http_port IS NULL OR (wallet_http_port >= 1 AND wallet_http_port <= 65535));

ALTER TABLE pending_submissions ADD COLUMN IF NOT EXISTS wallet_http_port integer
    CHECK (wallet_http_port IS NULL OR (wallet_http_port >= 1 AND wallet_http_port <= 65535));

ALTER TABLE node_health DROP CONSTRAINT IF EXISTS node_health_probe_source_check;
ALTER TABLE node_health ADD CONSTRAINT node_health_probe_source_check
    CHECK (probe_source IN ('grpc', 'p2p', 'wallet_http'));
