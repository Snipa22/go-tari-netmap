-- 0014_wallet_node_submissions.sql
--
-- Adds pending_wallet_submissions: a SEPARATE public review queue for wallet-sync
-- HTTP-port registrations, mirroring pending_submissions' SHAPE (public submit -> queued
-- 'pending' -> admin approve/reject) but as its own, independent resource -- NOT a field
-- bolted onto pending_submissions/nodes' existing base-node submission flow (see
-- 0013_wallet_http_port.sql's migration comment for that FIRST DRAFT's bundled approach,
-- which this table's introduction supersedes -- see internal/api/api.go's
-- handleCreateWalletNodeSubmission/handleApproveWalletSubmission for the new flow, and
-- 0015_drop_pending_submissions_wallet_http_port.sql for the corresponding un-bundling of
-- the old draft's pending_submissions.wallet_http_port column).
--
-- Why a separate table rather than reusing pending_submissions: this feature's operator
-- directives (see Netmap-Wallet-HTTP-Port-Design-2026-09-19.md and this feature's dispatch
-- brief) require (a) wallet_http_port to be UNCONDITIONALLY REQUIRED here (never optional,
-- unlike the old bundled draft's optional field on a base-node submission), and (b) linking
-- to an ALREADY-KNOWN node purely by public-IP HOST match (host, deliberately NOT
-- "host:port" -- a submitter never needs to know/reference that node's internal id or its
-- base P2P port) -- rather than a submission that itself creates a new node the way
-- pending_submissions/POST /nodes does. These are different enough semantics (a required
-- field vs. an optional one; "must already exist" vs. "may not exist yet") that bolting
-- them onto the existing table/columns would make CreatePendingSubmission's contract
-- ambiguous depending on which fields happen to be set -- a dedicated table keeps each
-- flow's invariants enforceable at the schema level (wallet_http_port NOT NULL here; no
-- such column on pending_submissions at all after 0015).
--
-- host (not "host:port"): this column intentionally has no format constraint beyond
-- NOT NULL -- Go-layer validation (internal/api's validateSubmittedHost, the same
-- SSRF-hardening check POST /nodes already applies) is the real gate on what a submitted
-- host may look like; this table doesn't duplicate that as a CHECK constraint, matching
-- pending_submissions.address's own precedent (also a bare `text NOT NULL`, no format
-- CHECK).
--
-- wallet_http_port NOT NULL, with the exact same range CHECK as
-- 0013_wallet_http_port.sql's nodes.wallet_http_port/pending_submissions.wallet_http_port
-- columns -- defense-in-depth mirroring that migration's own reasoning for why the DB-level
-- invariant costs nothing on top of the Go-layer 1-65535 range check.
--
-- promoted_node_id uuid REFERENCES nodes(id), nullable until approval -- mirrors
-- pending_submissions.promoted_node_id exactly, including the same mergeNodeInto
-- FK-repoint handling this codebase has already needed twice for that column (see
-- internal/storage/storage.go's mergeNodeInto, updated by this same change to also repoint
-- this new FK) -- a wallet-node submission's linked node can itself later be retired into
-- another node's id via a pubkey-confirmation merge, exactly like a base-node submission's
-- promoted node can.
--
-- probe_attempted_at/probe_reachable: the informational, non-blocking, submission-time
-- sanity-probe result shown to the admin reviewer (mirrors
-- pending_submissions.probe_attempted_at/probe_reachable from 0008_submission_probe_result.sql
-- exactly) -- NOT the approval-gating probe.
--
-- probe_succeeded/probe_checked_at/probe_is_synced/probe_height: the "permissions matrix"
-- (this feature's dispatch brief, operator directive #3) -- a small, honest record of
-- exactly what the SYNCHRONOUS, approval-GATING sanity probe
-- (walletHTTPClient.GetInfo -- GET /get_tip_info ONLY, see that client's own hard
-- safety-rule doc comment) confirmed, written ONLY at successful-approval time (see
-- internal/api/api.go's handleApproveWalletSubmission) -- never for a failed/rejected
-- approval attempt, and never at submission time. probe_succeeded is nullable but is only
-- ever written as `true` in practice: a submission whose approval-gating probe fails never
-- reaches the write that would set it, so the row simply stays 'pending' with these columns
-- still NULL, ready for a human to retry approval later or reject it outright.
--
-- This migration must always succeed for the binary to start, same as every other
-- non-"_optional" migration in this directory -- see internal/storage/migrate.go for how
-- that distinction is enforced.
--
-- gen_random_uuid() is built into Postgres core as of PG13+, no extension required --
-- matching every other migration in this directory's convention.

CREATE TABLE IF NOT EXISTS pending_wallet_submissions (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    host              text NOT NULL,
    wallet_http_port  integer NOT NULL CHECK (wallet_http_port >= 1 AND wallet_http_port <= 65535),
    status            text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    submitted_at      timestamptz NOT NULL DEFAULT now(),
    reviewed_at       timestamptz,
    rejection_reason  text,
    promoted_node_id  uuid REFERENCES nodes (id),
    probe_attempted_at timestamptz,
    probe_reachable   boolean,
    probe_succeeded   boolean,
    probe_checked_at  timestamptz,
    probe_is_synced   boolean,
    probe_height      bigint
);

CREATE INDEX IF NOT EXISTS idx_pending_wallet_submissions_status ON pending_wallet_submissions (status);

-- Same partial-unique-index pattern as idx_pending_submissions_pending_address
-- (0007_submission_queue.sql) -- at most one PENDING row per host at once; the app-level
-- CreatePendingWalletSubmission path handles an "already has a pending row for this host"
-- resubmission by updating that row in place (see its doc comment) rather than ever hitting
-- this constraint.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_wallet_submissions_pending_host ON pending_wallet_submissions (host) WHERE status = 'pending';
