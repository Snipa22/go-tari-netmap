-- 0007_submission_queue.sql
--
-- Adds a review queue for public POST /nodes registry submissions.
--
-- Why: POST /nodes used to auto-publish straight into `nodes` via
-- UpsertDiscoveredNode, with no review step and no protection against
-- re-submitting an address that's already publicly opted-in
-- (discovery_source IN ('registry_submitted', 'both')) — a second
-- submission of an already-opted-in address would silently merge into the
-- existing node (bumping last_seen, merging tags, overwriting label) with
-- no ownership check at all. This table lets a human review a submission
-- before it's promoted into the real nodes table (see
-- Store.CreatePendingSubmission/ApprovePendingSubmission/
-- RejectPendingSubmission), and the partial unique index below prevents
-- more than one PENDING row from existing for the same address at a time.
--
-- gen_random_uuid() is built into Postgres core as of PG13+, no extension
-- required — matching 0001_init.sql's convention of not introducing
-- pgcrypto for uuid generation.
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0006 (no "_optional" marker) — see
-- internal/storage/migrate.go for how that distinction is enforced.

CREATE TABLE IF NOT EXISTS pending_submissions (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    address          text NOT NULL,
    label            text,
    owner_tag        text,
    status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    submitted_at     timestamptz NOT NULL DEFAULT now(),
    reviewed_at      timestamptz,
    rejection_reason text,
    promoted_node_id uuid REFERENCES nodes (id)
);

CREATE INDEX IF NOT EXISTS idx_pending_submissions_status ON pending_submissions (status);

-- A partial unique index scoped to status = 'pending' is the standard
-- Postgres pattern for "unique only while a row is in a given state" —
-- an address can have many approved/rejected submissions over time (its
-- history), but at most one pending submission at once. The app-level
-- CreatePendingSubmission path handles the "already has a pending row"
-- case by updating that row in place rather than ever hitting this
-- constraint (see its doc comment for why).
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_submissions_pending_address ON pending_submissions (address) WHERE status = 'pending';
