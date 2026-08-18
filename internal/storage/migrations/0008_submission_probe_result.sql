-- 0008_submission_probe_result.sql
--
-- Adds a fast, best-effort connectivity signal recorded against a
-- still-pending submission at creation time (see
-- internal/api/probe.go's probeSubmission), so a human reviewer gets some
-- indication of reachability before deciding whether to approve or
-- reject a POST /nodes submission.
--
-- Why these columns live on pending_submissions, and NEVER on
-- node_health/nodes: the submitted address hasn't been approved yet —
-- nothing about an unreviewed submission belongs in the real,
-- publicly-facing tables. probe_reachable is purely informational for
-- the reviewer and is never treated as an authoritative health check
-- (that happens separately, post-approval, via the existing
-- async-health-check-on-approve path).
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0006/0007 (no "_optional" marker) — see
-- internal/storage/migrate.go for how that distinction is enforced.

ALTER TABLE pending_submissions
    ADD COLUMN IF NOT EXISTS probe_attempted_at timestamptz,
    ADD COLUMN IF NOT EXISTS probe_reachable boolean;
