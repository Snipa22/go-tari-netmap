-- brief6_cleanup_bogus_p2p_addresses.sql
--
-- One-time, MANUALLY-RUN cleanup for the bug described in BRIEF6.md:
-- cmd/netmap-p2p-responder/responder.go's onPeerIdentity used to record the raw inbound TCP
-- connection's remote address (the client's OS-assigned ephemeral source port, e.g.
-- "104.161.20.146:49456") as if it were a real, dialable, self-claimed peer address. That bug
-- is now fixed (see responder.go's onPeerIdentity + storage.UpsertConfirmedNodeByPubKey), but
-- it has already been live for a while against both mainnet (CT129) and testnet (CT132)
-- responders, and has almost certainly written garbage rows into both environments'
-- node_addresses tables.
--
-- This file is DELIBERATELY NOT under internal/storage/migrations/ -- it must NEVER be run
-- automatically by storage.Store.Migrate() (which runs unconditionally, every startup, against
-- every environment). It is a one-time cleanup, run manually, once, by a human who has reviewed
-- the dry-run counts below.
--
-- ============================================================================================
-- DO NOT RUN THE DELETE STATEMENT IN THIS FILE AGAINST PRODUCTION (mainnet CT129 / testnet
-- CT132) POSTGRES WITHOUT SUPERVISOR REVIEW. The agent that wrote this file only ever ran it
-- against a disposable local/throwaway Postgres instance seeded with synthetic data (see
-- BRIEF6.md's report for that test's output) -- it has NEVER been run against real production
-- data, and this agent has no network access to production Postgres from its sandbox anyway.
-- ============================================================================================
--
-- Heuristic for "bad" (deliberately conservative -- see BRIEF6.md's own discussion of the
-- trade-off between an aggressive heuristic that risks deleting a legitimate node's
-- genuinely-configured non-standard port, and a conservative one that might leave a few
-- ambiguous garbage rows behind; this file picks conservative):
--
--   (A) address host is exactly 127.0.0.1, any port -- UNAMBIGUOUS garbage. No legitimate Tari
--       peer address is ever loopback; this can only be an artifact of the responder's own
--       localhost-sourced test/probe traffic being recorded as if it were a real peer address.
--
--   (B) address port is >= 32768 (the IANA ephemeral/dynamic port range an OS assigns an
--       outbound TCP connection's source port from -- see IANA "Dynamic Ports", also Linux's
--       default net.ipv4.ip_local_port_range starting around 32768) AND the SAME node_id has
--       at least one OTHER node_addresses row whose port falls in this repo's observed real
--       Tari port range (18000-18999 -- covers every real port literal already used in this
--       repo's own test fixtures/docs: 18141, 18142, 18150, 18189). This combination is exactly
--       this bug's fingerprint: a real peer that DID claim a real address (recorded correctly)
--       ALSO got a second, bogus row for the ephemeral source port of that same connection.
--       A node that only ever has ONE address, even if it's a genuinely-configured ephemeral-
--       looking port, does NOT match (B) requires a sibling standard-range address on the same
--       node_id) -- so this can't delete a real single-address node's real, if unusual, port.
--
-- Rows matching (A) OR (B) are backed up into a new table (brief6_bogus_node_addresses_backup)
-- BEFORE deletion, so the cleanup is trivially reversible without needing a full pg_dump
-- snapshot -- see the ROLLBACK section at the bottom of this file.

-- ============================================================================================
-- STEP 1: dry-run counts (SAFE -- read-only, no rows modified). Run this first and review the
-- numbers before going anywhere near the DELETE at the bottom of this file.
-- ============================================================================================

-- Category (A): loopback rows, unconditionally bad.
SELECT count(*) AS bad_loopback_rows
FROM node_addresses
WHERE split_part(address, ':', 1) = '127.0.0.1';

-- Category (B): ephemeral-port rows with a sibling standard-range address on the same node.
SELECT count(*) AS bad_ephemeral_with_standard_sibling_rows
FROM node_addresses na
WHERE
    -- na's own port is in the OS ephemeral range.
    (regexp_match(na.address, ':(\d+)$'))[1]::int >= 32768
    -- ...and na is NOT itself already in the standard Tari port range (so a node whose ONLY
    -- address happens to be, say, 18189 never matches itself here).
    AND (regexp_match(na.address, ':(\d+)$'))[1]::int NOT BETWEEN 18000 AND 18999
    AND EXISTS (
        SELECT 1
        FROM node_addresses sibling
        WHERE sibling.node_id = na.node_id
          AND sibling.id != na.id
          AND (regexp_match(sibling.address, ':(\d+)$'))[1]::int BETWEEN 18000 AND 18999
    );

-- Combined total (A) OR (B) -- this is the exact row count the DELETE at the bottom of this
-- file would remove. This is the number to report/review before anyone runs the DELETE for
-- real.
SELECT count(*) AS total_bad_rows
FROM node_addresses na
WHERE
    split_part(na.address, ':', 1) = '127.0.0.1'
    OR (
        (regexp_match(na.address, ':(\d+)$'))[1]::int >= 32768
        AND (regexp_match(na.address, ':(\d+)$'))[1]::int NOT BETWEEN 18000 AND 18999
        AND EXISTS (
            SELECT 1
            FROM node_addresses sibling
            WHERE sibling.node_id = na.node_id
              AND sibling.id != na.id
              AND (regexp_match(sibling.address, ':(\d+)$'))[1]::int BETWEEN 18000 AND 18999
        )
    );

-- Inspect the actual matching rows before deleting anything (sanity-check the heuristic against
-- real data -- e.g. confirm none of these are a node's ONLY address, which would indicate the
-- heuristic needs tightening for this specific environment's data before proceeding).
SELECT na.id, na.node_id, na.address, n.discovery_source, n.first_seen, n.last_seen
FROM node_addresses na
JOIN nodes n ON n.id = na.node_id
WHERE
    split_part(na.address, ':', 1) = '127.0.0.1'
    OR (
        (regexp_match(na.address, ':(\d+)$'))[1]::int >= 32768
        AND (regexp_match(na.address, ':(\d+)$'))[1]::int NOT BETWEEN 18000 AND 18999
        AND EXISTS (
            SELECT 1
            FROM node_addresses sibling
            WHERE sibling.node_id = na.node_id
              AND sibling.id != na.id
              AND (regexp_match(sibling.address, ':(\d+)$'))[1]::int BETWEEN 18000 AND 18999
        )
    )
ORDER BY na.first_seen;

-- ============================================================================================
-- STEP 2: backup + delete. DO NOT RUN past this point without supervisor review of STEP 1's
-- counts/row listing above.
-- ============================================================================================

-- Snapshot every row about to be deleted into a plain backup table first -- this is the
-- rollback mechanism (see bottom of file). Safe to re-run: DROP TABLE IF EXISTS first so a
-- second accidental run doesn't silently union two different snapshots together.
DROP TABLE IF EXISTS brief6_bogus_node_addresses_backup;
CREATE TABLE brief6_bogus_node_addresses_backup AS
SELECT na.*
FROM node_addresses na
WHERE
    split_part(na.address, ':', 1) = '127.0.0.1'
    OR (
        (regexp_match(na.address, ':(\d+)$'))[1]::int >= 32768
        AND (regexp_match(na.address, ':(\d+)$'))[1]::int NOT BETWEEN 18000 AND 18999
        AND EXISTS (
            SELECT 1
            FROM node_addresses sibling
            WHERE sibling.node_id = na.node_id
              AND sibling.id != na.id
              AND (regexp_match(sibling.address, ':(\d+)$'))[1]::int BETWEEN 18000 AND 18999
        )
    );

-- The actual cleanup. Matches STEP 1's "total_bad_rows" query exactly.
DELETE FROM node_addresses na
WHERE na.id IN (SELECT id FROM brief6_bogus_node_addresses_backup);

-- ============================================================================================
-- ROLLBACK: if this cleanup turns out to have been wrong (e.g. STEP 1's row listing turns up a
-- legitimate node in the deleted set that wasn't caught during review), restore every deleted
-- row from the backup table:
--
--   INSERT INTO node_addresses SELECT * FROM brief6_bogus_node_addresses_backup
--   ON CONFLICT (node_id, address) DO NOTHING;
--
-- The backup table (brief6_bogus_node_addresses_backup) is intentionally left in place after
-- the DELETE above -- it is NOT dropped automatically. Drop it manually once you're confident
-- the cleanup was correct and no rollback will be needed:
--
--   DROP TABLE brief6_bogus_node_addresses_backup;
-- ============================================================================================
