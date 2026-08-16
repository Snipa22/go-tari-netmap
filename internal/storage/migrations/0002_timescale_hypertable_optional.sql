-- 0002_timescale_hypertable_optional.sql
--
-- OPTIONAL migration: converts node_health into a TimescaleDB hypertable for
-- better time-series performance at scale (chunked storage, retention
-- policies, continuous aggregates, etc.).
--
-- This migration is ALLOWED TO FAIL. Not every target Postgres instance has
-- a working TimescaleDB extension installed. Notably, this project's dev
-- sandbox runs Postgres 17 without a compatible prebuilt TimescaleDB binary
-- available (ABI mismatch between the only obtainable prebuilt TimescaleDB
-- package and the only obtainable Postgres build, and there's no root
-- access in the sandbox to install matching packages instead).
--
-- The migration runner (see internal/storage/migrate.go) recognizes
-- migrations whose filename contains an "_optional" marker and, if running
-- them fails, logs the error and continues rather than treating the whole
-- migration run (and thus binary startup) as failed. The plain node_health
-- table itself is created unconditionally and non-optionally in
-- 0001_init.sql — only this hypertable conversion is best-effort.
--
-- Because this file is not recorded as applied when it fails, it will be
-- retried on every subsequent startup until it succeeds (e.g. once
-- TimescaleDB is actually installed on the target instance).

CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('node_health', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
