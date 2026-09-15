-- 0010_geoip_cache.sql
--
-- Adds a geoip_cache table backing the /map feature's spike (see
-- BRIEF.md): a lat/lon/city/country cache for IPv4 addresses belonging
-- to the owner-tagged + has_ipv4 node population, keyed by the raw IP
-- (not host:port -- a given IP is looked up once regardless of which
-- port(s) it's seen with).
--
-- lookup_failed lets a failed lookup be cached too, with its own much
-- shorter re-try TTL enforced application-side (see
-- internal/collector's GeoIPSuccessTTL/GeoIPFailedTTL) -- this is what
-- stops a persistently-unresolvable IP from being re-queried against
-- ip-api.com's free-tier rate limit on every single refresh tick.
--
-- This migration must always succeed for the binary to start, same as
-- 0001/0003/0004/0005/0006/0009 (no "_optional" marker) -- see
-- internal/storage/migrate.go for how that distinction is enforced.

CREATE TABLE IF NOT EXISTS geoip_cache (
	ip            inet PRIMARY KEY,
	latitude      double precision,
	longitude     double precision,
	city          text,
	country       text,
	looked_up_at  timestamptz NOT NULL,
	lookup_failed boolean NOT NULL DEFAULT false
);
