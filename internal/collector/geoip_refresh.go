package collector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/netaddr"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// RefreshGeoIP opportunistically tops up storage's geoip_cache table for
// the /map feature's population — every node with a non-empty
// tags["owner"] (HasOwnerTag) and at least one known IPv4 address (see
// BRIEF.md's Scope section 1) — so that GET /nodes/map's HTTP handler
// (internal/api's handleNodesMap) never needs to perform a live outbound
// geoip lookup itself: an HTTP GET must never block on an outbound
// network call, so this loop does that work ahead of time on its own
// independent cadence (see runGeoIPRefreshLoop/GeoIPRefreshTickInterval),
// exactly like DiscoverOwned does for peer-walk data.
//
// It is a cheap no-op — not even a Storage query — when GeoIPClient is
// nil (the /map feature not opted into at all); it returns an error if
// Storage is unset but GeoIPClient isn't (a genuine misconfiguration,
// unlike the deliberate nil-disables convention for GeoIPClient itself).
//
// Read-through + TTL policy: only IPs staleGeoIPs reports as due (never
// cached at all, or past GeoIPSuccessTTL/GeoIPFailedTTL — see its doc
// comment) are ever sent to GeoIPClient.Lookup; a warm cache costs zero
// outbound requests on most ticks. Every IP passed to Lookup gets a
// geoip_cache row written back regardless of outcome — including one
// GeoIPClient.Lookup itself couldn't get a result for at all (a
// transport/decode error for that IP's batch, see Client.Lookup's doc
// comment) — cached as LookupFailed so a persistently-unresolvable IP
// gets GeoIPFailedTTL's shorter backoff instead of being retried on
// every single refresh tick forever.
func (c *Collector) RefreshGeoIP(ctx context.Context) error {
	if c.GeoIPClient == nil {
		return nil
	}
	if c.Storage == nil {
		return errors.New("collector: Storage is not configured")
	}

	ips, err := c.ownerIPv4Addresses(ctx)
	if err != nil {
		return fmt.Errorf("collector: refresh geoip: %w", err)
	}
	if len(ips) == 0 {
		return nil
	}

	cached, err := c.Storage.GetGeoIPCache(ctx, ips)
	if err != nil {
		return fmt.Errorf("collector: refresh geoip: get cache: %w", err)
	}

	stale := staleGeoIPs(ips, cached, time.Now())
	if len(stale) == 0 {
		return nil
	}

	results, err := c.GeoIPClient.Lookup(ctx, stale)
	if err != nil {
		// Lookup already logs/returns per-batch errors internally and
		// still returns whatever it DID resolve — see its doc comment
		// — so this is logged, not fatal: entries below still get
		// written for whatever came back, and any IP with no result at
		// all is still cached as failed (see the missing-key handling
		// below), rather than this whole pass aborting.
		log.Printf("collector: geoip refresh: lookup error (continuing with partial results): %v", err)
	}

	now := time.Now()
	entries := make([]storage.GeoIPEntry, 0, len(stale))
	for _, ip := range stale {
		res, ok := results[ip]
		if !ok {
			entries = append(entries, storage.GeoIPEntry{IP: ip, LookedUpAt: now, LookupFailed: true})
			continue
		}
		entries = append(entries, storage.GeoIPEntry{
			IP:           ip,
			Latitude:     res.Latitude,
			Longitude:    res.Longitude,
			City:         res.City,
			Country:      res.Country,
			LookedUpAt:   now,
			LookupFailed: res.Failed,
		})
	}

	if err := c.Storage.UpsertGeoIPCache(ctx, entries); err != nil {
		return fmt.Errorf("collector: refresh geoip: upsert cache: %w", err)
	}
	log.Printf("collector: geoip refresh: looked up %d/%d candidate ip(s)", len(stale), len(ips))
	return nil
}

// ownerIPv4Addresses returns the deduped set of IPv4 addresses belonging
// to the /map population: every node with HasOwnerTag true, restricted
// to their known IPv4 addresses only (via netaddr.IPv4Host — the same
// classification internal/api's isMapEligible/firstIPv4 use for the
// identical predicate; deliberately re-implemented here rather than
// imported, since internal/api already imports internal/collector, so
// internal/collector cannot import internal/api back — see
// internal/netaddr's own doc comment for this established constraint).
func (c *Collector) ownerIPv4Addresses(ctx context.Context) ([]string, error) {
	nodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		return nil, err
	}

	var owned []storage.Node
	nodeIDs := make([]uuid.UUID, 0, len(nodes))
	for _, n := range nodes {
		if HasOwnerTag(n) {
			owned = append(owned, n)
			nodeIDs = append(nodeIDs, n.ID)
		}
	}
	if len(owned) == 0 {
		return nil, nil
	}

	addrsByNode, err := c.Storage.ListNodeAddressesForNodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var ips []string
	addIfIPv4 := func(raw string) {
		ip, ok := netaddr.IPv4Host(raw)
		if !ok || seen[ip] {
			return
		}
		seen[ip] = true
		ips = append(ips, ip)
	}

	for _, n := range owned {
		addrs := addrsByNode[n.ID]
		if len(addrs) == 0 {
			// Mirrors internal/api/privacy.go's addressStrings fallback
			// for legacy rows with no node_addresses rows recorded yet.
			if n.Address != "" {
				addIfIPv4(n.Address)
			}
			continue
		}
		for _, a := range addrs {
			addIfIPv4(a.Address)
		}
	}
	return ips, nil
}

// staleGeoIPs returns the subset of ips that need a fresh geoip lookup
// given already-cached entries (keyed by ip, e.g. from
// storage.Store.GetGeoIPCache) and now — BRIEF.md's exact TTL policy:
// an ip with no cache entry at all always needs a lookup; a successful
// (LookupFailed == false) entry is stale once GeoIPSuccessTTL has
// elapsed since LookedUpAt; a failed entry uses the much shorter
// GeoIPFailedTTL instead, so a transient rate-limit reject doesn't get
// treated as a month-long "known unresolvable" verdict.
//
// Pure function, no I/O of any kind — unit-testable without a real
// cache store, HTTP client, or network access.
func staleGeoIPs(ips []string, cached map[string]storage.GeoIPEntry, now time.Time) []string {
	var stale []string
	for _, ip := range ips {
		entry, ok := cached[ip]
		if !ok {
			stale = append(stale, ip)
			continue
		}
		ttl := GeoIPSuccessTTL
		if entry.LookupFailed {
			ttl = GeoIPFailedTTL
		}
		if now.Sub(entry.LookedUpAt) >= ttl {
			stale = append(stale, ip)
		}
	}
	return stale
}
