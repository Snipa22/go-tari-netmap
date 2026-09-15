package storage

import (
	"context"
	"testing"
	"time"
)

// TestUpsertAndGetGeoIPCache covers the basic write-then-read round trip:
// UpsertGeoIPCache writes a row, GetGeoIPCache reads it back keyed by
// plain IP (not CIDR-suffixed, despite the underlying column being
// inet), and an IP with no cached row at all is simply absent from the
// result map rather than a zero-value entry.
func TestUpsertAndGetGeoIPCache(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Second)
	entry := GeoIPEntry{
		IP:         "203.0.113.5",
		Latitude:   51.5074,
		Longitude:  -0.1278,
		City:       "London",
		Country:    "United Kingdom",
		LookedUpAt: now,
	}

	if err := store.UpsertGeoIPCache(ctx, []GeoIPEntry{entry}); err != nil {
		t.Fatalf("upsert geoip cache: %v", err)
	}

	got, err := store.GetGeoIPCache(ctx, []string{"203.0.113.5", "198.51.100.1"})
	if err != nil {
		t.Fatalf("get geoip cache: %v", err)
	}

	e, ok := got["203.0.113.5"]
	if !ok {
		t.Fatalf("expected an entry for 203.0.113.5, map = %+v", got)
	}
	if e.Latitude != entry.Latitude || e.Longitude != entry.Longitude {
		t.Errorf("lat/lon = %v/%v, want %v/%v", e.Latitude, e.Longitude, entry.Latitude, entry.Longitude)
	}
	if e.City != entry.City || e.Country != entry.Country {
		t.Errorf("city/country = %q/%q, want %q/%q", e.City, e.Country, entry.City, entry.Country)
	}
	if e.LookupFailed {
		t.Errorf("LookupFailed = true, want false")
	}
	if !e.LookedUpAt.Equal(now) {
		t.Errorf("LookedUpAt = %v, want %v", e.LookedUpAt, now)
	}

	if _, ok := got["198.51.100.1"]; ok {
		t.Errorf("expected 198.51.100.1 to be absent (never cached), got an entry")
	}
	if len(got) != 1 {
		t.Errorf("got %d entries, want exactly 1", len(got))
	}
}

// TestUpsertGeoIPCacheOverwritesOnConflict verifies a second
// UpsertGeoIPCache call for the same IP fully replaces the previous row
// (no merge semantics -- see the Store interface's doc comment), e.g. a
// stale successful lookup being overwritten by a later failed one.
func TestUpsertGeoIPCacheOverwritesOnConflict(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	first := GeoIPEntry{IP: "203.0.113.9", Latitude: 1, Longitude: 2, City: "A", Country: "B", LookedUpAt: time.Now().Add(-time.Hour)}
	if err := store.UpsertGeoIPCache(ctx, []GeoIPEntry{first}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	second := GeoIPEntry{IP: "203.0.113.9", LookedUpAt: time.Now(), LookupFailed: true}
	if err := store.UpsertGeoIPCache(ctx, []GeoIPEntry{second}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, err := store.GetGeoIPCache(ctx, []string{"203.0.113.9"})
	if err != nil {
		t.Fatalf("get geoip cache: %v", err)
	}
	e, ok := got["203.0.113.9"]
	if !ok {
		t.Fatalf("expected an entry for 203.0.113.9")
	}
	if !e.LookupFailed {
		t.Errorf("LookupFailed = false, want true (second upsert should have fully replaced the first)")
	}
	if e.City != "" {
		t.Errorf("City = %q, want empty (fully replaced, not merged)", e.City)
	}
}

// TestGetGeoIPCacheEmptyInput confirms an empty ips slice returns an
// empty, non-nil map without erroring or querying.
func TestGetGeoIPCacheEmptyInput(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	got, err := store.GetGeoIPCache(ctx, nil)
	if err != nil {
		t.Fatalf("get geoip cache (empty input): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d entries for empty input, want 0", len(got))
	}
}

// TestUpsertGeoIPCacheRequiresIP confirms an entry with an empty IP is
// rejected rather than silently written as a garbage row.
func TestUpsertGeoIPCacheRequiresIP(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.UpsertGeoIPCache(ctx, []GeoIPEntry{{LookedUpAt: time.Now()}})
	if err == nil {
		t.Fatal("expected an error for an entry with an empty IP, got nil")
	}
}
