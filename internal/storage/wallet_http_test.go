package storage

import (
	"context"
	"testing"
	"time"
)

// intPtr is a small helper for building *int literals inline in test table data, mirroring
// strPtr (pending_submissions_test.go) for the int case this feature needs.
func intPtr(n int) *int { return &n }

// TestWalletHTTPPortMigrationColumnsExist is this feature's migration test: it proves the
// 0013_wallet_http_port.sql migration actually ran (newTestStore calls store.Migrate) and
// that nodes.wallet_http_port is usable end-to-end, by round-tripping a real value through
// it -- a lighter-weight, more meaningful check than just querying information_schema, since
// it also exercises scanNode's updated column list.
//
// This no longer also exercises pending_submissions.wallet_http_port -- that column was
// part of this feature's FIRST DRAFT (bundling wallet_http_port onto the base-node
// submission flow) and was dropped by 0015_drop_pending_submissions_wallet_http_port.sql
// when this feature was reworked into the separate pending_wallet_submissions flow (see
// storage.PendingWalletSubmission and pending_wallet_submissions_test.go's own migration
// coverage for THAT table's columns instead).
func TestWalletHTTPPortMigrationColumnsExist(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if node.WalletHTTPPort != nil {
		t.Fatalf("WalletHTTPPort = %v, want nil for a freshly-created node", node.WalletHTTPPort)
	}

	if err := store.SetNodeWalletHTTPPort(ctx, node.ID, intPtr(9000)); err != nil {
		t.Fatalf("set node wallet_http_port: %v", err)
	}
	got, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.WalletHTTPPort == nil || *got.WalletHTTPPort != 9000 {
		t.Errorf("nodes.wallet_http_port = %v, want 9000", got.WalletHTTPPort)
	}
}

// TestRecordHealthCheckWalletHTTPProbeSource proves 0013's widening of node_health's
// probe_source CHECK constraint (originally 'grpc'/'p2p' only, see 0003_probe_source.sql) to
// also accept 'wallet_http' actually took effect -- a plain INSERT with the old constraint
// still in place would fail with a check-violation error here.
func TestRecordHealthCheckWalletHTTPProbeSource(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	height := int64(12345)
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID:      node.ID,
		Reachable:   true,
		ProbeSource: ProbeSourceWalletHTTP,
		Height:      &height,
	}); err != nil {
		t.Fatalf("record health check with probe_source=wallet_http: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get node history: %v", err)
	}
	if len(history) != 1 || history[0].ProbeSource != ProbeSourceWalletHTTP {
		t.Fatalf("history = %+v, want one row with probe_source=wallet_http", history)
	}
}

// TestSetNodeWalletHTTPPortClearsWithNil verifies passing a nil port clears a
// previously-set value.
func TestSetNodeWalletHTTPPortClearsWithNil(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if err := store.SetNodeWalletHTTPPort(ctx, node.ID, intPtr(9000)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.SetNodeWalletHTTPPort(ctx, node.ID, nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	got, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.WalletHTTPPort != nil {
		t.Errorf("WalletHTTPPort = %v, want nil after clearing", got.WalletHTTPPort)
	}
}

// TestSetNodeWalletHTTPPortByAddress verifies the address-keyed sibling used by
// internal/collector's owned-node reconciliation: found=true and the port is applied when a
// node exists for that address, found=false (no error) when no node exists yet.
func TestSetNodeWalletHTTPPortByAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	found, err := store.SetNodeWalletHTTPPortByAddress(ctx, "1.2.3.4:18142", 9000)
	if err != nil {
		t.Fatalf("set by address: %v", err)
	}
	if !found {
		t.Fatalf("found = false, want true (node exists for this address)")
	}
	got, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.WalletHTTPPort == nil || *got.WalletHTTPPort != 9000 {
		t.Errorf("WalletHTTPPort = %v, want 9000", got.WalletHTTPPort)
	}

	found, err = store.SetNodeWalletHTTPPortByAddress(ctx, "9.9.9.9:18142", 9000)
	if err != nil {
		t.Fatalf("set by address (unknown): %v", err)
	}
	if found {
		t.Fatalf("found = true, want false (no node exists for this address yet)")
	}
}

// TestListNodesHasWalletHTTPPortFilter verifies NodeFilter.HasWalletHTTPPort narrows
// correctly both ways, and that a zero-value NodeFilter (nil HasWalletHTTPPort) is
// unaffected -- same "opt-in filtering" convention as every other NodeFilter field.
func TestListNodesHasWalletHTTPPortFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	withPort, err := store.UpsertDiscoveredNode(ctx, "with-port:1", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert with-port node: %v", err)
	}
	if err := store.SetNodeWalletHTTPPort(ctx, withPort.ID, intPtr(9000)); err != nil {
		t.Fatalf("set wallet_http_port: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "without-port:1", DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert without-port node: %v", err)
	}

	yes := true
	withFilter, err := store.ListNodes(ctx, NodeFilter{HasWalletHTTPPort: &yes})
	if err != nil {
		t.Fatalf("list with filter true: %v", err)
	}
	if len(withFilter) != 1 || withFilter[0].ID != withPort.ID {
		t.Fatalf("ListNodes(HasWalletHTTPPort: true) = %+v, want just %v", withFilter, withPort.ID)
	}

	no := false
	withoutFilter, err := store.ListNodes(ctx, NodeFilter{HasWalletHTTPPort: &no})
	if err != nil {
		t.Fatalf("list with filter false: %v", err)
	}
	if len(withoutFilter) != 1 || withoutFilter[0].ID == withPort.ID {
		t.Fatalf("ListNodes(HasWalletHTTPPort: false) = %+v, want just the without-port node", withoutFilter)
	}

	all, err := store.ListNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("list unfiltered: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListNodes({}) = %d nodes, want 2 (a zero-value filter must be unaffected)", len(all))
	}
}

// TestWalletHTTPUptime verifies the fraction/sample-count computation and the "zero rows ->
// nil" fallback, and that rows outside the window / from a different probe_source are
// excluded.
func TestWalletHTTPUptime(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	// No rows yet -> nil.
	uptime, count, err := store.WalletHTTPUptime(ctx, node.ID, 24*time.Hour)
	if err != nil {
		t.Fatalf("uptime (no rows): %v", err)
	}
	if uptime != nil || count != 0 {
		t.Fatalf("uptime = %v, count = %d, want (nil, 0) with zero rows", uptime, count)
	}

	// 3 wallet_http rows: 2 reachable, 1 not -> 2/3.
	for _, reachable := range []bool{true, true, false} {
		if err := store.RecordHealthCheck(ctx, HealthCheckInput{
			NodeID:      node.ID,
			Reachable:   reachable,
			ProbeSource: ProbeSourceWalletHTTP,
		}); err != nil {
			t.Fatalf("record wallet_http health check: %v", err)
		}
	}
	// A grpc-probe-source row for the SAME node must NOT count toward this computation.
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID:      node.ID,
		Reachable:   false,
		ProbeSource: ProbeSourceGRPC,
	}); err != nil {
		t.Fatalf("record grpc health check: %v", err)
	}

	uptime, count, err = store.WalletHTTPUptime(ctx, node.ID, 24*time.Hour)
	if err != nil {
		t.Fatalf("uptime: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3 (the grpc-probe-source row must be excluded)", count)
	}
	if uptime == nil || *uptime < 0.666 || *uptime > 0.667 {
		t.Fatalf("uptime = %v, want ~0.6667 (2/3)", uptime)
	}

	// A window that excludes all rows (they were all just inserted "now") -> nil.
	uptime, count, err = store.WalletHTTPUptime(ctx, node.ID, -1*time.Hour)
	if err != nil {
		t.Fatalf("uptime (excluding window): %v", err)
	}
	if uptime != nil || count != 0 {
		t.Fatalf("uptime = %v, count = %d, want (nil, 0) when the window excludes every row", uptime, count)
	}
}

// TestGetLatestHealthCheckScopedToProbeSource verifies GetLatestHealthCheck returns the
// most recent row for the REQUESTED probe source specifically, not just the most recent row
// of any kind -- the whole reason this method exists rather than reusing GetNodeHistory (see
// its doc comment on the Store interface).
func TestGetLatestHealthCheckScopedToProbeSource(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	walletHeight := int64(111)
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID: node.ID, Reachable: true, ProbeSource: ProbeSourceWalletHTTP, Height: &walletHeight,
	}); err != nil {
		t.Fatalf("record wallet_http check: %v", err)
	}

	// A LATER grpc row must not shadow the wallet_http result when querying specifically
	// for storage.ProbeSourceWalletHTTP.
	time.Sleep(10 * time.Millisecond)
	grpcHeight := int64(222)
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID: node.ID, Reachable: true, ProbeSource: ProbeSourceGRPC, Height: &grpcHeight,
	}); err != nil {
		t.Fatalf("record grpc check: %v", err)
	}

	got, err := store.GetLatestHealthCheck(ctx, node.ID, ProbeSourceWalletHTTP)
	if err != nil {
		t.Fatalf("get latest health check: %v", err)
	}
	if got == nil {
		t.Fatal("got = nil, want the wallet_http row")
	}
	if got.ProbeSource != ProbeSourceWalletHTTP || got.Height == nil || *got.Height != walletHeight {
		t.Errorf("got = %+v, want probe_source=wallet_http height=%d", got, walletHeight)
	}

	// A probe source with zero rows returns (nil, nil), not an error.
	none, err := store.GetLatestHealthCheck(ctx, node.ID, ProbeSourceP2P)
	if err != nil {
		t.Fatalf("get latest health check (p2p, none recorded): %v", err)
	}
	if none != nil {
		t.Errorf("got = %+v, want nil (no p2p rows recorded)", none)
	}
}
