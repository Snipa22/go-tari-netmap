package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// defaultTestDSN matches this sandbox's real Postgres 17 instance: unix
// socket at /workspace/pg-embed/sockets, port 5433, database "netmap",
// user "postgres", trust auth (no password).
const defaultTestDSN = "postgres://postgres@localhost:5433/netmap?sslmode=disable&host=/workspace/pg-embed/sockets"

func testDSN() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return defaultTestDSN
}

// acquireTestDBLock takes a session-level Postgres advisory lock shared by
// every test package that exercises the real test database (storage, api,
// collector). `go test ./...` runs each package's tests as a separate
// process, potentially in parallel, but they all point at the same shared
// Postgres instance and truncate its tables — without this lock, two
// packages' tests running concurrently stomp on each other's data. The
// returned func releases the lock and must be deferred/registered as a
// cleanup that runs after the test (and its Store) are done with the DB.
func acquireTestDBLock(t *testing.T, ctx context.Context, dsn string) func() {
	t.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: cannot reach test database: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('go-tari-netmap-tests')::bigint)"); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("acquire test db advisory lock: %v", err)
	}

	return func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(hashtext('go-tari-netmap-tests')::bigint)")
		_ = conn.Close(unlockCtx)
	}
}

// newTestStore connects to the configured test database, runs migrations,
// and truncates all tables so each test starts from a clean slate. It
// skips the test (t.Skip) if the database can't be reached, so
// `go test ./...` doesn't hard-fail in environments with no test DB
// configured. It holds a cross-process advisory lock for the duration of
// the test to serialize access with the api and collector packages' tests
// against the same shared database.
func newTestStore(t *testing.T) Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dsn := testDSN()

	// Registered before store.Close below, so it runs last (t.Cleanup is
	// LIFO): the lock stays held until the Store is fully closed.
	t.Cleanup(acquireTestDBLock(t, ctx, dsn))

	store, err := New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: cannot reach test database: %v", err)
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ps := store.(*pgStore)
	if _, err := ps.pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edge_observations, node_addresses, pending_submissions, nodes CASCADE"); err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	return store
}

func TestUpsertDiscoveredNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	n, err := store.UpsertDiscoveredNode(ctx, "127.0.0.1:18142", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if n.Address != "127.0.0.1:18142" {
		t.Errorf("address = %q, want %q", n.Address, "127.0.0.1:18142")
	}
	if n.PublicKey != nil {
		t.Errorf("expected PublicKey to be nil for a discovered (placeholder) node, got %x", n.PublicKey)
	}
	if n.DiscoverySource != DiscoverySourceP2P {
		t.Errorf("discovery_source = %q, want %q", n.DiscoverySource, DiscoverySourceP2P)
	}
	if n.FirstSeen.IsZero() || n.LastSeen.IsZero() {
		t.Errorf("expected first_seen/last_seen to be set")
	}

	firstSeen := n.FirstSeen

	// Upsert again with a different source and a pool-owned tag: the
	// discovery_source should merge to "both", first_seen should be
	// preserved, last_seen should be bumped, and the tag should stick.
	time.Sleep(10 * time.Millisecond)
	n2, err := store.UpsertDiscoveredNode(ctx, "127.0.0.1:18142", DiscoverySourceRegistry, map[string]any{"pool_owned": true}, nil)
	if err != nil {
		t.Fatalf("upsert node again: %v", err)
	}
	if n2.ID != n.ID {
		t.Errorf("expected same node ID on re-upsert by address")
	}
	if n2.DiscoverySource != DiscoverySourceBoth {
		t.Errorf("discovery_source = %q, want %q", n2.DiscoverySource, DiscoverySourceBoth)
	}
	if !n2.FirstSeen.Equal(firstSeen) {
		t.Errorf("first_seen changed on re-upsert: got %v, want %v", n2.FirstSeen, firstSeen)
	}
	if !n2.LastSeen.After(n.LastSeen) {
		t.Errorf("expected last_seen to be bumped")
	}
	if poolOwned, _ := n2.Tags["pool_owned"].(bool); !poolOwned {
		t.Errorf("expected pool_owned tag to be set, got %v", n2.Tags)
	}

	addrs, err := store.ListNodeAddresses(ctx, n.ID)
	if err != nil {
		t.Fatalf("list node addresses: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Address != "127.0.0.1:18142" {
		t.Fatalf("addrs = %+v, want just 127.0.0.1:18142", addrs)
	}
}

func TestListNodesFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	all, err := store.ListNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all) = %d, want 2", len(all))
	}

	p2p, err := store.ListNodes(ctx, NodeFilter{DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("list p2p: %v", err)
	}
	if len(p2p) != 1 || p2p[0].Address != "a:1" {
		t.Fatalf("p2p filter = %+v, want just a:1", p2p)
	}
}

// TestListNodesReachableSinceFilter exercises NodeFilter.ReachableSince:
// only nodes with at least one reachable=true node_health row at or
// after the cutoff should come back. It uses a short 1-hour window
// (rather than the dashboard's real 24h) to keep the test fast: n1 has a
// reachable check 30 minutes ago (within a 1h cutoff) and must be
// included; n2 has a reachable check 2 hours ago (older than the
// cutoff, so effectively stale) and must be excluded, even though it
// has been "seen" in the general sense. It also explicitly proves the
// critical invariant that a zero-value NodeFilter{} (and a
// NodeFilter{Limit: N} with ReachableSince left nil) are byte-for-byte
// unaffected by this field's mere existence — old behavior must not
// change at all when ReachableSince isn't set.
func TestListNodesReachableSinceFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	n1, err := store.UpsertDiscoveredNode(ctx, "reach1:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert n1: %v", err)
	}
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_health (node_id, ts, reachable, probe_source)
		VALUES ($1, now() - interval '30 minutes', true, 'grpc')
	`, n1.ID); err != nil {
		t.Fatalf("insert recent reachable health n1: %v", err)
	}

	n2, err := store.UpsertDiscoveredNode(ctx, "reach2:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert n2: %v", err)
	}
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_health (node_id, ts, reachable, probe_source)
		VALUES ($1, now() - interval '2 hours', true, 'grpc')
	`, n2.ID); err != nil {
		t.Fatalf("insert stale reachable health n2: %v", err)
	}

	cutoff := time.Now().Add(-1 * time.Hour)
	got, err := store.ListNodes(ctx, NodeFilter{ReachableSince: &cutoff})
	if err != nil {
		t.Fatalf("list reachable-since: %v", err)
	}
	if len(got) != 1 || got[0].ID != n1.ID {
		t.Fatalf("ReachableSince filter = %+v, want just n1 (%s)", got, n1.ID)
	}

	// Critical invariant: a zero-value filter (ReachableSince left nil)
	// must return everything, completely unaffected by the new field's
	// existence — this is what the collector's Poll loop relies on.
	all, err := store.ListNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("list all (zero-value filter): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all) = %d, want 2 (zero-value NodeFilter{} must be unaffected by ReachableSince)", len(all))
	}

	// Same invariant, but with Limit set alone (ReachableSince still
	// nil): must behave exactly as before this field was added.
	limited, err := store.ListNodes(ctx, NodeFilter{Limit: 1})
	if err != nil {
		t.Fatalf("list limit=1 (no ReachableSince): %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("len(limited) = %d, want 1 (NodeFilter{Limit: 1} alone must be unaffected by ReachableSince)", len(limited))
	}
}

// TestListNodesPagination verifies that NodeFilter.Limit/Offset apply
// real SQL-level pagination (a correct page, in the same address-sorted
// order ListNodes always uses), and that a zero-value filter still
// returns everything unpaginated.
func TestListNodesPagination(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Addresses are named so that address-sort order (ListNodes' ORDER
	// BY address) is n1..n5.
	want := []string{"n1:1", "n2:1", "n3:1", "n4:1", "n5:1"}
	for _, addr := range want {
		if _, err := store.UpsertDiscoveredNode(ctx, addr, DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
	}

	// Zero-value filter: unpaginated, all 5.
	all, err := store.ListNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("len(all) = %d, want 5", len(all))
	}

	// Page size 2, page 2 (offset 2) should be n3, n4.
	page2, err := store.ListNodes(ctx, NodeFilter{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("len(page2) = %d, want 2", len(page2))
	}
	if page2[0].Address != "n3:1" || page2[1].Address != "n4:1" {
		t.Fatalf("page2 = [%s, %s], want [n3:1, n4:1]", page2[0].Address, page2[1].Address)
	}

	// Last page (page 3, offset 4) should be just n5 (partial page).
	page3, err := store.ListNodes(ctx, NodeFilter{Limit: 2, Offset: 4})
	if err != nil {
		t.Fatalf("list page 3: %v", err)
	}
	if len(page3) != 1 || page3[0].Address != "n5:1" {
		t.Fatalf("page3 = %+v, want just n5:1", page3)
	}

	// Offset past the end returns an empty page, not an error.
	pastEnd, err := store.ListNodes(ctx, NodeFilter{Limit: 2, Offset: 10})
	if err != nil {
		t.Fatalf("list past end: %v", err)
	}
	if len(pastEnd) != 0 {
		t.Fatalf("len(pastEnd) = %d, want 0", len(pastEnd))
	}
}

// TestCountNodes verifies CountNodes returns the correct total, ignoring
// Limit/Offset entirely, and respects DiscoverySource filtering the same
// way ListNodes does.
func TestCountNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "c:3", DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert c: %v", err)
	}

	total, err := store.CountNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("count all: %v", err)
	}
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}

	// Limit/Offset must be ignored by CountNodes -- passing them
	// shouldn't change the total.
	totalWithLimitOffset, err := store.CountNodes(ctx, NodeFilter{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("count all with limit/offset: %v", err)
	}
	if totalWithLimitOffset != 3 {
		t.Fatalf("totalWithLimitOffset = %d, want 3 (Limit/Offset must be ignored)", totalWithLimitOffset)
	}

	p2pCount, err := store.CountNodes(ctx, NodeFilter{DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("count p2p: %v", err)
	}
	if p2pCount != 2 {
		t.Fatalf("p2pCount = %d, want 2", p2pCount)
	}
}

// TestListNodeAddressesForNodes verifies the batch address lookup
// returns correct per-node addresses for a set of node IDs, including a
// node with zero addresses getting an empty-slice map entry (not a
// missing key, not an error), and that an empty input returns an empty
// map without erroring.
func TestListNodeAddressesForNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	withAddr, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	pubkey := []byte("multi-addr-pubkey")
	multiAddr, err := store.UpsertConfirmedNode(ctx, "b:1", pubkey, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed b: %v", err)
	}
	if _, err := store.UpsertConfirmedNode(ctx, "b:2", pubkey, DiscoverySourceP2P); err != nil {
		t.Fatalf("upsert confirmed b2: %v", err)
	}

	// A node row with no node_addresses row at all (bypasses the normal
	// upsert path, which always ensures one) to exercise the "zero
	// addresses" case.
	ps := store.(*pgStore)
	var noAddrID uuid.UUID
	if err := ps.pool.QueryRow(ctx, `
		INSERT INTO nodes (address, discovery_source, tags, first_seen, last_seen)
		VALUES ('noaddr:1', 'p2p_discovered', '{}'::jsonb, now(), now())
		RETURNING id
	`).Scan(&noAddrID); err != nil {
		t.Fatalf("insert no-address node: %v", err)
	}
	if _, err := ps.pool.Exec(ctx, `DELETE FROM node_addresses WHERE node_id = $1`, noAddrID); err != nil {
		t.Fatalf("delete node_addresses: %v", err)
	}

	got, err := store.ListNodeAddressesForNodes(ctx, []uuid.UUID{withAddr.ID, multiAddr.ID, noAddrID})
	if err != nil {
		t.Fatalf("list node addresses for nodes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got) = %d, want 3 entries (one per input id)", len(got))
	}

	if addrs, ok := got[withAddr.ID]; !ok || len(addrs) != 1 || addrs[0].Address != "a:1" {
		t.Fatalf("got[withAddr.ID] = %+v (ok=%v), want just a:1", addrs, ok)
	}

	if addrs, ok := got[multiAddr.ID]; !ok || len(addrs) != 2 {
		t.Fatalf("got[multiAddr.ID] = %+v (ok=%v), want 2 addresses (b:1, b:2)", addrs, ok)
	}

	addrs, ok := got[noAddrID]
	if !ok {
		t.Fatalf("got[noAddrID] missing key, want present with an empty slice")
	}
	if addrs == nil {
		t.Fatalf("got[noAddrID] = nil, want a non-nil empty slice")
	}
	if len(addrs) != 0 {
		t.Fatalf("got[noAddrID] = %+v, want empty", addrs)
	}

	empty, err := store.ListNodeAddressesForNodes(ctx, nil)
	if err != nil {
		t.Fatalf("list node addresses for nodes (empty input): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("len(empty) = %d, want 0", len(empty))
	}
}

// TestListTopologyMaxNodesCap verifies that ListTopology with a
// TopologyFilter.MaxNodes cap returns a bounded, edge-consistent (no
// dangling edges to nodes outside the returned set) subgraph, keeping
// the top-N by (undirected) degree.
func TestListTopologyMaxNodesCap(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// hub: degree 3 (edges to leaf1, leaf2, leaf3).
	// leaf1: degree 2 (hub, leaf2).
	// leaf2: degree 2 (hub, leaf1).
	// leaf3: degree 1 (hub).
	// isolated: degree 0, no edges at all.
	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	leaf1, err := store.UpsertDiscoveredNode(ctx, "leaf1:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf1: %v", err)
	}
	leaf2, err := store.UpsertDiscoveredNode(ctx, "leaf2:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf2: %v", err)
	}
	leaf3, err := store.UpsertDiscoveredNode(ctx, "leaf3:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf3: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "isolated:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert isolated: %v", err)
	}

	for _, e := range []struct{ from, to uuid.UUID }{
		{hub.ID, leaf1.ID},
		{hub.ID, leaf2.ID},
		{leaf3.ID, hub.ID},
		{leaf1.ID, leaf2.ID},
	} {
		if err := store.RecordPeerEdgeObservation(ctx, e.from, e.to); err != nil {
			t.Fatalf("record edge observation %v -> %v: %v", e.from, e.to, err)
		}
	}

	// MaxNodes=3: the top 3 by degree must be hub (3), then leaf1 and
	// leaf2 (2 each) -- leaf3 (1) and isolated (0) must NOT be included.
	nodes, edges, err := store.ListTopology(ctx, TopologyFilter{MaxNodes: 3})
	if err != nil {
		t.Fatalf("list topology (capped): %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("len(nodes) = %d, want 3", len(nodes))
	}
	gotIDs := map[uuid.UUID]bool{}
	for _, n := range nodes {
		gotIDs[n.ID] = true
	}
	if !gotIDs[hub.ID] || !gotIDs[leaf1.ID] || !gotIDs[leaf2.ID] {
		t.Fatalf("nodes = %+v, want hub+leaf1+leaf2", nodes)
	}
	if gotIDs[leaf3.ID] {
		t.Errorf("nodes contains leaf3 (degree 1), want excluded in favor of higher-degree nodes")
	}

	// Edge consistency: every edge's endpoints must both be in the
	// returned node set. leaf3's edge to hub must be excluded since
	// leaf3 itself isn't in the capped set (no dangling edges).
	for _, e := range edges {
		if !gotIDs[e.FromNodeID] || !gotIDs[e.ToNodeID] {
			t.Errorf("edge %+v has an endpoint outside the returned node set %v", e, gotIDs)
		}
	}
	// Expect exactly 3 edges among {hub, leaf1, leaf2}: hub->leaf1,
	// hub->leaf2, leaf1->leaf2.
	if len(edges) != 3 {
		t.Fatalf("len(edges) = %d, want 3 (edges among hub/leaf1/leaf2 only)", len(edges))
	}

	// MaxNodes larger than the whole population: same node/edge counts
	// as the unbounded call.
	uncappedNodes, uncappedEdges, err := store.ListTopology(ctx, TopologyFilter{})
	if err != nil {
		t.Fatalf("list topology (uncapped): %v", err)
	}
	bigCapNodes, bigCapEdges, err := store.ListTopology(ctx, TopologyFilter{MaxNodes: 100})
	if err != nil {
		t.Fatalf("list topology (large cap): %v", err)
	}
	if len(bigCapNodes) != len(uncappedNodes) {
		t.Fatalf("len(bigCapNodes) = %d, want %d (same as uncapped)", len(bigCapNodes), len(uncappedNodes))
	}
	if len(bigCapEdges) != len(uncappedEdges) {
		t.Fatalf("len(bigCapEdges) = %d, want %d (same as uncapped)", len(bigCapEdges), len(uncappedEdges))
	}
}

// TestListTopologyMaxEdgesCap verifies that ListTopology with
// TopologyFilter.MaxEdges set (independent of MaxNodes) caps the number
// of returned edges, even when MaxNodes is large enough to admit the
// full node set and more real edges exist between those nodes than the
// MaxEdges cap.
func TestListTopologyMaxEdgesCap(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// A small clique: a, b, c, d all pairwise connected -- 6 undirected
	// edges recorded as 6 directed observations, more than the MaxEdges
	// cap used below.
	var ids []uuid.UUID
	for _, addr := range []string{"clique-a:1", "clique-b:1", "clique-c:1", "clique-d:1"} {
		n, err := store.UpsertDiscoveredNode(ctx, addr, DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		ids = append(ids, n.ID)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if err := store.RecordPeerEdgeObservation(ctx, ids[i], ids[j]); err != nil {
				t.Fatalf("record edge observation %v -> %v: %v", ids[i], ids[j], err)
			}
		}
	}

	// Sanity check: uncapped, there really are 6 edges among these 4
	// nodes (so a MaxEdges=3 cap below is actually truncating something).
	_, uncappedEdges, err := store.ListTopology(ctx, TopologyFilter{})
	if err != nil {
		t.Fatalf("list topology (uncapped): %v", err)
	}
	if len(uncappedEdges) != 6 {
		t.Fatalf("len(uncappedEdges) = %d, want 6 (sanity check on seeded clique)", len(uncappedEdges))
	}

	// MaxNodes is large enough to include all 4 clique nodes untouched;
	// MaxEdges=3 is smaller than the real edge count (6) between them.
	nodes, edges, err := store.ListTopology(ctx, TopologyFilter{MaxNodes: 100, MaxEdges: 3})
	if err != nil {
		t.Fatalf("list topology (edge-capped): %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("len(nodes) = %d, want 4 (MaxNodes=100 admits all of them)", len(nodes))
	}
	if len(edges) > 3 {
		t.Fatalf("len(edges) = %d, want <= 3 (MaxEdges cap)", len(edges))
	}
	if len(edges) == 0 {
		t.Fatalf("len(edges) = 0, want > 0 (some edges should still be returned)")
	}
}

func TestGetNodeNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetNode(ctx, uuid.New())
	if err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRecordHealthCheckAndHistory(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	n, err := store.UpsertDiscoveredNode(ctx, "node:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	height := int64(100)
	latency := 42
	for i := 0; i < 3; i++ {
		h := height + int64(i)
		if err := store.RecordHealthCheck(ctx, HealthCheckInput{
			NodeID:      n.ID,
			Reachable:   true,
			ProbeSource: ProbeSourceGRPC,
			Height:      &h,
			LatencyMS:   &latency,
		}); err != nil {
			t.Fatalf("record health check %d: %v", i, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	history, err := store.GetNodeHistory(ctx, n.ID, 2)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	// Newest first.
	if *history[0].Height != height+2 {
		t.Errorf("history[0].Height = %d, want %d", *history[0].Height, height+2)
	}
	if *history[1].Height != height+1 {
		t.Errorf("history[1].Height = %d, want %d", *history[1].Height, height+1)
	}
	if history[0].ProbeSource != ProbeSourceGRPC {
		t.Errorf("history[0].ProbeSource = %q, want %q", history[0].ProbeSource, ProbeSourceGRPC)
	}
}

// TestRecordHealthCheckProbeSourceRoundTrip verifies that both
// ProbeSourceGRPC and ProbeSourceP2P round-trip correctly through
// RecordHealthCheck + GetNodeHistory, and that a node can accrue history
// rows from both probe sources independently.
func TestRecordHealthCheckProbeSourceRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	n, err := store.UpsertDiscoveredNode(ctx, "node:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID:      n.ID,
		Reachable:   true,
		ProbeSource: ProbeSourceGRPC,
	}); err != nil {
		t.Fatalf("record grpc health check: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{
		NodeID:      n.ID,
		Reachable:   true,
		ProbeSource: ProbeSourceP2P,
	}); err != nil {
		t.Fatalf("record p2p health check: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, n.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	// Newest first: p2p was recorded after grpc.
	if history[0].ProbeSource != ProbeSourceP2P {
		t.Errorf("history[0].ProbeSource = %q, want %q", history[0].ProbeSource, ProbeSourceP2P)
	}
	if history[1].ProbeSource != ProbeSourceGRPC {
		t.Errorf("history[1].ProbeSource = %q, want %q", history[1].ProbeSource, ProbeSourceGRPC)
	}
}

func TestRecordHealthCheckRequiresProbeSource(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	n, err := store.UpsertDiscoveredNode(ctx, "node:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	err = store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: n.ID, Reachable: true})
	if err == nil {
		t.Fatal("expected error when ProbeSource is unset, got nil")
	}
}

func TestPeerEdgesAndTopology(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record edge observation: %v", err)
	}
	// Two observations of the same (from, to) pair are NOT deduped at the
	// storage layer anymore — each is its own row in
	// peer_edge_observations (see TestPeerEdgeObservationsAccumulateOverTime
	// for a direct assertion of that). ListTopology's rollup view still
	// shows this pair as a single current-snapshot edge, though.
	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record edge observation again: %v", err)
	}

	nodes, edges, err := store.ListTopology(ctx, TopologyFilter{})
	if err != nil {
		t.Fatalf("list topology: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("len(nodes) = %d, want 2", len(nodes))
	}
	if len(edges) != 1 {
		t.Fatalf("len(edges) = %d, want 1", len(edges))
	}
	if edges[0].FromNodeID != a.ID || edges[0].ToNodeID != b.ID {
		t.Errorf("edge = %+v, want from=%v to=%v", edges[0], a.ID, b.ID)
	}
}

// TestPeerEdgeObservationsAccumulateOverTime is the core proof that the
// old UpsertPeerEdge overwrite behavior is gone: two
// RecordPeerEdgeObservation calls for the same (from, to) pair, separated
// by a real time gap, must produce two distinct rows in
// peer_edge_observations with two distinct, strictly increasing
// observed_at timestamps — not one row with a bumped last_seen.
func TestPeerEdgeObservationsAccumulateOverTime(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record edge observation 1: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record edge observation 2: %v", err)
	}

	ps := store.(*pgStore)
	rows, err := ps.pool.Query(ctx, `
		SELECT observed_at FROM peer_edge_observations
		WHERE from_node_id = $1 AND to_node_id = $2
		ORDER BY observed_at
	`, a.ID, b.ID)
	if err != nil {
		t.Fatalf("query observations: %v", err)
	}
	defer rows.Close()

	var timestamps []time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			t.Fatalf("scan observed_at: %v", err)
		}
		timestamps = append(timestamps, ts)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("query observations: %v", err)
	}

	if len(timestamps) != 2 {
		t.Fatalf("len(timestamps) = %d, want 2", len(timestamps))
	}
	if !timestamps[1].After(timestamps[0]) {
		t.Errorf("timestamps[1] = %v, want strictly after timestamps[0] = %v", timestamps[1], timestamps[0])
	}
}

// TestTopPeeredNodes verifies that TopPeeredNodes ranks nodes by distinct
// peer count (undirected) and returns the correct Degree for the
// highest-connected node.
func TestTopPeeredNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	leaf1, err := store.UpsertDiscoveredNode(ctx, "leaf1:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf1: %v", err)
	}
	leaf2, err := store.UpsertDiscoveredNode(ctx, "leaf2:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf2: %v", err)
	}
	leaf3, err := store.UpsertDiscoveredNode(ctx, "leaf3:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf3: %v", err)
	}

	// hub has edges to/from leaf1, leaf2, leaf3 (degree 3). leaf1 and
	// leaf2 have one edge between them too (degree 2 each, counting the
	// hub edge). leaf3 only connects to hub (degree 1).
	for _, e := range []struct{ from, to uuid.UUID }{
		{hub.ID, leaf1.ID},
		{hub.ID, leaf2.ID},
		{leaf3.ID, hub.ID},
		{leaf1.ID, leaf2.ID},
	} {
		if err := store.RecordPeerEdgeObservation(ctx, e.from, e.to); err != nil {
			t.Fatalf("record edge observation %v -> %v: %v", e.from, e.to, err)
		}
	}

	since := time.Now().Add(-time.Hour)
	top, err := store.TopPeeredNodes(ctx, since, 10)
	if err != nil {
		t.Fatalf("top peered nodes: %v", err)
	}
	if len(top) == 0 {
		t.Fatalf("expected at least 1 result")
	}
	if top[0].NodeID != hub.ID {
		t.Fatalf("top[0].NodeID = %v, want hub %v (top = %+v)", top[0].NodeID, hub.ID, top)
	}
	if top[0].Degree != 3 {
		t.Errorf("top[0].Degree = %d, want 3", top[0].Degree)
	}
	if top[0].Address != "hub:1" {
		t.Errorf("top[0].Address = %q, want %q", top[0].Address, "hub:1")
	}
}

// TestTopPeeredNodesOnionClearnetBreakdown verifies that TopPeeredNodes'
// OnionPeerCount/ClearnetPeerCount fields correctly classify a hub node's
// distinct peers by their OWN known node_addresses: one peer with only an
// onion address, one peer with only a clearnet (IPv4) address, and one
// dual-stack peer with BOTH an onion and a clearnet address recorded (two
// separate node_addresses rows on the same node, inserted directly via
// raw SQL following TestMigrationBackfillsNodeAddresses' established
// pattern for that). The dual-stack peer must count toward BOTH numbers.
func TestTopPeeredNodesOnionClearnetBreakdown(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	const onionAddr = "abcdefghijklmnopqrstuvwxyz234567.onion:18142"
	const clearnetAddr = "203.0.113.5:18142"
	const dualStackPrimaryAddr = "203.0.113.9:18142"
	const dualStackOnionAddr = "qrstuvwxyzabcdefghijklmnop234567.onion:18142"

	hub, err := store.UpsertDiscoveredNode(ctx, "hub2:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	onionOnlyPeer, err := store.UpsertDiscoveredNode(ctx, onionAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert onion-only peer: %v", err)
	}
	clearnetOnlyPeer, err := store.UpsertDiscoveredNode(ctx, clearnetAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert clearnet-only peer: %v", err)
	}
	dualStackPeer, err := store.UpsertDiscoveredNode(ctx, dualStackPrimaryAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert dual-stack peer: %v", err)
	}
	// Give the dual-stack peer a SECOND node_addresses row (its onion
	// address) alongside the clearnet address it was created with above.
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_addresses (node_id, address, first_seen, last_seen)
		VALUES ($1, $2, now(), now())
	`, dualStackPeer.ID, dualStackOnionAddr); err != nil {
		t.Fatalf("insert dual-stack peer's onion address: %v", err)
	}

	for _, e := range []struct{ from, to uuid.UUID }{
		{hub.ID, onionOnlyPeer.ID},
		{hub.ID, clearnetOnlyPeer.ID},
		{hub.ID, dualStackPeer.ID},
	} {
		if err := store.RecordPeerEdgeObservation(ctx, e.from, e.to); err != nil {
			t.Fatalf("record edge observation %v -> %v: %v", e.from, e.to, err)
		}
	}

	since := time.Now().Add(-time.Hour)
	top, err := store.TopPeeredNodes(ctx, since, 10)
	if err != nil {
		t.Fatalf("top peered nodes: %v", err)
	}

	var hubRow *NodeDegree
	for i := range top {
		if top[i].NodeID == hub.ID {
			hubRow = &top[i]
			break
		}
	}
	if hubRow == nil {
		t.Fatalf("hub %v not found in top peered nodes result: %+v", hub.ID, top)
	}

	if hubRow.Degree != 3 {
		t.Fatalf("hub Degree = %d, want 3", hubRow.Degree)
	}
	// onion-only peer + dual-stack peer = 2.
	if hubRow.OnionPeerCount != 2 {
		t.Errorf("hub OnionPeerCount = %d, want 2 (onion-only peer + dual-stack peer)", hubRow.OnionPeerCount)
	}
	// clearnet-only peer + dual-stack peer = 2.
	if hubRow.ClearnetPeerCount != 2 {
		t.Errorf("hub ClearnetPeerCount = %d, want 2 (clearnet-only peer + dual-stack peer)", hubRow.ClearnetPeerCount)
	}

	t.Logf("hub row: degree=%d onion_peer_count=%d clearnet_peer_count=%d (dual-stack peer %v counted in both)",
		hubRow.Degree, hubRow.OnionPeerCount, hubRow.ClearnetPeerCount, dualStackPeer.ID)
}

// TestTopPeeredNodesLiveCounts verifies TopPeeredNodes' Live* fields
// (LiveDegree/LiveInDegree/LiveOutDegree/LiveOnionPeerCount/
// LiveClearnetPeerCount) correctly restrict each corresponding raw total
// to the subset of peers with a confirmed (non-null) public_key, while
// the raw totals keep counting confirmed AND unconfirmed peers alike.
//
// The hub has six distinct peers, three reached via an out-edge
// (hub -> peer) and three via an in-edge (peer -> hub):
//
//   - confirmedOnionPeer:      out-edge, CONFIRMED, onion address only.
//   - unconfirmedOnionPeer:    out-edge, unconfirmed, onion address only.
//   - confirmedDualStackPeer:  out-edge, CONFIRMED, both a clearnet AND
//     an onion address (second node_addresses row inserted directly,
//     same pattern as TestTopPeeredNodesOnionClearnetBreakdown's
//     dual-stack peer).
//   - confirmedClearnetPeer:   in-edge, CONFIRMED, clearnet address only.
//   - unconfirmedClearnetPeer: in-edge, unconfirmed, clearnet address
//     only.
//   - unconfirmedPlainPeer:    in-edge, unconfirmed, an address that
//     classifies as neither onion nor clearnet (no dots, not `.onion`)
//     — exercises a peer that shouldn't move either onion/clearnet
//     number regardless of confirmation.
//
// Expected raw totals (everyone counts, confirmed or not):
//   - Degree = 6 (all six distinct peers).
//   - OutDegree = 3 (confirmedOnionPeer, unconfirmedOnionPeer, confirmedDualStackPeer).
//   - InDegree = 3 (confirmedClearnetPeer, unconfirmedClearnetPeer, unconfirmedPlainPeer).
//   - OnionPeerCount = 3 (confirmedOnionPeer, unconfirmedOnionPeer, confirmedDualStackPeer).
//   - ClearnetPeerCount = 3 (confirmedClearnetPeer, unconfirmedClearnetPeer, confirmedDualStackPeer).
//
// Expected Live* totals (confirmed peers only):
//   - LiveDegree = 3 (confirmedOnionPeer, confirmedClearnetPeer, confirmedDualStackPeer).
//   - LiveOutDegree = 2 (confirmedOnionPeer, confirmedDualStackPeer — unconfirmedOnionPeer excluded).
//   - LiveInDegree = 1 (confirmedClearnetPeer — unconfirmedClearnetPeer and unconfirmedPlainPeer excluded).
//   - LiveOnionPeerCount = 2 (confirmedOnionPeer, confirmedDualStackPeer — unconfirmedOnionPeer excluded).
//   - LiveClearnetPeerCount = 2 (confirmedClearnetPeer, confirmedDualStackPeer — unconfirmedClearnetPeer excluded).
func TestTopPeeredNodesLiveCounts(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	const (
		confirmedOnionAddr      = "confonionpeer1abcdefghijklmnopqrst.onion:18142"
		unconfirmedOnionAddr    = "unconfonionpeer2abcdefghijklmnopqr.onion:18142"
		dualStackClearnetAddr   = "203.0.113.21:18142"
		dualStackOnionAddr      = "confdualstackpeerabcdefghijklmnopq.onion:18142"
		confirmedClearnetAddr   = "203.0.113.22:18142"
		unconfirmedClearnetAddr = "203.0.113.23:18142"
		unconfirmedPlainAddr    = "unconfirmed-plain-peer:18142"
	)

	hub, err := store.UpsertDiscoveredNode(ctx, "hub-live:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}

	confirmedOnionPeer, err := store.UpsertConfirmedNode(ctx, confirmedOnionAddr, []byte("some-unique-fake-pubkey-bytes-01"), DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed onion peer: %v", err)
	}
	unconfirmedOnionPeer, err := store.UpsertDiscoveredNode(ctx, unconfirmedOnionAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert unconfirmed onion peer: %v", err)
	}
	confirmedDualStackPeer, err := store.UpsertConfirmedNode(ctx, dualStackClearnetAddr, []byte("some-unique-fake-pubkey-bytes-02"), DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed dual-stack peer: %v", err)
	}
	// Give the confirmed dual-stack peer a SECOND node_addresses row (its
	// onion address) alongside the clearnet address it was created
	// with above.
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_addresses (node_id, address, first_seen, last_seen)
		VALUES ($1, $2, now(), now())
	`, confirmedDualStackPeer.ID, dualStackOnionAddr); err != nil {
		t.Fatalf("insert confirmed dual-stack peer's onion address: %v", err)
	}
	confirmedClearnetPeer, err := store.UpsertConfirmedNode(ctx, confirmedClearnetAddr, []byte("some-unique-fake-pubkey-bytes-03"), DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed clearnet peer: %v", err)
	}
	unconfirmedClearnetPeer, err := store.UpsertDiscoveredNode(ctx, unconfirmedClearnetAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert unconfirmed clearnet peer: %v", err)
	}
	unconfirmedPlainPeer, err := store.UpsertDiscoveredNode(ctx, unconfirmedPlainAddr, DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert unconfirmed plain peer: %v", err)
	}

	// Out-edges (hub -> peer).
	for _, peerID := range []uuid.UUID{confirmedOnionPeer.ID, unconfirmedOnionPeer.ID, confirmedDualStackPeer.ID} {
		if err := store.RecordPeerEdgeObservation(ctx, hub.ID, peerID); err != nil {
			t.Fatalf("record edge hub -> %v: %v", peerID, err)
		}
	}
	// In-edges (peer -> hub).
	for _, peerID := range []uuid.UUID{confirmedClearnetPeer.ID, unconfirmedClearnetPeer.ID, unconfirmedPlainPeer.ID} {
		if err := store.RecordPeerEdgeObservation(ctx, peerID, hub.ID); err != nil {
			t.Fatalf("record edge %v -> hub: %v", peerID, err)
		}
	}

	since := time.Now().Add(-time.Hour)
	top, err := store.TopPeeredNodes(ctx, since, 10)
	if err != nil {
		t.Fatalf("top peered nodes: %v", err)
	}

	var hubRow *NodeDegree
	for i := range top {
		if top[i].NodeID == hub.ID {
			hubRow = &top[i]
			break
		}
	}
	if hubRow == nil {
		t.Fatalf("hub %v not found in top peered nodes result: %+v", hub.ID, top)
	}

	// Raw totals: confirmed AND unconfirmed peers all count.
	if hubRow.Degree != 6 {
		t.Errorf("hub Degree = %d, want 6", hubRow.Degree)
	}
	if hubRow.OutDegree != 3 {
		t.Errorf("hub OutDegree = %d, want 3", hubRow.OutDegree)
	}
	if hubRow.InDegree != 3 {
		t.Errorf("hub InDegree = %d, want 3", hubRow.InDegree)
	}
	if hubRow.OnionPeerCount != 3 {
		t.Errorf("hub OnionPeerCount = %d, want 3", hubRow.OnionPeerCount)
	}
	if hubRow.ClearnetPeerCount != 3 {
		t.Errorf("hub ClearnetPeerCount = %d, want 3", hubRow.ClearnetPeerCount)
	}

	// Live totals: only confirmed (non-null public_key) peers count.
	if hubRow.LiveDegree != 3 {
		t.Errorf("hub LiveDegree = %d, want 3 (confirmedOnionPeer, confirmedClearnetPeer, confirmedDualStackPeer)", hubRow.LiveDegree)
	}
	if hubRow.LiveOutDegree != 2 {
		t.Errorf("hub LiveOutDegree = %d, want 2 (confirmedOnionPeer, confirmedDualStackPeer)", hubRow.LiveOutDegree)
	}
	if hubRow.LiveInDegree != 1 {
		t.Errorf("hub LiveInDegree = %d, want 1 (confirmedClearnetPeer)", hubRow.LiveInDegree)
	}
	if hubRow.LiveOnionPeerCount != 2 {
		t.Errorf("hub LiveOnionPeerCount = %d, want 2 (confirmedOnionPeer, confirmedDualStackPeer)", hubRow.LiveOnionPeerCount)
	}
	if hubRow.LiveClearnetPeerCount != 2 {
		t.Errorf("hub LiveClearnetPeerCount = %d, want 2 (confirmedClearnetPeer, confirmedDualStackPeer)", hubRow.LiveClearnetPeerCount)
	}

	t.Logf("hub row: degree=%d(live=%d) in=%d(live=%d) out=%d(live=%d) onion=%d(live=%d) clearnet=%d(live=%d)",
		hubRow.Degree, hubRow.LiveDegree, hubRow.InDegree, hubRow.LiveInDegree,
		hubRow.OutDegree, hubRow.LiveOutDegree, hubRow.OnionPeerCount, hubRow.LiveOnionPeerCount,
		hubRow.ClearnetPeerCount, hubRow.LiveClearnetPeerCount)
}

// TestListNodeEdges verifies that ListNodeEdges returns edges in both
// directions for a node (as either the from or to side) and respects the
// limit parameter.
func TestListNodeEdges(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	c, err := store.UpsertDiscoveredNode(ctx, "c:3", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert c: %v", err)
	}
	other1, err := store.UpsertDiscoveredNode(ctx, "other1:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert other1: %v", err)
	}
	other2, err := store.UpsertDiscoveredNode(ctx, "other2:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert other2: %v", err)
	}

	// a -> b (a is the "from" side), c -> a (a is the "to" side), plus
	// two edges unrelated to a that should never show up in a's results.
	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record a->b: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, c.ID, a.ID); err != nil {
		t.Fatalf("record c->a: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, other1.ID, other2.ID); err != nil {
		t.Fatalf("record other1->other2: %v", err)
	}

	edges, err := store.ListNodeEdges(ctx, a.ID, 50)
	if err != nil {
		t.Fatalf("list node edges: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("len(edges) = %d, want 2 (a->b and c->a), got %+v", len(edges), edges)
	}

	sawAToB, sawCToA := false, false
	for _, e := range edges {
		if e.FromNodeID == a.ID && e.ToNodeID == b.ID {
			sawAToB = true
		}
		if e.FromNodeID == c.ID && e.ToNodeID == a.ID {
			sawCToA = true
		}
		if e.FromNodeID != a.ID && e.ToNodeID != a.ID {
			t.Errorf("edge %+v does not involve node a", e)
		}
	}
	if !sawAToB || !sawCToA {
		t.Fatalf("expected both a->b and c->a among edges, got %+v", edges)
	}

	limited, err := store.ListNodeEdges(ctx, a.ID, 1)
	if err != nil {
		t.Fatalf("list node edges (limit 1): %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("len(limited) = %d, want 1", len(limited))
	}
}

// TestNetworkHeight verifies that NetworkHeight returns the mode of the
// latest-per-node heights, along with how many nodes contributed to that
// mode, and that it returns (nil, 0, nil) when no health checks with a
// non-nil height exist.
func TestNetworkHeight(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	height, count, err := store.NetworkHeight(ctx)
	if err != nil {
		t.Fatalf("network height (empty): %v", err)
	}
	if height != nil || count != 0 {
		t.Fatalf("network height (empty) = (%v, %d), want (nil, 0)", height, count)
	}

	a, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	c, err := store.UpsertDiscoveredNode(ctx, "c:3", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert c: %v", err)
	}

	h100, h101 := int64(100), int64(101)

	// a's latest height is 100 (its earlier 99 reading is superseded).
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: a.ID, Reachable: true, ProbeSource: ProbeSourceGRPC, Height: func() *int64 { v := int64(99); return &v }()}); err != nil {
		t.Fatalf("record a height 99: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: a.ID, Reachable: true, ProbeSource: ProbeSourceGRPC, Height: &h100}); err != nil {
		t.Fatalf("record a height 100: %v", err)
	}
	// b and c both latest at 100 too, so 100 is the mode across 3 nodes.
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: b.ID, Reachable: true, ProbeSource: ProbeSourceGRPC, Height: &h100}); err != nil {
		t.Fatalf("record b height 100: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: c.ID, Reachable: true, ProbeSource: ProbeSourceGRPC, Height: &h101}); err != nil {
		t.Fatalf("record c height 101: %v", err)
	}

	height, count, err = store.NetworkHeight(ctx)
	if err != nil {
		t.Fatalf("network height: %v", err)
	}
	if height == nil {
		t.Fatal("height = nil, want non-nil")
	}
	if *height != 100 {
		t.Errorf("height = %d, want 100", *height)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2 (a and b)", count)
	}
}

// TestNodeHealthHypertableCompatiblePK verifies that, after
// 0005_node_health_hypertable_pk_fix.sql has replaced node_health's
// single-column primary key with a composite UNIQUE(id, ts) constraint,
// TimescaleDB's create_hypertable() actually succeeds against the table
// on a real, working TimescaleDB install. Without that migration this
// fails with SQLSTATE TS103 ("cannot create a unique index without the
// column \"ts\" (used in partitioning)"), since Timescale requires the
// partitioning column to be part of any UNIQUE/PK constraint on the
// table.
//
// This sandbox's TimescaleDB install is known to be unusable (ABI
// mismatch between the only obtainable prebuilt TimescaleDB package and
// the only obtainable Postgres build — see
// 0002_timescale_hypertable_optional.sql), so if CREATE EXTENSION itself
// fails here, this test skips rather than fails: it is asserting that
// the PK fix works when TimescaleDB is actually available, not asserting
// that TimescaleDB is available in every test environment.
func TestNodeHealthHypertableCompatiblePK(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ps := store.(*pgStore)

	if _, err := ps.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		t.Skipf("skipping: timescaledb extension unavailable: %v", err)
	}

	if _, err := ps.pool.Exec(ctx,
		"SELECT create_hypertable('node_health', 'ts', if_not_exists => TRUE, migrate_data => TRUE)",
	); err != nil {
		t.Fatalf("create_hypertable: %v", err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Migrate was already called once inside newTestStore; calling it
	// again must be a no-op, not an error.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate call: %v", err)
	}
}

// TestMigrationBackfillsNodeAddresses proves
// 0006_pubkey_identity.sql's backfill INSERT actually populates
// node_addresses from existing nodes rows, and that it is safe/idempotent
// to re-run (as its own doc comment claims): it simulates a node row that
// predates node_addresses tracking (by deleting its node_addresses row
// after creating it directly), re-runs the migration's backfill statement
// verbatim, and asserts the row comes back.
func TestMigrationBackfillsNodeAddresses(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	var nodeID uuid.UUID
	if err := ps.pool.QueryRow(ctx, `
		INSERT INTO nodes (address, discovery_source, tags, first_seen, last_seen)
		VALUES ('legacy:1', 'p2p_discovered', '{}'::jsonb, now(), now())
		RETURNING id
	`).Scan(&nodeID); err != nil {
		t.Fatalf("insert legacy node: %v", err)
	}
	if _, err := ps.pool.Exec(ctx, `DELETE FROM node_addresses WHERE node_id = $1`, nodeID); err != nil {
		t.Fatalf("delete node_addresses: %v", err)
	}

	// Re-run 0006's backfill statement directly, simulating a re-run of
	// the (idempotent) migration itself.
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_addresses (node_id, address, first_seen, last_seen)
			SELECT id, address, first_seen, last_seen FROM nodes
			ON CONFLICT (node_id, address) DO NOTHING
	`); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	addrs, err := store.ListNodeAddresses(ctx, nodeID)
	if err != nil {
		t.Fatalf("list node addresses: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Address != "legacy:1" {
		t.Fatalf("addrs = %+v, want just legacy:1", addrs)
	}
}

// TestNodesPublicKeyPartialUniqueIndexAllowsMultipleNulls proves the
// partial unique index on nodes.public_key (WHERE public_key IS NOT NULL)
// does not reject multiple placeholder rows, each with public_key NULL.
func TestNodesPublicKeyPartialUniqueIndexAllowsMultipleNulls(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "b:2", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	nodes, err := store.ListNodes(ctx, NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2", len(nodes))
	}
	for _, n := range nodes {
		if n.PublicKey != nil {
			t.Errorf("node %s public_key = %x, want nil", n.Address, n.PublicKey)
		}
	}
}

// TestNodesPublicKeyUniqueIndexRejectsDuplicate proves the partial unique
// index on nodes.public_key DOES reject a second row with the same
// non-null public_key.
func TestNodesPublicKeyUniqueIndexRejectsDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	pubkey := []byte("dup-pubkey")
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO nodes (address, public_key, discovery_source, tags, first_seen, last_seen)
		VALUES ('a:1', $1, 'p2p_discovered', '{}'::jsonb, now(), now())
	`, pubkey); err != nil {
		t.Fatalf("insert first node: %v", err)
	}

	_, err := ps.pool.Exec(ctx, `
		INSERT INTO nodes (address, public_key, discovery_source, tags, first_seen, last_seen)
		VALUES ('b:2', $1, 'p2p_discovered', '{}'::jsonb, now(), now())
	`, pubkey)
	if err == nil {
		t.Fatal("expected a unique-violation error inserting a second node with the same public_key, got nil")
	}
}

// TestUpsertConfirmedNodePromotesPlaceholder is case (c) of
// UpsertConfirmedNode: an existing placeholder row (public_key NULL) at
// the given address gets promoted in place, preserving its id.
func TestUpsertConfirmedNodePromotesPlaceholder(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	placeholder, err := store.UpsertDiscoveredNode(ctx, "node:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert discovered: %v", err)
	}
	if placeholder.PublicKey != nil {
		t.Fatalf("expected placeholder.PublicKey = nil, got %x", placeholder.PublicKey)
	}

	pubkey := []byte("confirmed-pubkey-1")
	confirmed, err := store.UpsertConfirmedNode(ctx, "node:1", pubkey, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed: %v", err)
	}

	if confirmed.ID != placeholder.ID {
		t.Errorf("confirmed.ID = %v, want placeholder.ID = %v (promote in place)", confirmed.ID, placeholder.ID)
	}
	if !bytes.Equal(confirmed.PublicKey, pubkey) {
		t.Errorf("confirmed.PublicKey = %x, want %x", confirmed.PublicKey, pubkey)
	}
}

// TestUpsertConfirmedNodeAddsAddressToKnownPubkey is case (b) of
// UpsertConfirmedNode: a second, brand-new address confirmed under a
// pubkey that already has a confirmed node attaches to that same node
// (same id) rather than creating a new one.
func TestUpsertConfirmedNodeAddsAddressToKnownPubkey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	pubkey := []byte("confirmed-pubkey-2")
	n1, err := store.UpsertConfirmedNode(ctx, "addrA:1", pubkey, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed A: %v", err)
	}

	n2, err := store.UpsertConfirmedNode(ctx, "addrB:1", pubkey, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed B: %v", err)
	}

	if n2.ID != n1.ID {
		t.Fatalf("n2.ID = %v, want same as n1.ID = %v (same pubkey => same node)", n2.ID, n1.ID)
	}

	addrs, err := store.ListNodeAddresses(ctx, n1.ID)
	if err != nil {
		t.Fatalf("list node addresses: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("len(addrs) = %d, want 2 (addrA:1 and addrB:1)", len(addrs))
	}
}

// TestListSeedCandidates exercises ListSeedCandidates' full gating logic
// against four scenarios:
//
//   - n1: opted-in (registry_submitted) + healthy, recently observed ->
//     INCLUDED.
//   - n2: opted-in (both) but its only health-check row is STALE (older
//     than the queried window) -> EXCLUDED.
//   - n3: healthy, recently observed, but NOT opted-in
//     (p2p_discovered) -> EXCLUDED (the opt-in gate alone rules it out).
//   - n4: opted-in + healthy recent + MULTIPLE addresses under the same
//     pubkey -> INCLUDED, with both addresses present.
//
// Plus a fifth case proving a placeholder (no public_key) that is
// otherwise opted-in and healthy is still excluded, since it can't
// produce a peer_seeds line without a real pubkey.
func TestListSeedCandidates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ps := store.(*pgStore)

	pubkey1 := []byte("seed-pubkey-1")
	n1, err := store.UpsertConfirmedNode(ctx, "seed1:18189", pubkey1, DiscoverySourceRegistry)
	if err != nil {
		t.Fatalf("upsert n1: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: n1.ID, Reachable: true, ProbeSource: ProbeSourceGRPC}); err != nil {
		t.Fatalf("record health n1: %v", err)
	}

	pubkey2 := []byte("seed-pubkey-2")
	n2, err := store.UpsertConfirmedNode(ctx, "seed2:18189", pubkey2, DiscoverySourceBoth)
	if err != nil {
		t.Fatalf("upsert n2: %v", err)
	}
	if _, err := ps.pool.Exec(ctx, `
		INSERT INTO node_health (node_id, ts, reachable, probe_source)
		VALUES ($1, now() - interval '10 hours', true, 'grpc')
	`, n2.ID); err != nil {
		t.Fatalf("insert stale health n2: %v", err)
	}

	pubkey3 := []byte("seed-pubkey-3")
	n3, err := store.UpsertConfirmedNode(ctx, "seed3:18189", pubkey3, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert n3: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: n3.ID, Reachable: true, ProbeSource: ProbeSourceGRPC}); err != nil {
		t.Fatalf("record health n3: %v", err)
	}

	pubkey4 := []byte("seed-pubkey-4")
	n4, err := store.UpsertConfirmedNode(ctx, "seed4a:18189", pubkey4, DiscoverySourceRegistry)
	if err != nil {
		t.Fatalf("upsert n4a: %v", err)
	}
	if _, err := store.UpsertConfirmedNode(ctx, "seed4b:18141", pubkey4, DiscoverySourceRegistry); err != nil {
		t.Fatalf("upsert n4b: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: n4.ID, Reachable: true, ProbeSource: ProbeSourceGRPC}); err != nil {
		t.Fatalf("record health n4: %v", err)
	}

	// A placeholder (no public_key) that's opted-in and healthy must
	// also be excluded — it can't produce a peer_seeds line.
	placeholder, err := store.UpsertDiscoveredNode(ctx, "placeholder:18189", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert placeholder: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: placeholder.ID, Reachable: true, ProbeSource: ProbeSourceGRPC}); err != nil {
		t.Fatalf("record health placeholder: %v", err)
	}

	got, err := store.ListSeedCandidates(ctx, 3*time.Hour)
	if err != nil {
		t.Fatalf("list seed candidates: %v", err)
	}

	byID := map[uuid.UUID]SeedCandidate{}
	for _, c := range got {
		byID[c.NodeID] = c
	}

	if _, ok := byID[n1.ID]; !ok {
		t.Errorf("expected n1 (opted-in + healthy recent) to be included, got %+v", got)
	}
	if _, ok := byID[n2.ID]; ok {
		t.Errorf("expected n2 (opted-in + stale health) to be EXCLUDED, got %+v", got)
	}
	if _, ok := byID[n3.ID]; ok {
		t.Errorf("expected n3 (not opted-in) to be EXCLUDED, got %+v", got)
	}
	c4, ok := byID[n4.ID]
	if !ok {
		t.Fatalf("expected n4 (opted-in + healthy + multi-address) to be included, got %+v", got)
	}
	if len(c4.Addresses) != 2 {
		t.Fatalf("n4 addresses = %+v, want 2", c4.Addresses)
	}
	addrSet := map[string]bool{}
	for _, a := range c4.Addresses {
		addrSet[a] = true
	}
	if !addrSet["seed4a:18189"] || !addrSet["seed4b:18141"] {
		t.Errorf("n4 addresses = %+v, want seed4a:18189 and seed4b:18141", c4.Addresses)
	}
	if !bytes.Equal(c4.PublicKey, pubkey4) {
		t.Errorf("n4 public key = %x, want %x", c4.PublicKey, pubkey4)
	}
	if _, ok := byID[placeholder.ID]; ok {
		t.Errorf("expected placeholder (no public_key) to be EXCLUDED, got %+v", got)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2 (n1 and n4 only), got %+v", len(got), got)
	}
}

// TestUpsertConfirmedNodeMergesPlaceholderIntoConfirmedNode is case (d) of
// UpsertConfirmedNode, and the most important new test in this task: two
// independent placeholders (A, B), each already carrying its own
// health-check and peer-edge history, turn out — once directly, separately
// probed — to be the SAME real node (pubkeyX). Promoting A first, then
// confirming B at the same pubkeyX, must MERGE B into A: A's id survives,
// B's history is repointed onto A's id, B's address becomes one of A's
// addresses, and B's own nodes row is gone.
func TestUpsertConfirmedNodeMergesPlaceholderIntoConfirmedNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.UpsertDiscoveredNode(ctx, "addrA:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert discovered A: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "addrB:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert discovered B: %v", err)
	}
	other, err := store.UpsertDiscoveredNode(ctx, "other:1", DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert discovered other: %v", err)
	}

	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: a.ID, Reachable: true, ProbeSource: ProbeSourceP2P}); err != nil {
		t.Fatalf("record health check A: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, a.ID, other.ID); err != nil {
		t.Fatalf("record edge A->other: %v", err)
	}

	if err := store.RecordHealthCheck(ctx, HealthCheckInput{NodeID: b.ID, Reachable: true, ProbeSource: ProbeSourceP2P}); err != nil {
		t.Fatalf("record health check B: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, b.ID, other.ID); err != nil {
		t.Fatalf("record edge B->other: %v", err)
	}

	pubkeyX := []byte("shared-pubkey-x")

	// Promote A to a confirmed node with pubkeyX.
	confirmedA, err := store.UpsertConfirmedNode(ctx, "addrA:1", pubkeyX, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed A: %v", err)
	}
	if confirmedA.ID != a.ID {
		t.Fatalf("confirmedA.ID = %v, want a.ID = %v (promote in place)", confirmedA.ID, a.ID)
	}

	// Confirm B is reachable at addrB:1 with the SAME pubkeyX: B (still a
	// placeholder) must be merged into A (already confirmed with
	// pubkeyX). The surviving node's id is documented to be the
	// already-confirmed node's id (A's), not B's.
	merged, err := store.UpsertConfirmedNode(ctx, "addrB:1", pubkeyX, DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed B (merge): %v", err)
	}
	if merged.ID != a.ID {
		t.Fatalf("merged.ID = %v, want a.ID = %v (A survives the merge)", merged.ID, a.ID)
	}

	// B's own nodes row must be gone.
	if _, err := store.GetNode(ctx, b.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetNode(b.ID) err = %v, want ErrNotFound (placeholder should have been deleted)", err)
	}

	// B's health check must now reference the surviving node's ID.
	history, err := store.GetNodeHistory(ctx, a.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2 (A's + B's health checks, both now under a.ID)", len(history))
	}

	// B's node_addresses row must now point at the surviving node.
	addrs, err := store.ListNodeAddresses(ctx, a.ID)
	if err != nil {
		t.Fatalf("list node addresses: %v", err)
	}
	addrSet := map[string]bool{}
	for _, addr := range addrs {
		addrSet[addr.Address] = true
	}
	if !addrSet["addrA:1"] || !addrSet["addrB:1"] {
		t.Fatalf("addrs = %+v, want both addrA:1 and addrB:1 under the surviving node", addrs)
	}

	// Both peer edges (A->other, and B->other repointed to A->other) must
	// now reference a.ID directly in peer_edge_observations -- queried
	// directly, not just inferred from ListTopology's rollup view.
	ps := store.(*pgStore)
	var fromCount int
	if err := ps.pool.QueryRow(ctx, `
		SELECT count(*) FROM peer_edge_observations WHERE from_node_id = $1 AND to_node_id = $2
	`, a.ID, other.ID).Scan(&fromCount); err != nil {
		t.Fatalf("query peer_edge_observations: %v", err)
	}
	if fromCount != 2 {
		t.Fatalf("peer_edge_observations from=a.ID to=other.ID count = %d, want 2 (A's original edge + B's repointed edge)", fromCount)
	}
}
