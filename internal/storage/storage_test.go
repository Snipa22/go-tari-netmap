package storage

import (
	"context"
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
	if _, err := ps.pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edge_observations, nodes CASCADE"); err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	return store
}

func TestUpsertNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	n, err := store.UpsertNode(ctx, NodeInput{
		Address:         "127.0.0.1:18142",
		DiscoverySource: DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if n.Address != "127.0.0.1:18142" {
		t.Errorf("address = %q, want %q", n.Address, "127.0.0.1:18142")
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
	n2, err := store.UpsertNode(ctx, NodeInput{
		Address:         "127.0.0.1:18142",
		DiscoverySource: DiscoverySourceRegistry,
		Tags:            map[string]any{"pool_owned": true},
	})
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
}

func TestListNodesFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.UpsertNode(ctx, NodeInput{Address: "a:1", DiscoverySource: DiscoverySourceP2P}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertNode(ctx, NodeInput{Address: "b:2", DiscoverySource: DiscoverySourceRegistry}); err != nil {
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

	n, err := store.UpsertNode(ctx, NodeInput{Address: "node:1", DiscoverySource: DiscoverySourceP2P})
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

	n, err := store.UpsertNode(ctx, NodeInput{Address: "node:1", DiscoverySource: DiscoverySourceP2P})
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

	n, err := store.UpsertNode(ctx, NodeInput{Address: "node:1", DiscoverySource: DiscoverySourceP2P})
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

	a, err := store.UpsertNode(ctx, NodeInput{Address: "a:1", DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertNode(ctx, NodeInput{Address: "b:2", DiscoverySource: DiscoverySourceP2P})
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

	nodes, edges, err := store.ListTopology(ctx)
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

	a, err := store.UpsertNode(ctx, NodeInput{Address: "a:1", DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertNode(ctx, NodeInput{Address: "b:2", DiscoverySource: DiscoverySourceP2P})
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

	hub, err := store.UpsertNode(ctx, NodeInput{Address: "hub:1", DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	leaf1, err := store.UpsertNode(ctx, NodeInput{Address: "leaf1:1", DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert leaf1: %v", err)
	}
	leaf2, err := store.UpsertNode(ctx, NodeInput{Address: "leaf2:1", DiscoverySource: DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert leaf2: %v", err)
	}
	leaf3, err := store.UpsertNode(ctx, NodeInput{Address: "leaf3:1", DiscoverySource: DiscoverySourceP2P})
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

func TestMigrateIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Migrate was already called once inside newTestStore; calling it
	// again must be a no-op, not an error.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate call: %v", err)
	}
}
