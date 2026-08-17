package collector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
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
// returned func releases the lock and must be registered as a cleanup that
// runs after the test (and its Store) are done with the DB.
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
// the test to serialize access with the storage and api packages' tests
// against the same shared database. The discovery-walk-logic tests
// themselves never touch the network — only this real Postgres Store and
// the in-memory fakeClient below.
func newTestStore(t *testing.T) storage.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dsn := testDSN()

	// Registered before store.Close below, so it runs last (t.Cleanup is
	// LIFO): the lock stays held until the Store is fully closed.
	t.Cleanup(acquireTestDBLock(t, ctx, dsn))

	store, err := storage.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: cannot reach test database: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for truncate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edges, nodes CASCADE"); err != nil {
		pool.Close()
		t.Fatalf("truncate test tables: %v", err)
	}
	pool.Close()

	t.Cleanup(func() { _ = store.Close() })
	return store
}

// fakeClient is an in-memory fixture NodeClient: no real network access,
// used to exercise the collector's discovery-walk and polling logic
// deterministically.
type fakeClient struct {
	peers map[string][]string
	info  map[string]NodeInfo
}

func (f *fakeClient) GetPeers(ctx context.Context, addr string) ([]string, error) {
	return f.peers[addr], nil
}

func (f *fakeClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	info, ok := f.info[addr]
	if !ok {
		return NodeInfo{}, fmt.Errorf("collector_test: no fixture GetInfo response for %s", addr)
	}
	return info, nil
}

func TestDiscoverWalksAndDedupes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{
		peers: map[string][]string{
			"seed:1":  {"peerA:1", "peerB:1"},
			"peerA:1": {"peerB:1", "seed:1"}, // cycle back to seed + shared peer
			"peerB:1": {},
		},
	}

	c := New(Config{SeedNodes: []string{"seed:1"}})
	c.Storage = store
	c.GRPCClient = client

	if err := c.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("len(nodes) = %d, want 3 (seed, peerA, peerB)", len(nodes))
	}
	for _, n := range nodes {
		if n.DiscoverySource != storage.DiscoverySourceP2P {
			t.Errorf("node %s discovery_source = %q, want %q", n.Address, n.DiscoverySource, storage.DiscoverySourceP2P)
		}
	}

	_, edges, err := store.ListTopology(ctx)
	if err != nil {
		t.Fatalf("list topology: %v", err)
	}
	// seed->peerA, seed->peerB, peerA->peerB, peerA->seed = 4 distinct
	// directed edges from the walk. peerB is reached via both seed and
	// peerA (visited-set dedup means it's only queued/walked once), and
	// its own outgoing peer list is empty so it contributes no edges.
	if len(edges) != 4 {
		t.Fatalf("len(edges) = %d, want 4", len(edges))
	}
}

// TestDiscoverWalksBothTransportsIndependently verifies that when both
// GRPCClient and P2PClient are configured, Discover runs a separate walk
// over each, and a peer only reachable via one transport still ends up
// discovered — neither walk's peer graph is required to feed into the
// other's, and one having a smaller graph doesn't limit the other.
func TestDiscoverWalksBothTransportsIndependently(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	grpcClient := &fakeClient{
		peers: map[string][]string{
			"seed:1": {"grpc-only-peer:1"},
		},
	}
	p2pClient := &fakeClient{
		peers: map[string][]string{
			"seed:1": {"p2p-only-peer:1"},
		},
	}

	c := New(Config{SeedNodes: []string{"seed:1"}})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	addrs := make(map[string]bool)
	for _, n := range nodes {
		addrs[n.Address] = true
	}
	for _, want := range []string{"seed:1", "grpc-only-peer:1", "p2p-only-peer:1"} {
		if !addrs[want] {
			t.Errorf("expected node %q to be discovered, got nodes %v", want, addrs)
		}
	}
	if len(nodes) != 3 {
		t.Fatalf("len(nodes) = %d, want 3", len(nodes))
	}
}

func TestPollRecordsHealthChecksAndRespectsCadence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	height := int64(500)
	client := &fakeClient{
		info: map[string]NodeInfo{
			"node:1": {Reachable: true, Height: &height},
		},
	}

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "node:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if !history[0].Reachable {
		t.Errorf("expected reachable = true")
	}
	if history[0].Height == nil || *history[0].Height != height {
		t.Errorf("height = %v, want %d", history[0].Height, height)
	}
	if history[0].ProbeSource != storage.ProbeSourceGRPC {
		t.Errorf("probe_source = %q, want %q", history[0].ProbeSource, storage.ProbeSourceGRPC)
	}

	// Polling again immediately must be a no-op: the generic node's
	// next-poll time (PollIntervalGeneric = 1h) hasn't elapsed yet.
	if err := c.Poll(ctx); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	history, err = store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history after second poll: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) after second poll = %d, want still 1 (poll cadence should skip)", len(history))
	}
}

func TestPollUnreachableNodeRecordsFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{info: map[string]NodeInfo{}} // no fixture => GetInfo errors

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "unreachable:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].Reachable {
		t.Errorf("expected reachable = false for a GetInfo error")
	}
	if history[0].ProbeSource != storage.ProbeSourceGRPC {
		t.Errorf("probe_source = %q, want %q", history[0].ProbeSource, storage.ProbeSourceGRPC)
	}
}

// historyByProbeSource groups history rows by their ProbeSource, for tests
// that need to assert on both a grpc-sourced and a p2p-sourced row
// independently.
func historyByProbeSource(history []storage.HealthCheck) map[storage.ProbeSource][]storage.HealthCheck {
	out := make(map[storage.ProbeSource][]storage.HealthCheck)
	for _, h := range history {
		out[h.ProbeSource] = append(out[h.ProbeSource], h)
	}
	return out
}

// TestPollDualProbeBothSucceed verifies that when both GRPCClient and
// P2PClient are configured and both successfully report info for a node,
// Poll (via PollOnce) records two independent rows, one per probe_source.
func TestPollDualProbeBothSucceed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	grpcHeight := int64(100)
	p2pHeight := int64(200)
	grpcClient := &fakeClient{info: map[string]NodeInfo{
		"node:1": {Reachable: true, Height: &grpcHeight},
	}}
	p2pClient := &fakeClient{info: map[string]NodeInfo{
		"node:1": {Reachable: true, Height: &p2pHeight},
	}}

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "node:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2 (one per probe source)", len(history))
	}

	bySource := historyByProbeSource(history)
	grpcRows := bySource[storage.ProbeSourceGRPC]
	p2pRows := bySource[storage.ProbeSourceP2P]
	if len(grpcRows) != 1 {
		t.Fatalf("len(grpc rows) = %d, want 1", len(grpcRows))
	}
	if len(p2pRows) != 1 {
		t.Fatalf("len(p2p rows) = %d, want 1", len(p2pRows))
	}
	if grpcRows[0].Height == nil || *grpcRows[0].Height != grpcHeight {
		t.Errorf("grpc row height = %v, want %d", grpcRows[0].Height, grpcHeight)
	}
	if p2pRows[0].Height == nil || *p2pRows[0].Height != p2pHeight {
		t.Errorf("p2p row height = %v, want %d", p2pRows[0].Height, p2pHeight)
	}
}

// TestPollDualProbeGRPCFailsP2PSucceeds verifies that when GRPCClient
// fails (no fixture => GetInfo errors) but P2PClient succeeds for the same
// node, both attempts are still made independently: a p2p-sourced
// reachable row is recorded, and a grpc-sourced unreachable row is
// recorded — not a missing grpc row and not a skipped p2p attempt.
func TestPollDualProbeGRPCFailsP2PSucceeds(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	p2pHeight := int64(300)
	grpcClient := &fakeClient{info: map[string]NodeInfo{}} // no fixture => errors
	p2pClient := &fakeClient{info: map[string]NodeInfo{
		"node:1": {Reachable: true, Height: &p2pHeight},
	}}

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "node:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2 (one failed grpc row, one successful p2p row)", len(history))
	}

	bySource := historyByProbeSource(history)
	grpcRows := bySource[storage.ProbeSourceGRPC]
	p2pRows := bySource[storage.ProbeSourceP2P]
	if len(grpcRows) != 1 || grpcRows[0].Reachable {
		t.Fatalf("grpc rows = %+v, want exactly 1 unreachable row", grpcRows)
	}
	if len(p2pRows) != 1 || !p2pRows[0].Reachable {
		t.Fatalf("p2p rows = %+v, want exactly 1 reachable row", p2pRows)
	}
	if p2pRows[0].Height == nil || *p2pRows[0].Height != p2pHeight {
		t.Errorf("p2p row height = %v, want %d", p2pRows[0].Height, p2pHeight)
	}
}

// TestPollDualProbeP2PFailsGRPCSucceeds is the mirror of
// TestPollDualProbeGRPCFailsP2PSucceeds, with the failing/succeeding
// transports swapped.
func TestPollDualProbeP2PFailsGRPCSucceeds(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	grpcHeight := int64(400)
	grpcClient := &fakeClient{info: map[string]NodeInfo{
		"node:1": {Reachable: true, Height: &grpcHeight},
	}}
	p2pClient := &fakeClient{info: map[string]NodeInfo{}} // no fixture => errors

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "node:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2 (one successful grpc row, one failed p2p row)", len(history))
	}

	bySource := historyByProbeSource(history)
	grpcRows := bySource[storage.ProbeSourceGRPC]
	p2pRows := bySource[storage.ProbeSourceP2P]
	if len(grpcRows) != 1 || !grpcRows[0].Reachable {
		t.Fatalf("grpc rows = %+v, want exactly 1 reachable row", grpcRows)
	}
	if len(p2pRows) != 1 || p2pRows[0].Reachable {
		t.Fatalf("p2p rows = %+v, want exactly 1 unreachable row", p2pRows)
	}
	if grpcRows[0].Height == nil || *grpcRows[0].Height != grpcHeight {
		t.Errorf("grpc row height = %v, want %d", grpcRows[0].Height, grpcHeight)
	}
}

// TestPollNilP2PClientSkipsP2PProbe verifies that a nil P2PClient (the
// default for a Collector that only has GRPCClient configured) is simply
// skipped, not treated as an error, and does not prevent the GRPCClient
// probe from being recorded.
func TestPollNilP2PClientSkipsP2PProbe(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	height := int64(50)
	grpcClient := &fakeClient{info: map[string]NodeInfo{
		"node:1": {Reachable: true, Height: &height},
	}}

	node, err := store.UpsertNode(ctx, storage.NodeInput{
		Address:         "node:1",
		DiscoverySource: storage.DiscoverySourceP2P,
	})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	// c.P2PClient intentionally left nil.

	if err := c.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1 (grpc only, p2p skipped)", len(history))
	}
	if history[0].ProbeSource != storage.ProbeSourceGRPC {
		t.Errorf("probe_source = %q, want %q", history[0].ProbeSource, storage.ProbeSourceGRPC)
	}
}

func TestPollIntervalUsesPoolOwnedCadence(t *testing.T) {
	poolOwned := storage.Node{Tags: map[string]any{"pool_owned": true}}
	regular := storage.Node{Tags: map[string]any{}}

	c := New(Config{})
	if got := c.pollInterval(poolOwned); got != PollIntervalPoolOwned {
		t.Errorf("pollInterval(pool-owned) = %v, want %v", got, PollIntervalPoolOwned)
	}
	if got := c.pollInterval(regular); got != PollIntervalGeneric {
		t.Errorf("pollInterval(regular) = %v, want %v", got, PollIntervalGeneric)
	}
}

func TestRunRespectsContextCancellation(t *testing.T) {
	store := newTestStore(t)

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = &fakeClient{}
	c.TickInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after ctx cancellation")
	}
}
