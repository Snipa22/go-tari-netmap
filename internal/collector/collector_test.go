package collector

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/google/uuid"

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
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edge_observations, node_addresses, pending_submissions, nodes CASCADE"); err != nil {
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

	// getPeersCalls counts GetPeers calls per address, letting tests
	// assert directly on whether/how-many-times a given node was
	// (re-)dialed for its peer list, rather than only inferring it from
	// what did or didn't end up in storage.
	getPeersCalls map[string]int
}

func (f *fakeClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	if f.getPeersCalls != nil {
		f.getPeersCalls[addr]++
	}
	addrs := f.peers[addr]
	if addrs == nil {
		return nil, nil
	}
	peers := make([]DiscoveredPeer, len(addrs))
	for i, a := range addrs {
		peers[i] = DiscoveredPeer{Address: a}
	}
	return peers, nil
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

	_, edges, err := store.ListTopology(ctx, storage.TopologyFilter{})
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

// TestDiscoverRespectsPerNodeCooldown verifies that a node re-encountered
// within the same seed list on a second Discover() call, before its
// discovery cooldown (DiscoveryIntervalGeneric) has elapsed, is upserted
// (recording it was seen) but NOT re-dialed via GetPeers — proving the
// per-node discovery cooldown actually gates the expensive/impolite
// re-walk, not just the cheap node-existence bookkeeping.
func TestDiscoverRespectsPerNodeCooldown(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{
		peers: map[string][]string{
			"seed:1":  {"peerA:1"},
			"peerA:1": {},
		},
		getPeersCalls: map[string]int{},
	}

	c := New(Config{SeedNodes: []string{"seed:1"}})
	c.Storage = store
	c.GRPCClient = client

	if err := c.Discover(ctx); err != nil {
		t.Fatalf("first discover: %v", err)
	}
	if client.getPeersCalls["seed:1"] != 1 {
		t.Fatalf("getPeersCalls[seed:1] after first discover = %d, want 1", client.getPeersCalls["seed:1"])
	}

	// Mutate seed:1's peer list to include a brand-new peer. If seed:1
	// gets re-dialed on the second Discover() call (the cooldown bug),
	// this new peer would show up in storage; if the cooldown correctly
	// blocks the re-dial, it won't.
	client.peers["seed:1"] = []string{"peerA:1", "newPeer:1"}

	if err := c.Discover(ctx); err != nil {
		t.Fatalf("second discover: %v", err)
	}

	if client.getPeersCalls["seed:1"] != 1 {
		t.Errorf("getPeersCalls[seed:1] after second discover = %d, want still 1 (cooldown should block re-dial)", client.getPeersCalls["seed:1"])
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	for _, n := range nodes {
		if n.Address == "newPeer:1" {
			t.Fatalf("newPeer:1 was discovered even though seed:1's discovery cooldown should have blocked re-dialing it; nodes = %+v", nodes)
		}
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

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	// node:1 has zero recorded health checks at this point, so its
	// very first poll comes from PollNeverContacted, not PollUnconfirmed
	// (see PollNeverContacted's doc comment).
	if err := c.PollNeverContacted(ctx); err != nil {
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

	// Polling again immediately must be a no-op: node:1 now has one
	// recorded health check, so it has graduated onto PollUnconfirmed's
	// side of the split -- but its next-poll time (set by the
	// escalating new-node checkpoint schedule off the call above)
	// hasn't elapsed yet, so due() correctly skips it.
	if err := c.PollUnconfirmed(ctx); err != nil {
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

// TestPollThreadsPeerIdentityUpdatedAtIntoStorage verifies that a
// NodeInfo.PeerIdentityUpdatedAt returned by GetInfo makes it all the way
// through pollOnceWithSource into the recorded storage.HealthCheck.
func TestPollThreadsPeerIdentityUpdatedAtIntoStorage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	identityTS := time.Now().Add(-30 * time.Minute).Truncate(time.Microsecond).UTC()
	client := &fakeClient{
		info: map[string]NodeInfo{
			"node:1": {Reachable: true, PeerIdentityUpdatedAt: &identityTS},
		},
	}

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.PollNeverContacted(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	history, err := store.GetNodeHistory(ctx, node.ID, 10)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].PeerIdentityUpdatedAt == nil || !history[0].PeerIdentityUpdatedAt.Equal(identityTS) {
		t.Errorf("history[0].PeerIdentityUpdatedAt = %v, want %v", history[0].PeerIdentityUpdatedAt, identityTS)
	}
}

func TestPollUnreachableNodeRecordsFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{info: map[string]NodeInfo{}} // no fixture => GetInfo errors

	node, err := store.UpsertDiscoveredNode(ctx, "unreachable:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.PollNeverContacted(ctx); err != nil {
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

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.PollNeverContacted(ctx); err != nil {
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

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.PollNeverContacted(ctx); err != nil {
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

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient

	if err := c.PollNeverContacted(ctx); err != nil {
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

	node, err := store.UpsertDiscoveredNode(ctx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = grpcClient
	// c.P2PClient intentionally left nil.

	if err := c.PollNeverContacted(ctx); err != nil {
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
	ctx := context.Background()
	poolOwned := storage.Node{PublicKey: []byte{0x01, 0x02}, Tags: map[string]any{"pool_owned": true}}
	regular := storage.Node{PublicKey: []byte{0x03, 0x04}, Tags: map[string]any{}}

	c := New(Config{})
	if got := c.pollInterval(ctx, poolOwned); got != PollIntervalPoolOwned {
		t.Errorf("pollInterval(pool-owned) = %v, want %v", got, PollIntervalPoolOwned)
	}
	if got := c.pollInterval(ctx, regular); got != PollIntervalGeneric {
		t.Errorf("pollInterval(regular) = %v, want %v", got, PollIntervalGeneric)
	}
}

// TestPollIntervalUsesPoolOwnedCadenceForOwnerTag covers the real-world
// production case that motivated isPoolOwned's owner-tag signal: a
// confirmed node approved via the submission-review flow (see
// internal/api/api.go's approve-submission handler) only ever gets
// tags["owner"] set — tags["pool_owned"] is never written by that path.
// Production has 24 confirmed nodes with tags = {"owner": "Jagtech"} and
// zero nodes anywhere with pool_owned: true, so before this fix every one
// of those nodes was incorrectly falling through to PollIntervalGeneric's
// 2-hour cadence instead of the intended 5-minute PollIntervalPoolOwned.
func TestPollIntervalUsesPoolOwnedCadenceForOwnerTag(t *testing.T) {
	ctx := context.Background()
	ownerTagged := storage.Node{PublicKey: []byte{0x05, 0x06}, Tags: map[string]any{"owner": "Jagtech"}}

	c := New(Config{})
	if got := c.pollInterval(ctx, ownerTagged); got != PollIntervalPoolOwned {
		t.Errorf("pollInterval(owner-tagged, no pool_owned key) = %v, want %v", got, PollIntervalPoolOwned)
	}
}

// TestPollIntervalIgnoresEmptyOwnerTag covers the negative case: an empty
// string owner tag must not count as "someone registered/claimed this
// IP", so it must not trigger fast cadence.
func TestPollIntervalIgnoresEmptyOwnerTag(t *testing.T) {
	ctx := context.Background()
	emptyOwner := storage.Node{PublicKey: []byte{0x07, 0x08}, Tags: map[string]any{"owner": ""}}

	c := New(Config{})
	if got := c.pollInterval(ctx, emptyOwner); got != PollIntervalGeneric {
		t.Errorf("pollInterval(empty owner tag) = %v, want %v", got, PollIntervalGeneric)
	}
}

// TestDiscoveryIntervalUsesPoolOwnedCadence covers discoveryInterval's use
// of the same isPoolOwned helper as pollInterval, for both existing
// signals: the explicit pool_owned bool and the real-world owner-tag
// path. See TestPollIntervalUsesPoolOwnedCadenceForOwnerTag for the
// production evidence behind the owner-tag case.
func TestDiscoveryIntervalUsesPoolOwnedCadence(t *testing.T) {
	poolOwned := storage.Node{PublicKey: []byte{0x01, 0x02}, Tags: map[string]any{"pool_owned": true}}
	ownerTagged := storage.Node{PublicKey: []byte{0x05, 0x06}, Tags: map[string]any{"owner": "Jagtech"}}
	emptyOwner := storage.Node{PublicKey: []byte{0x07, 0x08}, Tags: map[string]any{"owner": ""}}
	regular := storage.Node{PublicKey: []byte{0x03, 0x04}, Tags: map[string]any{}}

	c := New(Config{})
	if got := c.discoveryInterval(poolOwned); got != DiscoveryIntervalPoolOwned {
		t.Errorf("discoveryInterval(pool-owned) = %v, want %v", got, DiscoveryIntervalPoolOwned)
	}
	if got := c.discoveryInterval(ownerTagged); got != DiscoveryIntervalPoolOwned {
		t.Errorf("discoveryInterval(owner-tagged, no pool_owned key) = %v, want %v", got, DiscoveryIntervalPoolOwned)
	}
	if got := c.discoveryInterval(emptyOwner); got != DiscoveryIntervalGeneric {
		t.Errorf("discoveryInterval(empty owner tag) = %v, want %v", got, DiscoveryIntervalGeneric)
	}
	if got := c.discoveryInterval(regular); got != DiscoveryIntervalGeneric {
		t.Errorf("discoveryInterval(regular) = %v, want %v", got, DiscoveryIntervalGeneric)
	}
}

// TestPollIntervalUsesUnconfirmedCadenceForPlaceholderNodes covers
// scenario (d): an unconfirmed placeholder node with zero history is not
// (and cannot be) likely-dead — collectorLikelyDead needs 3+ history
// entries to conclude anything — so it still gets the normal
// PollIntervalUnconfirmed cadence, while a confirmed node gets
// PollIntervalGeneric.
//
// unconfirmedNode's FirstSeen is explicitly backdated past
// NewNodeCheckpoint3 so this test continues to exercise the flat
// steady-state PollIntervalUnconfirmed cadence in isolation from the new
// age-based checkpoint schedule (see NewNodeCheckpoint1/2/3), which would
// otherwise apply to a freshly-discovered node (age ~= 0).
func TestPollIntervalUsesUnconfirmedCadenceForPlaceholderNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	unconfirmedNode, err := store.UpsertDiscoveredNode(ctx, "unconfirmed:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed unconfirmed node: %v", err)
	}
	unconfirmedNode.FirstSeen = time.Now().Add(-90 * time.Minute)
	confirmedNode, err := store.UpsertConfirmedNode(ctx, "confirmed:1", []byte{0x01, 0x02}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed confirmed node: %v", err)
	}

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, unconfirmedNode); got != PollIntervalUnconfirmed {
		t.Errorf("pollInterval(unconfirmed, no history) = %v, want %v", got, PollIntervalUnconfirmed)
	}
	if got := c.pollInterval(ctx, confirmedNode); got != PollIntervalGeneric {
		t.Errorf("pollInterval(confirmed) = %v, want %v", got, PollIntervalGeneric)
	}
	if PollIntervalUnconfirmed >= PollIntervalGeneric {
		t.Errorf("PollIntervalUnconfirmed = %v, want shorter than PollIntervalGeneric = %v", PollIntervalUnconfirmed, PollIntervalGeneric)
	}
}

// recordHistory records n health checks for nodeID, in order, with
// reachable taken from the given sequence (recorded oldest-first so that
// GetNodeHistory's newest-first ordering matches reachable's order
// reversed — callers here only care about the aggregate
// collectorLikelyDead result, which is order-independent, so this detail
// doesn't otherwise matter).
func recordHistory(t *testing.T, ctx context.Context, store storage.Store, nodeID uuid.UUID, reachable ...bool) {
	t.Helper()
	for _, r := range reachable {
		if err := store.RecordHealthCheck(ctx, storage.HealthCheckInput{
			NodeID:      nodeID,
			Reachable:   r,
			ProbeSource: storage.ProbeSourceGRPC,
		}); err != nil {
			t.Fatalf("record health check: %v", err)
		}
	}
}

// TestPollIntervalBacksOffLikelyDeadUnconfirmedNode covers scenario (a):
// an unconfirmed node with 3+ consecutive failed probes and zero
// successes is backed off to PollIntervalLikelyDead.
//
// node's FirstSeen is explicitly backdated past NewNodeCheckpoint3 so
// this test exercises the likely-dead logic in isolation from the new
// age-based checkpoint schedule — collectorLikelyDead is only ever
// reachable once age >= NewNodeCheckpoint3 anyway (see pollInterval's doc
// comment on the precedence rule).
func TestPollIntervalBacksOffLikelyDeadUnconfirmedNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "likely-dead:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, node.ID, false, false, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalLikelyDead {
		t.Errorf("pollInterval(likely-dead unconfirmed) = %v, want %v", got, PollIntervalLikelyDead)
	}
}

// TestPollIntervalUnconfirmedNodeWithFewHistoryEntriesStaysUnconfirmed
// covers scenario (b): fewer than 3 history entries (even if all
// failures) is not enough to conclude "likely dead", so the node stays
// on the normal PollIntervalUnconfirmed cadence.
//
// Both nodes' FirstSeen are explicitly backdated past NewNodeCheckpoint3
// so this test exercises the history-count logic in isolation from the
// new age-based checkpoint schedule.
func TestPollIntervalUnconfirmedNodeWithFewHistoryEntriesStaysUnconfirmed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	oneFailure, err := store.UpsertDiscoveredNode(ctx, "one-failure:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	oneFailure.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, oneFailure.ID, false)

	twoFailures, err := store.UpsertDiscoveredNode(ctx, "two-failures:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	twoFailures.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, twoFailures.ID, false, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, oneFailure); got != PollIntervalUnconfirmed {
		t.Errorf("pollInterval(1 failure) = %v, want %v", got, PollIntervalUnconfirmed)
	}
	if got := c.pollInterval(ctx, twoFailures); got != PollIntervalUnconfirmed {
		t.Errorf("pollInterval(2 failures) = %v, want %v", got, PollIntervalUnconfirmed)
	}
}

// TestPollIntervalUnconfirmedNodeWithAnySuccessStaysUnconfirmed covers
// scenario (c): 3+ history entries but at least one Reachable == true
// means the node is not likely-dead, so it stays on
// PollIntervalUnconfirmed.
//
// node's FirstSeen is explicitly backdated past NewNodeCheckpoint3 so
// this test exercises the likely-dead logic in isolation from the new
// age-based checkpoint schedule.
func TestPollIntervalUnconfirmedNodeWithAnySuccessStaysUnconfirmed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "mixed-history:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, node.ID, false, true, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalUnconfirmed {
		t.Errorf("pollInterval(mixed history, unconfirmed) = %v, want %v", got, PollIntervalUnconfirmed)
	}
}

// TestPollIntervalConfirmedNodeUnaffectedByHistory covers scenario (e): a
// confirmed, non-pool-owned node with 3+ failed history entries (which
// would satisfy collectorLikelyDead if it were checked) still gets
// PollIntervalGeneric — confirmed nodes are never subject to the
// likely-dead backoff at all, and GetNodeHistory must not even be called
// for them.
func TestPollIntervalConfirmedNodeUnaffectedByHistory(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertConfirmedNode(ctx, "confirmed-dead-history:1", []byte{0x05, 0x06}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	recordHistory(t, ctx, store, node.ID, false, false, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalGeneric {
		t.Errorf("pollInterval(confirmed, 3+ failures) = %v, want %v (confirmed nodes must be unaffected)", got, PollIntervalGeneric)
	}
}

// TestPollIntervalConfirmedPoolOwnedNodeUnaffectedByHistory covers
// scenario (f): a confirmed, pool-owned node still returns
// PollIntervalPoolOwned regardless of its history.
func TestPollIntervalConfirmedPoolOwnedNodeUnaffectedByHistory(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertConfirmedNode(ctx, "pool-owned-dead-history:1", []byte{0x07, 0x08}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.Tags = map[string]any{"pool_owned": true}
	recordHistory(t, ctx, store, node.ID, false, false, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalPoolOwned {
		t.Errorf("pollInterval(confirmed, pool-owned, 3+ failures) = %v, want %v", got, PollIntervalPoolOwned)
	}
}

// checkpointTolerance bounds how far off a checkpoint-schedule pollInterval
// result is allowed to be from its expected "time remaining until the
// checkpoint" value, to absorb test-execution wall-clock slop without
// weakening the assertion that this is genuinely an age-based countdown
// (not a flat interval).
const checkpointTolerance = 30 * time.Second

// TestPollIntervalEscalatingCheckpointForVeryNewNode covers scenario (a):
// a brand-new unconfirmed node (age ~3min since FirstSeen), with no
// history, gets a pollInterval landing at the ~15-minute-since-FirstSeen
// mark (NewNodeCheckpoint1) -- i.e. ~12 minutes from now, NOT a flat 15
// minutes.
func TestPollIntervalEscalatingCheckpointForVeryNewNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "checkpoint1:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-3 * time.Minute)

	c := New(Config{})
	c.Storage = store

	want := NewNodeCheckpoint1 - 3*time.Minute // ~12 minutes
	got := c.pollInterval(ctx, node)
	if diff := got - want; diff < -checkpointTolerance || diff > checkpointTolerance {
		t.Errorf("pollInterval(3min-old unconfirmed) = %v, want within %v of %v", got, checkpointTolerance, want)
	}
	if got >= PollIntervalUnconfirmed {
		t.Errorf("pollInterval(3min-old unconfirmed) = %v, want less than flat PollIntervalUnconfirmed = %v", got, PollIntervalUnconfirmed)
	}
}

// TestPollIntervalEscalatingCheckpointForModeratelyNewNode covers scenario
// (b): an unconfirmed node aged ~20min since FirstSeen, with no/
// insufficient history, gets a pollInterval landing at the
// ~30-minute-since-FirstSeen mark (NewNodeCheckpoint2) -- i.e. ~10 minutes
// from now.
func TestPollIntervalEscalatingCheckpointForModeratelyNewNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "checkpoint2:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-20 * time.Minute)

	c := New(Config{})
	c.Storage = store

	want := NewNodeCheckpoint2 - 20*time.Minute // ~10 minutes
	got := c.pollInterval(ctx, node)
	if diff := got - want; diff < -checkpointTolerance || diff > checkpointTolerance {
		t.Errorf("pollInterval(20min-old unconfirmed) = %v, want within %v of %v", got, checkpointTolerance, want)
	}
}

// TestPollIntervalPastAllCheckpointsFallsBackToFlatUnconfirmed covers
// scenario (c): a node aged past all 3 checkpoints (>= 60min since
// FirstSeen) with fewer than 3 history entries falls through to the
// existing flat PollIntervalUnconfirmed steady-state, not another
// checkpoint calculation and not likely-dead.
func TestPollIntervalPastAllCheckpointsFallsBackToFlatUnconfirmed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "past-checkpoints-unconfirmed:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, node.ID, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalUnconfirmed {
		t.Errorf("pollInterval(past checkpoints, 1 failure) = %v, want %v", got, PollIntervalUnconfirmed)
	}
}

// TestPollIntervalPastAllCheckpointsFallsBackToLikelyDead covers scenario
// (d): a node aged past all 3 checkpoints (>= 60min since FirstSeen) with
// 3+ failed history entries and zero successes returns
// PollIntervalLikelyDead -- confirming the prior commit's behavior still
// works correctly once gated behind "past all 3 checkpoints".
func TestPollIntervalPastAllCheckpointsFallsBackToLikelyDead(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "past-checkpoints-likely-dead:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-90 * time.Minute)
	recordHistory(t, ctx, store, node.ID, false, false, false)

	c := New(Config{})
	c.Storage = store

	if got := c.pollInterval(ctx, node); got != PollIntervalLikelyDead {
		t.Errorf("pollInterval(past checkpoints, 3+ failures) = %v, want %v", got, PollIntervalLikelyDead)
	}
}

// TestPollIntervalCheckpointPrecedesLikelyDeadForVeryNewNode covers
// scenario (e) and is a deliberate, confirmed precedence check: a node
// well within its first checkpoint window (age ~10min since FirstSeen,
// under NewNodeCheckpoint1's 15min) that ALREADY has 3+ failed history
// entries (e.g. from aggressive early polling) must still get the
// checkpoint-based duration, NOT PollIntervalLikelyDead. The age-based
// checkpoint schedule takes priority over collectorLikelyDead for a
// node's entire first hour -- collectorLikelyDead is simply never
// consulted (GetNodeHistory is never even called) until age >=
// NewNodeCheckpoint3. This is intentional per pollInterval's doc comment,
// not an oversight.
func TestPollIntervalCheckpointPrecedesLikelyDeadForVeryNewNode(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "very-new-but-failing:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	node.FirstSeen = time.Now().Add(-10 * time.Minute)
	recordHistory(t, ctx, store, node.ID, false, false, false)

	c := New(Config{})
	c.Storage = store

	want := NewNodeCheckpoint1 - 10*time.Minute // ~5 minutes
	got := c.pollInterval(ctx, node)
	if got == PollIntervalLikelyDead {
		t.Fatalf("pollInterval(10min-old unconfirmed, 3+ failures) = %v (PollIntervalLikelyDead), want checkpoint-based duration -- age-based checkpoints must take precedence over likely-dead for the first hour", got)
	}
	if diff := got - want; diff < -checkpointTolerance || diff > checkpointTolerance {
		t.Errorf("pollInterval(10min-old unconfirmed, 3+ failures) = %v, want within %v of %v", got, checkpointTolerance, want)
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

// slowPeersFastInfoClient is a NodeClient fixture whose GetPeers blocks
// until either its unblock channel is closed or ctx is cancelled — used
// to simulate Discover()'s real-mainnet peer-walk hanging for a long
// time — while GetInfo (used by Poll) returns immediately from an
// in-memory fixture, same as fakeClient. peersCalled is closed the first
// time GetPeers is entered (before it blocks), so a test can synchronize
// on "discovery has started and is now hung" without a race. getPeersDone
// is set (atomically) after GetPeers actually returns, so a test can
// assert Poll produced data *before* the slow Discover call unblocked —
// i.e. that Poll genuinely wasn't waiting on it.
type slowPeersFastInfoClient struct {
	unblock         chan struct{}
	peersCalled     chan struct{}
	peersCalledOnce sync.Once
	getPeersDone    atomic.Bool

	info map[string]NodeInfo
}

// GetPeers may now legitimately be called concurrently by more than one
// caller (the general Discover loop AND the independent DiscoverOwned
// loop both walk Config.SeedNodes — see DiscoverOwned's doc comment),
// so peersCalled is only ever closed once, via sync.Once, rather than
// panicking on a second close.
func (s *slowPeersFastInfoClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	s.peersCalledOnce.Do(func() { close(s.peersCalled) })
	defer s.getPeersDone.Store(true)
	select {
	case <-s.unblock:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *slowPeersFastInfoClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	info, ok := s.info[addr]
	if !ok {
		return NodeInfo{}, fmt.Errorf("collector_test: no fixture GetInfo response for %s", addr)
	}
	return info, nil
}

// TestRunDoesNotStarvePollOnSlowDiscover is the core regression test for
// the Discover-vs-Poll starvation bug: previously, Run() executed
// Discover() and Poll() sequentially within a single runPass() gated by
// one shared ticker, so a Discover() call that hangs (as the real
// mainnet peer-walk can, given real network dials with multi-second
// timeouts across a large/slow-to-respond graph) starved Poll()
// entirely — no health checks would ever be recorded. This test uses a
// NodeClient whose GetPeers blocks indefinitely (until the test
// unblocks it) as Discover's transport, while a separate, already-due
// node is polled via that same client's fast, in-memory GetInfo. It
// asserts a health check is recorded for the due node — proving Poll
// ran and completed — while Discover's GetPeers call is still
// (verifiably) in flight, i.e. before Discover's first pass could
// possibly have returned.
func TestRunDoesNotStarvePollOnSlowDiscover(t *testing.T) {
	store := newTestStore(t)
	seedCtx := context.Background()

	client := &slowPeersFastInfoClient{
		unblock:     make(chan struct{}),
		peersCalled: make(chan struct{}),
		info: map[string]NodeInfo{
			"node:1": {Reachable: true},
		},
	}
	// Ensures GetPeers (and therefore Run's Discover goroutine) actually
	// unblocks/exits at the end of the test, even on failure, rather than
	// leaking a goroutine blocked forever on a channel nothing else will
	// ever close.
	t.Cleanup(func() { close(client.unblock) })

	// A node that's already due for a poll (no prior nextPoll entry —
	// due() treats that as immediately due).
	node, err := store.UpsertDiscoveredNode(seedCtx, "node:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	c := New(Config{SeedNodes: []string{"seed:1"}})
	c.Storage = store
	c.GRPCClient = client
	c.TickInterval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	// Wait for Discover's walk to actually reach (and hang in) GetPeers,
	// so we know it's genuinely in flight for the rest of the test rather
	// than, say, not having started yet.
	select {
	case <-client.peersCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Discover's GetPeers was never called")
	}

	// Poll ticks independently of the hung Discover call; poll for a
	// recorded health check on node:1 within a deadline that comfortably
	// exceeds several TickIntervals, but is short enough that this test
	// completes quickly if (and only if) the fix is in place.
	deadline := time.Now().Add(3 * time.Second)
	var history []storage.HealthCheck
	for {
		history, err = store.GetNodeHistory(seedCtx, node.ID, 10)
		if err != nil {
			t.Fatalf("get history: %v", err)
		}
		if len(history) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no health check recorded for node:1 while Discover's GetPeers was still hung — Poll appears starved by Discover")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The whole point: confirm the slow GetPeers call genuinely had not
	// returned yet when Poll's result showed up above. If this were
	// true, the test above wouldn't actually be proving independence —
	// it'd just mean Discover happened to finish fast.
	if client.getPeersDone.Load() {
		t.Fatal("GetPeers had already returned by the time Poll recorded a health check — test doesn't prove independence")
	}

	if history[0].ProbeSource != storage.ProbeSourceGRPC {
		t.Errorf("probe_source = %q, want %q", history[0].ProbeSource, storage.ProbeSourceGRPC)
	}
	if !history[0].Reachable {
		t.Errorf("expected reachable = true")
	}

	// Cancel ctx so both loops exit — GetPeers observes ctx.Done() and
	// returns rather than staying hung — and confirm Run actually returns
	// promptly and without error. (client.unblock is closed separately by
	// t.Cleanup, only as a belt-and-suspenders safety net in case this
	// assertion fails before reaching here.)
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// TestPollConfirmedOnlyTouchesConfirmedNodes verifies the core
// confirmed/unconfirmed priority-queue split: PollConfirmed only ever
// dials/records confirmed nodes (Node.PublicKey != nil), and
// PollUnconfirmed only ever dials/records unconfirmed placeholder nodes
// (Node.PublicKey == nil) -- neither function's node set ever includes
// the other's.
//
// unconfirmedNode is seeded with one pre-existing (failed) health check
// so it qualifies for PollUnconfirmed's HasHealthChecks: true half of
// its filter (see PollUnconfirmed's doc comment) -- a zero-history
// unconfirmed node belongs to PollNeverContacted instead, covered
// separately by TestPollNeverContactedOnlyTouchesNeverContactedNodes.
func TestPollConfirmedOnlyTouchesConfirmedNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	confirmedHeight := int64(111)
	unconfirmedHeight := int64(222)
	client := &fakeClient{info: map[string]NodeInfo{
		"confirmed:1":   {Reachable: true, Height: &confirmedHeight},
		"unconfirmed:1": {Reachable: true, Height: &unconfirmedHeight},
	}}

	confirmedNode, err := store.UpsertConfirmedNode(ctx, "confirmed:1", []byte{0x01, 0x02}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed confirmed node: %v", err)
	}
	unconfirmedNode, err := store.UpsertDiscoveredNode(ctx, "unconfirmed:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed unconfirmed node: %v", err)
	}
	recordHistory(t, ctx, store, unconfirmedNode.ID, false)

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.PollConfirmed(ctx); err != nil {
		t.Fatalf("poll confirmed: %v", err)
	}

	confirmedHistory, err := store.GetNodeHistory(ctx, confirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get confirmed history: %v", err)
	}
	if len(confirmedHistory) != 1 {
		t.Fatalf("len(confirmed history) after PollConfirmed = %d, want 1", len(confirmedHistory))
	}

	unconfirmedHistory, err := store.GetNodeHistory(ctx, unconfirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get unconfirmed history: %v", err)
	}
	if len(unconfirmedHistory) != 1 {
		t.Fatalf("len(unconfirmed history) after PollConfirmed = %d, want still 1 (PollConfirmed must never touch unconfirmed nodes)", len(unconfirmedHistory))
	}

	// PollUnconfirmed must now touch only the unconfirmed node, leaving
	// the confirmed node's history exactly as PollConfirmed left it.
	if err := c.PollUnconfirmed(ctx); err != nil {
		t.Fatalf("poll unconfirmed: %v", err)
	}

	confirmedHistory, err = store.GetNodeHistory(ctx, confirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get confirmed history after PollUnconfirmed: %v", err)
	}
	if len(confirmedHistory) != 1 {
		t.Fatalf("len(confirmed history) after PollUnconfirmed = %d, want still 1 (PollUnconfirmed must never touch confirmed nodes)", len(confirmedHistory))
	}

	unconfirmedHistory, err = store.GetNodeHistory(ctx, unconfirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get unconfirmed history after PollUnconfirmed: %v", err)
	}
	if len(unconfirmedHistory) != 2 {
		t.Fatalf("len(unconfirmed history) after PollUnconfirmed = %d, want 2 (1 pre-seeded + 1 new)", len(unconfirmedHistory))
	}
}

// TestPollNeverContactedOnlyTouchesNeverContactedNodes verifies the
// three-way confirmed/unconfirmed-with-history/never-contacted
// poll-queue split (see PollNeverContacted's doc comment): PollNeverContacted
// only ever dials/records nodes with ZERO recorded health checks at all,
// and (immediately after, on the same shared Collector -- exactly as
// Run's independent tickers would in production) PollUnconfirmed does
// not re-poll the node PollNeverContacted just gave its first probe to,
// because that node's due() cooldown (set from its escalating checkpoint
// pollInterval) hasn't elapsed yet -- proving the "no double-poll"
// requirement holds via the combination of the tightened query filters
// AND the shared due()/nextPoll cooldown, exactly as it does in
// production where all three loops share one Collector.
func TestPollNeverContactedOnlyTouchesNeverContactedNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{info: map[string]NodeInfo{
		"never-contacted:1":   {Reachable: true},
		"with-history:1":      {Reachable: true},
		"confirmed-no-poll:1": {Reachable: true},
	}}

	// Confirmed via UpsertConfirmedNode directly (bypassing PollOnce, as
	// every other test in this file does to seed a confirmed node) --
	// this deliberately leaves it with ZERO node_health rows, so it
	// exercises the Confirmed: false half of PollNeverContacted's
	// filter specifically: without that clause, a confirmed node with
	// no history (a real if unusual state -- e.g. right after a
	// pubkey-only migration/import) would incorrectly be swept up by
	// PollNeverContacted too.
	confirmedNode, err := store.UpsertConfirmedNode(ctx, "confirmed-no-poll:1", []byte{0x01, 0x02}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed confirmed node: %v", err)
	}

	withHistoryNode, err := store.UpsertDiscoveredNode(ctx, "with-history:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed with-history node: %v", err)
	}
	recordHistory(t, ctx, store, withHistoryNode.ID, false)

	neverContactedNode, err := store.UpsertDiscoveredNode(ctx, "never-contacted:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed never-contacted node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.PollNeverContacted(ctx); err != nil {
		t.Fatalf("poll never-contacted: %v", err)
	}

	neverContactedHistory, err := store.GetNodeHistory(ctx, neverContactedNode.ID, 10)
	if err != nil {
		t.Fatalf("get never-contacted history: %v", err)
	}
	if len(neverContactedHistory) != 1 {
		t.Fatalf("len(never-contacted history) after PollNeverContacted = %d, want 1", len(neverContactedHistory))
	}

	withHistoryHistory, err := store.GetNodeHistory(ctx, withHistoryNode.ID, 10)
	if err != nil {
		t.Fatalf("get with-history history: %v", err)
	}
	if len(withHistoryHistory) != 1 {
		t.Fatalf("len(with-history history) after PollNeverContacted = %d, want still 1 (PollNeverContacted must never touch nodes that already have history)", len(withHistoryHistory))
	}

	confirmedHistory, err := store.GetNodeHistory(ctx, confirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get confirmed history: %v", err)
	}
	if len(confirmedHistory) != 0 {
		t.Fatalf("len(confirmed history) after PollNeverContacted = %d, want still 0 (PollNeverContacted must never touch confirmed nodes, even ones with zero history)", len(confirmedHistory))
	}

	// PollUnconfirmed must now touch only withHistoryNode: it was
	// already eligible before this call, while neverContactedNode --
	// though it now technically also has >= 1 history row after the
	// call above -- is still cooling down under the pollInterval
	// due()/nextPoll entry PollNeverContacted just set for it, so it is
	// correctly NOT re-polled here.
	if err := c.PollUnconfirmed(ctx); err != nil {
		t.Fatalf("poll unconfirmed: %v", err)
	}

	withHistoryHistory, err = store.GetNodeHistory(ctx, withHistoryNode.ID, 10)
	if err != nil {
		t.Fatalf("get with-history history after PollUnconfirmed: %v", err)
	}
	if len(withHistoryHistory) != 2 {
		t.Fatalf("len(with-history history) after PollUnconfirmed = %d, want 2 (1 pre-seeded + 1 new)", len(withHistoryHistory))
	}

	neverContactedHistory, err = store.GetNodeHistory(ctx, neverContactedNode.ID, 10)
	if err != nil {
		t.Fatalf("get never-contacted history after PollUnconfirmed: %v", err)
	}
	if len(neverContactedHistory) != 1 {
		t.Fatalf("len(never-contacted history) after PollUnconfirmed = %d, want still 1 (not double-polled)", len(neverContactedHistory))
	}

	confirmedHistory, err = store.GetNodeHistory(ctx, confirmedNode.ID, 10)
	if err != nil {
		t.Fatalf("get confirmed history after PollUnconfirmed: %v", err)
	}
	if len(confirmedHistory) != 0 {
		t.Fatalf("len(confirmed history) after PollUnconfirmed = %d, want still 0", len(confirmedHistory))
	}
}

// slowInfoClient is a NodeClient fixture whose GetInfo blocks (until
// either unblock is closed or ctx is cancelled) when called for
// slowAddr specifically, while returning immediately from an in-memory
// fixture for every other address -- used to simulate a large/slow
// unconfirmed-node population's real network dials hanging, without
// affecting a confirmed node's fast poll. GetPeers always returns
// immediately with no peers, since these tests don't exercise discovery.
// infoCalled is closed the first time GetInfo is entered for slowAddr
// (before it blocks), and infoDone is set (atomically) after that call
// actually returns, mirroring slowPeersFastInfoClient's synchronization
// pattern above but for GetInfo instead of GetPeers.
type slowInfoClient struct {
	slowAddr string
	unblock  chan struct{}

	infoCalled     chan struct{}
	infoCalledOnce sync.Once
	infoDone       atomic.Bool

	info map[string]NodeInfo
}

func (s *slowInfoClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	return nil, nil
}

func (s *slowInfoClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	if addr == s.slowAddr {
		s.infoCalledOnce.Do(func() { close(s.infoCalled) })
		defer s.infoDone.Store(true)
		select {
		case <-s.unblock:
		case <-ctx.Done():
			return NodeInfo{}, ctx.Err()
		}
	}
	info, ok := s.info[addr]
	if !ok {
		return NodeInfo{}, fmt.Errorf("collector_test: no fixture GetInfo response for %s", addr)
	}
	return info, nil
}

// TestPollConfirmedNotStarvedBySlowUnconfirmedPoll is the poll-loop
// analogue of TestRunDoesNotStarvePollOnSlowDiscover: it proves that a
// slow (simulating a large/backlogged) unconfirmed-node poll cannot
// delay or starve the confirmed loop's poll attempts, since the two run
// on fully independent tickers/goroutines (see Run). A slow-blocking
// GetInfo is used for the unconfirmed node; the confirmed node's GetInfo
// returns immediately from an in-memory fixture. Both loops run
// concurrently via c.Run, and the test asserts a health check is
// recorded for the confirmed node while the unconfirmed node's GetInfo
// call is still (verifiably) in flight.
func TestPollConfirmedNotStarvedBySlowUnconfirmedPoll(t *testing.T) {
	store := newTestStore(t)
	seedCtx := context.Background()

	client := &slowInfoClient{
		slowAddr:   "unconfirmed:1",
		unblock:    make(chan struct{}),
		infoCalled: make(chan struct{}),
		info: map[string]NodeInfo{
			"confirmed:1": {Reachable: true},
		},
	}
	// Ensures the slow GetInfo call actually unblocks/returns at the end
	// of the test, even on failure, rather than leaking a goroutine
	// blocked forever on a channel nothing else will ever close.
	t.Cleanup(func() { close(client.unblock) })

	confirmedNode, err := store.UpsertConfirmedNode(seedCtx, "confirmed:1", []byte{0x01, 0x02}, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("seed confirmed node: %v", err)
	}
	unconfirmedNode, err := store.UpsertDiscoveredNode(seedCtx, "unconfirmed:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed unconfirmed node: %v", err)
	}
	// Pre-seed one health check so this node is picked up by
	// PollUnconfirmed specifically (HasHealthChecks: true) rather than
	// PollNeverContacted (HasHealthChecks: false) -- this test's whole
	// point is proving PollConfirmed's independence from a slow
	// PollUnconfirmed pass specifically; TestPollNeverContactedNotStarvedBySlowUnconfirmedPoll
	// and TestRunDoesNotStarvePollOnSlowDiscover cover the other two
	// loops' independence.
	recordHistory(t, seedCtx, store, unconfirmedNode.ID, false)

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client
	c.TickInterval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	// Wait for the unconfirmed loop's poll to actually reach (and hang
	// in) GetInfo, so we know it's genuinely in flight for the rest of
	// the test rather than, say, not having started yet.
	select {
	case <-client.infoCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("unconfirmed node's GetInfo was never called")
	}

	// The confirmed loop ticks independently of the hung unconfirmed
	// loop; poll for a recorded health check on the confirmed node
	// within a deadline that comfortably exceeds several TickIntervals.
	deadline := time.Now().Add(3 * time.Second)
	var history []storage.HealthCheck
	for {
		history, err = store.GetNodeHistory(seedCtx, confirmedNode.ID, 10)
		if err != nil {
			t.Fatalf("get history: %v", err)
		}
		if len(history) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no health check recorded for the confirmed node while the unconfirmed node's GetInfo was still hung -- PollConfirmed appears starved by PollUnconfirmed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The whole point: confirm the slow GetInfo call genuinely had not
	// returned yet when PollConfirmed's result showed up above.
	if client.infoDone.Load() {
		t.Fatal("unconfirmed GetInfo had already returned by the time PollConfirmed recorded a health check -- test doesn't prove independence")
	}

	if !history[0].Reachable {
		t.Errorf("expected reachable = true")
	}

	// Cancel ctx so all loops exit -- the hung GetInfo call observes
	// ctx.Done() and returns rather than staying hung forever -- and
	// confirm Run actually returns promptly and without error.
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// TestConcurrentPollLoopsNoDataRace runs PollConfirmed, PollUnconfirmed,
// and PollNeverContacted concurrently and repeatedly against the same
// Collector's shared nextPoll/mu state and the same underlying Storage.
// It makes no assertion beyond "no error" -- its entire purpose is to
// give `go test -race` real concurrent access to c.nextPoll (guarded by
// c.mu) from all three poll loops at once, proving that running them
// concurrently (as Run does) introduces no data race -- including the
// worker-pool concurrency WITHIN each individual poll() call (see
// TestPollBoundedConcurrency for that dimension in isolation).
func TestConcurrentPollLoopsNoDataRace(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	client := &fakeClient{info: map[string]NodeInfo{}}
	for i := 0; i < 20; i++ {
		addr := fmt.Sprintf("confirmed:%d", i)
		client.info[addr] = NodeInfo{Reachable: true}
		if _, err := store.UpsertConfirmedNode(ctx, addr, []byte{byte(i), byte(i + 1)}, storage.DiscoverySourceP2P); err != nil {
			t.Fatalf("seed confirmed node %d: %v", i, err)
		}
	}
	for i := 0; i < 20; i++ {
		addr := fmt.Sprintf("unconfirmed:%d", i)
		client.info[addr] = NodeInfo{Reachable: true}
		n, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("seed unconfirmed node %d: %v", i, err)
		}
		// Pre-seed one health check so this node qualifies for
		// PollUnconfirmed's HasHealthChecks: true filter (see its doc
		// comment) -- a zero-history node belongs to
		// PollNeverContacted instead, exercised separately below.
		recordHistory(t, ctx, store, n.ID, true)
	}
	for i := 0; i < 20; i++ {
		addr := fmt.Sprintf("never-contacted:%d", i)
		client.info[addr] = NodeInfo{Reachable: true}
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("seed never-contacted node %d: %v", i, err)
		}
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	var wg sync.WaitGroup
	wg.Add(3)
	errCh := make(chan error, 3)

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := c.PollConfirmed(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := c.PollUnconfirmed(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := c.PollNeverContacted(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent poll error: %v", err)
	}
}

// concurrencyTrackingClient is a NodeClient fixture whose GetInfo tracks
// the number of concurrently in-flight calls via atomic counters,
// recording the highest ("peak") value ever observed, and sleeps
// briefly on every call so that concurrent callers genuinely overlap in
// wall-clock time -- otherwise a bug that removed all concurrency could
// still show a misleadingly low peak just because calls happened to be
// fast enough not to overlap. Used by TestPollBoundedConcurrency to
// prove poll()'s worker pool never exceeds maxPollWorkers in-flight
// PollOnce calls, even when many more nodes than that are due in a
// single pass.
type concurrencyTrackingClient struct {
	current int64
	peak    int64
}

func (c *concurrencyTrackingClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	return nil, nil
}

func (c *concurrencyTrackingClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	cur := atomic.AddInt64(&c.current, 1)
	defer atomic.AddInt64(&c.current, -1)

	for {
		peak := atomic.LoadInt64(&c.peak)
		if cur <= peak {
			break
		}
		if atomic.CompareAndSwapInt64(&c.peak, peak, cur) {
			break
		}
	}

	// Long enough that, with true bounded concurrency (up to
	// maxPollWorkers in flight), a batch well in excess of
	// maxPollWorkers has no realistic way to complete without multiple
	// workers genuinely overlapping in time -- proving the peak
	// reflects real concurrency, not just a bookkeeping race won by
	// sheer luck.
	time.Sleep(20 * time.Millisecond)

	return NodeInfo{Reachable: true}, nil
}

// TestPollBoundedConcurrency is the core regression/proof test for Part
// 1 of the collector-concurrency-brief: poll() (shared by PollConfirmed/
// PollUnconfirmed/PollNeverContacted) must never have more than
// maxPollWorkers (250) PollOnce calls in flight at once, no matter how
// many more nodes than that are simultaneously due. It seeds
// numNodesDue (600, comfortably more than double maxPollWorkers) never-
// contacted nodes -- all due immediately, since none has ever been
// polled -- and asserts the concurrencyTrackingClient's observed peak
// concurrent GetInfo call count is both (a) greater than 1 (proving the
// pass is genuinely running concurrently at all, not accidentally back
// to sequential) and (b) never more than maxPollWorkers.
func TestPollBoundedConcurrency(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const numNodesDue = 600 // comfortably more than 2x maxPollWorkers (250)
	for i := 0; i < numNodesDue; i++ {
		addr := fmt.Sprintf("bounded-concurrency:%d", i)
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("seed node %d: %v", i, err)
		}
	}

	client := &concurrencyTrackingClient{}
	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client

	if err := c.PollNeverContacted(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	peak := atomic.LoadInt64(&client.peak)
	if peak > int64(maxPollWorkers) {
		t.Fatalf("peak concurrent PollOnce calls = %d, want <= maxPollWorkers (%d)", peak, maxPollWorkers)
	}
	if peak <= 1 {
		t.Fatalf("peak concurrent PollOnce calls = %d, want > 1 -- the pass does not appear to have run concurrently at all, so this test cannot be proving the concurrency limit is actually being exercised", peak)
	}
}

// TestPollNeverContactedNotStarvedBySlowUnconfirmedPoll mirrors
// TestPollConfirmedNotStarvedBySlowUnconfirmedPoll and
// TestRunDoesNotStarvePollOnSlowDiscover, but for the third loop: it
// proves that a slow (simulating a large/backlogged)
// unconfirmed-with-history poll cannot delay or starve the
// never-contacted loop's poll attempts, since PollNeverContacted runs
// on its own fully independent ticker/goroutine (see Run and
// runNeverContactedPollLoop's doc comments) -- exactly the "highest
// priority, walk the network more aggressively" property
// PollNeverContacted exists to guarantee.
func TestPollNeverContactedNotStarvedBySlowUnconfirmedPoll(t *testing.T) {
	store := newTestStore(t)
	seedCtx := context.Background()

	client := &slowInfoClient{
		slowAddr:   "unconfirmed-with-history:1",
		unblock:    make(chan struct{}),
		infoCalled: make(chan struct{}),
		info: map[string]NodeInfo{
			"never-contacted:1": {Reachable: true},
		},
	}
	// Ensures the slow GetInfo call actually unblocks/returns at the
	// end of the test, even on failure, rather than leaking a goroutine
	// blocked forever on a channel nothing else will ever close.
	t.Cleanup(func() { close(client.unblock) })

	withHistoryNode, err := store.UpsertDiscoveredNode(seedCtx, "unconfirmed-with-history:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed with-history node: %v", err)
	}
	recordHistory(t, seedCtx, store, withHistoryNode.ID, false)

	neverContactedNode, err := store.UpsertDiscoveredNode(seedCtx, "never-contacted:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("seed never-contacted node: %v", err)
	}

	c := New(Config{})
	c.Storage = store
	c.GRPCClient = client
	c.TickInterval = 20 * time.Millisecond
	c.UnconfirmedTickInterval = 20 * time.Millisecond
	c.NeverContactedTickInterval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	// Wait for the unconfirmed loop's poll to actually reach (and hang
	// in) GetInfo for the with-history node, so we know it's genuinely
	// in flight for the rest of the test rather than, say, not having
	// started yet.
	select {
	case <-client.infoCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("with-history node's GetInfo was never called")
	}

	// The never-contacted loop ticks independently of the hung
	// unconfirmed loop; poll for a recorded health check on the
	// never-contacted node within a deadline that comfortably exceeds
	// several TickIntervals.
	deadline := time.Now().Add(3 * time.Second)
	var history []storage.HealthCheck
	for {
		history, err = store.GetNodeHistory(seedCtx, neverContactedNode.ID, 10)
		if err != nil {
			t.Fatalf("get history: %v", err)
		}
		if len(history) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no health check recorded for the never-contacted node while the with-history node's GetInfo was still hung -- PollNeverContacted appears starved by PollUnconfirmed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The whole point: confirm the slow GetInfo call genuinely had not
	// returned yet when PollNeverContacted's result showed up above.
	if client.infoDone.Load() {
		t.Fatal("with-history node's GetInfo had already returned by the time PollNeverContacted recorded a health check -- test doesn't prove independence")
	}

	if !history[0].Reachable {
		t.Errorf("expected reachable = true")
	}
	// Cancel ctx so all loops exit -- the hung GetInfo call observes
	// ctx.Done() and returns rather than staying hung forever -- and
	// confirm Run actually returns promptly and without error.
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// slowAddrPeersClient is a NodeClient fixture whose GetPeers blocks
// (until unblock is closed or ctx is cancelled) ONLY for one configured
// address (slowAddr), returning immediately -- from an in-memory
// fixture -- for every other address. This lets a test simulate the
// general-population BFS hanging on one particular node while a
// SEPARATE owned/seed node's own GetPeers call stays fast, mirroring
// slowInfoClient's per-address-slow pattern but for GetPeers instead of
// GetInfo. slowCalled/slowCalledOnce/slowDone mirror
// slowPeersFastInfoClient/slowInfoClient's own synchronization fields
// exactly.
type slowAddrPeersClient struct {
	slowAddr string
	unblock  chan struct{}

	slowCalled     chan struct{}
	slowCalledOnce sync.Once
	slowDone       atomic.Bool

	peers map[string][]string
}

func (s *slowAddrPeersClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	if addr == s.slowAddr {
		s.slowCalledOnce.Do(func() { close(s.slowCalled) })
		defer s.slowDone.Store(true)
		select {
		case <-s.unblock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	addrs := s.peers[addr]
	peers := make([]DiscoveredPeer, len(addrs))
	for i, a := range addrs {
		peers[i] = DiscoveredPeer{Address: a}
	}
	return peers, nil
}

func (s *slowAddrPeersClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	return NodeInfo{Reachable: true}, nil
}

// TestDiscoverOwnedIndependentOfSlowGeneralDiscovery is the core
// regression/proof test for DiscoverOwned's whole reason for existing
// (see its doc comment): the general-population discoverWith BFS
// hanging on some non-owned, non-seed node must NOT delay or starve the
// independent owned-discovery loop's walk of a SEPARATE owned node.
//
// Config.SeedNodes is set to a single address ("hang:1") whose GetPeers
// call hangs -- this is deliberately the SEED itself (not a node
// reached via a second hop), so that the general BFS's very first dial
// is guaranteed to be the hang, with no race against whichever loop
// happens to reach it first: both runDiscoverLoop's general pass and
// runOwnedDiscoverLoop's pass (which also always walks Config.SeedNodes
// -- see DiscoverOwned's doc comment) will each independently call
// GetPeers("hang:1") and each independently block, in their own
// goroutines, since dueForDiscovery's cooldown for that address is
// never set while the call is hung (setNextDiscovery only runs AFTER
// GetPeers returns) -- so this can never flakily skip the hang in
// either loop.
//
// A separate node ("owned:1") is pre-tagged pool-owned (non-empty owner
// tag) and is NOT reachable from "hang:1"'s (nonexistent, since it
// never returns) peer list, so the general BFS never reaches it at all.
// The owned-discovery loop, via its own small bounded worker pool (see
// ownedDiscoveryWorkers), walks "owned:1" concurrently with -- and
// genuinely independently of -- its own stuck "hang:1" worker, and
// records "owned:1"'s reported peer ("owned-peer:1") into storage well
// within a bounded deadline, while the hang is still (verifiably) in
// flight.
func TestDiscoverOwnedIndependentOfSlowGeneralDiscovery(t *testing.T) {
	store := newTestStore(t)
	seedCtx := context.Background()

	client := &slowAddrPeersClient{
		slowAddr:   "hang:1",
		unblock:    make(chan struct{}),
		slowCalled: make(chan struct{}),
		peers: map[string][]string{
			"owned:1": {"owned-peer:1"},
		},
	}
	// Ensures both stuck GetPeers("hang:1") calls (general AND owned
	// loop) actually unblock/exit at the end of the test, even on
	// failure, rather than leaking goroutines blocked forever on a
	// channel nothing else will ever close.
	t.Cleanup(func() { close(client.unblock) })

	if _, err := store.UpsertDiscoveredNode(seedCtx, "owned:1", storage.DiscoverySourceP2P, map[string]any{"owner": "pool-ops"}, nil); err != nil {
		t.Fatalf("seed owned node: %v", err)
	}

	c := New(Config{SeedNodes: []string{"hang:1"}})
	c.Storage = store
	c.GRPCClient = client
	c.TickInterval = 20 * time.Millisecond
	c.OwnedDiscoveryTickInterval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	// Wait for GetPeers("hang:1") to actually be reached (and hang), so
	// we know the general BFS (and/or the owned loop's own walk of the
	// same seed address) is genuinely in flight for the rest of the
	// test rather than, say, not having started yet.
	select {
	case <-client.slowCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("GetPeers(hang:1) was never called")
	}

	// The owned-discovery loop ticks independently of the hung general
	// BFS; poll storage for owned:1's reported peer to show up within a
	// deadline that comfortably exceeds several tick intervals.
	deadline := time.Now().Add(3 * time.Second)
	for {
		nodes, err := store.ListNodes(seedCtx, storage.NodeFilter{})
		if err != nil {
			t.Fatalf("list nodes: %v", err)
		}
		found := false
		for _, n := range nodes {
			if n.Address == "owned-peer:1" {
				found = true
				break
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned-peer:1 was never discovered while GetPeers(hang:1) was still hung -- the owned-discovery loop appears starved by the general BFS")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The whole point: confirm the hung GetPeers(hang:1) call genuinely
	// had not returned yet when owned-peer:1 showed up above. If this
	// were false, the test above wouldn't actually be proving
	// independence -- it'd just mean the hang happened to resolve fast.
	if client.slowDone.Load() {
		t.Fatal("GetPeers(hang:1) had already returned by the time owned-peer:1 was discovered -- test doesn't prove independence")
	}

	// Cancel ctx so every loop exits -- the hung GetPeers calls observe
	// ctx.Done() and return rather than staying hung forever -- and
	// confirm Run actually returns promptly and without error.
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// wideBranchingClient is a NodeClient fixture whose GetPeers generates
// an artificially large reachable graph: every address gets `branching`
// children (addr + "/0", addr + "/1", ...) up to maxDepth hops from the
// root, and every call sleeps for `delay` first. This lets a test
// construct a discoverWith BFS queue that is provably impossible to
// fully drain within a short deadline (branching^maxDepth nodes, each
// costing at least `delay` sequentially, since discoverWith's BFS is
// single-threaded/non-concurrent) without relying on hard-to-reproduce
// real-network timing or an indefinite hang.
type wideBranchingClient struct {
	branching int
	maxDepth  int
	delay     time.Duration
}

func (w *wideBranchingClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	if w.delay > 0 {
		select {
		case <-time.After(w.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if strings.Count(addr, "/") >= w.maxDepth {
		return nil, nil
	}
	peers := make([]DiscoveredPeer, w.branching)
	for i := 0; i < w.branching; i++ {
		peers[i] = DiscoveredPeer{Address: fmt.Sprintf("%s/%d", addr, i)}
	}
	return peers, nil
}

func (w *wideBranchingClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	return NodeInfo{Reachable: true}, nil
}

// TestDiscoverWithRespectsContextDeadline is the core regression/proof
// test for Fix part (b) of the discovery-starvation fix: discoverWith's
// BFS must stop cleanly, without hanging or erroring, once its ctx is
// exceeded mid-walk -- and must genuinely leave nodes unwalked, proving
// the deadline actually cut the pass short rather than the graph simply
// being small enough to finish anyway.
//
// discoverWith derives its own internal DiscoveryPassDeadline-based
// sub-context from whatever ctx is passed in (see its doc comment), and
// context.WithTimeout always takes the EARLIER of the parent's deadline
// and its own new one -- so passing Discover(ctx) a ctx with a much
// shorter deadline than DiscoveryPassDeadline (4 minutes) exercises the
// exact same ctx.Done() short-circuit path a real DiscoveryPassDeadline
// expiry would, without needing to wait 4 minutes or mutate the
// production constant. wideBranchingClient's graph (branching=4,
// maxDepth=6, 5461 nodes total if fully drained) combined with a 5ms
// per-call delay and a 200ms deadline makes full drainage take upwards
// of 27 seconds sequentially -- so the graph cannot possibly finish
// within 200ms, comfortably proving the cutoff is doing real work.
func TestDiscoverWithRespectsContextDeadline(t *testing.T) {
	store := newTestStore(t)

	client := &wideBranchingClient{branching: 4, maxDepth: 6, delay: 5 * time.Millisecond}

	c := New(Config{SeedNodes: []string{"root"}})
	c.Storage = store
	c.GRPCClient = client

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Discover's own return value must stay nil -- the deadline being
	// hit mid-walk is logged, not surfaced as an error.
	if err := c.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// Total reachable nodes if the BFS were to fully drain: sum of
	// branching^depth for depth in [0, maxDepth].
	total := 0
	n := 1
	for d := 0; d <= client.maxDepth; d++ {
		total += n
		n *= client.branching
	}

	nodes, err := store.ListNodes(context.Background(), storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatal("expected at least the root node to have been upserted before the deadline hit")
	}
	if len(nodes) >= total {
		t.Fatalf("len(nodes) = %d, want < %d (the full graph) -- the context deadline should have cut the walk short before it could fully drain", len(nodes), total)
	}
}
