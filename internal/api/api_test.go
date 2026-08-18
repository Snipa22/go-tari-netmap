package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-netmap/internal/api"
	"github.com/Snipa22/go-tari-netmap/internal/collector"
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
// the test to serialize access with the storage and collector packages'
// tests against the same shared database.
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

// fakeClient is an in-memory fixture collector.NodeClient used to test the
// async health-check kickoff triggered by POST /nodes, without any real
// network access.
type fakeClient struct {
	info map[string]collector.NodeInfo

	// delay, if non-zero, is slept before GetInfo returns — used to
	// assert that the submission probe/health-check kickoff is truly
	// async and never blocks the triggering HTTP response.
	delay time.Duration
}

func (f *fakeClient) GetPeers(ctx context.Context, addr string) ([]collector.DiscoveredPeer, error) {
	return nil, nil
}

func (f *fakeClient) GetInfo(ctx context.Context, addr string) (collector.NodeInfo, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	info, ok := f.info[addr]
	if !ok {
		return collector.NodeInfo{}, fmt.Errorf("api_test: no fixture GetInfo response for %s", addr)
	}
	return info, nil
}

func newTestServer(t *testing.T, client collector.NodeClient) (*httptest.Server, storage.Store) {
	t.Helper()
	store := newTestStore(t)
	if client == nil {
		client = collector.NewStubClient()
	}
	// p2pClient is nil here: these tests only exercise the gRPC-labeled
	// async health-check kickoff path; dual-probe behavior is covered by
	// internal/collector's own tests.
	srv := httptest.NewServer(api.NewRouter(store, client, nil))
	t.Cleanup(srv.Close)
	return srv, store
}

// strPtr is a small helper for building *string literals inline in test
// assertions/fixtures.
func strPtr(s string) *string { return &s }

func TestHealthz(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// TestCreateNodeValid asserts a genuinely new address queues a pending
// submission (202 Accepted) rather than instantly publishing a node: no
// node is created, and the response body reflects the pending
// submission.
func TestCreateNodeValid(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	body := `{"host":"1.2.3.4","port":18142,"label":"my node","owner_tag":"pool-x"}`
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	var submission storage.PendingSubmission
	if err := json.NewDecoder(resp.Body).Decode(&submission); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if submission.Address != "1.2.3.4:18142" {
		t.Errorf("address = %q, want %q", submission.Address, "1.2.3.4:18142")
	}
	if submission.Status != storage.SubmissionStatusPending {
		t.Errorf("status = %q, want %q", submission.Status, storage.SubmissionStatusPending)
	}
	if submission.Label == nil || *submission.Label != "my node" {
		t.Errorf("label = %v, want %q", submission.Label, "my node")
	}
	if submission.OwnerTag == nil || *submission.OwnerTag != "pool-x" {
		t.Errorf("owner_tag = %v, want %q", submission.OwnerTag, "pool-x")
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("len(nodes) = %d, want 0 (submission must not auto-publish a node)", len(nodes))
	}
}

// TestCreateNodeAlreadyOptedInConflicts asserts submitting an address
// that already belongs to a publicly opted-in node (registry_submitted
// or both) is rejected with 409, and the existing node's data is left
// completely unchanged — the whole point of this check is to prevent a
// second submitter from silently merging into (and overwriting) someone
// else's already-registered node.
func TestCreateNodeAlreadyOptedInConflicts(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	const addr = "23.226.69.178:18189"
	original, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceRegistry,
		map[string]any{"owner": "Jagtech"}, strPtr("Jagtech node"))
	if err != nil {
		t.Fatalf("upsert original node: %v", err)
	}
	originalLastSeen := original.LastSeen

	body := `{"host":"23.226.69.178","port":18189,"label":"steal this node","owner_tag":"attacker"}`
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}

	got, err := store.GetNode(ctx, original.ID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.Label == nil || *got.Label != "Jagtech node" {
		t.Errorf("label = %v, want unchanged %q", got.Label, "Jagtech node")
	}
	if owner, _ := got.Tags["owner"].(string); owner != "Jagtech" {
		t.Errorf("tags[owner] = %v, want unchanged %q", got.Tags["owner"], "Jagtech")
	}
	if !got.LastSeen.Equal(originalLastSeen) {
		t.Errorf("last_seen changed: got %v, want unchanged %v", got.LastSeen, originalLastSeen)
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1 (no new node created)", len(nodes))
	}

	pending, err := store.ListPendingSubmissions(ctx, "pending")
	if err != nil {
		t.Fatalf("list pending submissions: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("len(pending) = %d, want 0 (conflict must not queue a submission either)", len(pending))
	}
}

func TestCreateNodeInvalid(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	cases := []struct {
		name string
		body string
	}{
		{"missing host", `{"port":18142}`},
		{"port zero", `{"host":"1.2.3.4","port":0}`},
		{"port too large", `{"host":"1.2.3.4","port":65536}`},
		{"invalid json", `{not json`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatalf("POST /nodes: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestListNodesFilter(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	if _, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "b:2", storage.DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	resp, err := http.Get(srv.URL + "/nodes")
	if err != nil {
		t.Fatalf("GET /nodes: %v", err)
	}
	defer resp.Body.Close()
	var all struct {
		Nodes   []api.PublicNode `json:"nodes"`
		Total   int              `json:"total"`
		Limit   int              `json:"limit"`
		Offset  int              `json:"offset"`
		HasMore bool             `json:"has_more"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(all.Nodes) != 2 {
		t.Fatalf("len(all.Nodes) = %d, want 2", len(all.Nodes))
	}
	if all.Total != 2 {
		t.Fatalf("all.Total = %d, want 2", all.Total)
	}

	resp2, err := http.Get(srv.URL + "/nodes?discovery_source=p2p_discovered")
	if err != nil {
		t.Fatalf("GET /nodes?discovery_source=...: %v", err)
	}
	defer resp2.Body.Close()
	var filtered struct {
		Nodes []api.PublicNode `json:"nodes"`
		Total int              `json:"total"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// "a:1" is a p2p_discovered node, so its real address is scrubbed
	// from the response — assert on discovery_source instead of address.
	if len(filtered.Nodes) != 1 || filtered.Nodes[0].DiscoverySource != storage.DiscoverySourceP2P {
		t.Fatalf("filtered.Nodes = %+v, want just the p2p_discovered node", filtered.Nodes)
	}
	if filtered.Total != 1 {
		t.Fatalf("filtered.Total = %d, want 1", filtered.Total)
	}
}

func TestGetNode(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes/%s", srv.URL, node.ID))
	if err != nil {
		t.Fatalf("GET /nodes/{id}: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got api.PublicNode
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != node.ID {
		t.Errorf("id = %v, want %v", got.ID, node.ID)
	}
}

func TestGetNodeNotFound(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	resp, err := http.Get(fmt.Sprintf("%s/nodes/%s", srv.URL, uuid.New()))
	if err != nil {
		t.Fatalf("GET /nodes/{id}: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestGetNodeHistory(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := store.RecordHealthCheck(ctx, storage.HealthCheckInput{
		NodeID:      node.ID,
		Reachable:   true,
		ProbeSource: storage.ProbeSourceGRPC,
	}); err != nil {
		t.Fatalf("record health check: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes/%s/history?limit=5", srv.URL, node.ID))
	if err != nil {
		t.Fatalf("GET /nodes/{id}/history: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var history []storage.HealthCheck
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].NodeID != node.ID {
		t.Errorf("node_id = %v, want %v", history[0].NodeID, node.ID)
	}
}

// TestGetNodeEdges seeds a center node with a p2p_discovered neighbor and
// a registry_submitted neighbor, records real peer-edge observations
// between them, and asserts GET /nodes/{id}/edges returns exactly that
// neighborhood — mirroring TestScrubbingP2PVsRegistry's setup style so
// scrubbing behavior is actually exercised, not just the neighbor/edge
// set itself. The raw-body substring check for the p2p neighbor's
// address is the single most important assertion here — see
// TestScrubbingP2PVsRegistry's doc comment for why a decoded-struct-only
// check wouldn't be sufficient.
func TestGetNodeEdges(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	const centerAddr = "center:1"
	const p2pAddr = "1.2.3.4:18142"
	const registryAddr = "5.6.7.8:18142"

	center, err := store.UpsertDiscoveredNode(ctx, centerAddr, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert center: %v", err)
	}
	p2pNeighbor, err := store.UpsertDiscoveredNode(ctx, p2pAddr, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert p2p neighbor: %v", err)
	}
	registryNeighbor, err := store.UpsertDiscoveredNode(ctx, registryAddr, storage.DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert registry neighbor: %v", err)
	}
	// A third node, not connected to center at all, must NOT show up in
	// the response — asserts ListNodeEdges' WHERE clause (and this
	// handler's neighbor-collection logic) doesn't over-broadly include
	// unrelated nodes.
	unrelated, err := store.UpsertDiscoveredNode(ctx, "unrelated:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert unrelated: %v", err)
	}

	if err := store.RecordPeerEdgeObservation(ctx, center.ID, p2pNeighbor.ID); err != nil {
		t.Fatalf("record edge center->p2p: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, registryNeighbor.ID, center.ID); err != nil {
		t.Fatalf("record edge registry->center: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes/%s/edges", srv.URL, center.ID))
	if err != nil {
		t.Fatalf("GET /nodes/{id}/edges: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Edges) != 2 {
		t.Fatalf("len(edges) = %d, want 2:\n%s", len(got.Edges), body)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2:\n%s", len(got.Nodes), body)
	}

	gotNodeIDs := map[uuid.UUID]bool{}
	for _, n := range got.Nodes {
		gotNodeIDs[n.ID] = true
	}
	if !gotNodeIDs[p2pNeighbor.ID] {
		t.Errorf("nodes missing p2p neighbor %v:\n%s", p2pNeighbor.ID, body)
	}
	if !gotNodeIDs[registryNeighbor.ID] {
		t.Errorf("nodes missing registry neighbor %v:\n%s", registryNeighbor.ID, body)
	}
	if gotNodeIDs[unrelated.ID] {
		t.Errorf("nodes unexpectedly contains unrelated node %v:\n%s", unrelated.ID, body)
	}
	if gotNodeIDs[center.ID] {
		t.Errorf("nodes unexpectedly contains the center node itself %v:\n%s", center.ID, body)
	}

	// Privacy: the p2p neighbor's real address must never appear in the
	// raw response body, but the registry neighbor's (opted-in) address
	// must.
	rawBody := string(body)
	if strings.Contains(rawBody, p2pAddr) {
		t.Errorf("GET /nodes/%s/edges body contains p2p neighbor's address %q:\n%s", center.ID, p2pAddr, rawBody)
	}
	if !strings.Contains(rawBody, registryAddr) {
		t.Errorf("GET /nodes/%s/edges body missing registry neighbor's address %q:\n%s", center.ID, registryAddr, rawBody)
	}
}

// TestGetNodeEdgesEmpty asserts a node with zero real edges (or an id
// that doesn't correspond to any node at all) gets a 200 with empty
// nodes/edges slices, not a 404 — matching ListNodeEdges' own
// "empty, not an error" semantics rather than inventing a 404 path that
// doesn't match how the store method actually behaves.
func TestGetNodeEdgesEmpty(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	lonely, err := store.UpsertDiscoveredNode(ctx, "lonely:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert lonely: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes/%s/edges", srv.URL, lonely.ID))
	if err != nil {
		t.Fatalf("GET /nodes/{id}/edges: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 0 {
		t.Errorf("len(nodes) = %d, want 0", len(got.Nodes))
	}
	if len(got.Edges) != 0 {
		t.Errorf("len(edges) = %d, want 0", len(got.Edges))
	}

	// A node id with no corresponding node at all behaves the same way
	// (empty, not 404) — ListNodeEdges doesn't error on zero matching
	// rows regardless of whether the id belongs to a real node.
	unknownResp, err := http.Get(fmt.Sprintf("%s/nodes/%s/edges", srv.URL, uuid.New()))
	if err != nil {
		t.Fatalf("GET /nodes/{unknown}/edges: %v", err)
	}
	defer unknownResp.Body.Close()
	if unknownResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", unknownResp.StatusCode, http.StatusOK)
	}
}

// TestGetNodeEdgesInvalidLimit asserts `?limit=` on GET /nodes/{id}/edges
// rejects non-positive/non-numeric values with 400, mirroring
// handleGetNodeHistory's identical limit-parsing/validation pattern.
func TestGetNodeEdgesInvalidLimit(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	for _, v := range []string{"0", "-1", "not-a-number"} {
		resp, err := http.Get(fmt.Sprintf("%s/nodes/%s/edges?limit=%s", srv.URL, node.ID, v))
		if err != nil {
			t.Fatalf("GET /nodes/{id}/edges?limit=%s: %v", v, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("limit=%s: status = %d, want %d", v, resp.StatusCode, http.StatusBadRequest)
		}
	}
}

func TestTopology(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	a, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertDiscoveredNode(ctx, "b:2", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("record edge observation: %v", err)
	}

	resp, err := http.Get(srv.URL + "/topology")
	if err != nil {
		t.Fatalf("GET /topology: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 2 {
		t.Errorf("len(nodes) = %d, want 2", len(got.Nodes))
	}
	if len(got.Edges) != 1 {
		t.Errorf("len(edges) = %d, want 1", len(got.Edges))
	}
}

// TestScrubbingP2PVsRegistry is the single most important test in this
// package: it asserts the hard privacy requirement end-to-end across the
// actual HTTP responses (raw JSON body substring checks, not just decoded
// struct field checks, since the whole point is that the raw address
// string must never appear anywhere in the response body for a
// p2p_discovered node) — GET /nodes, GET /nodes/{id}, and GET /topology
// must never leak a p2p_discovered node's address, but must show a
// registry_submitted node's address (the owner opted in).
func TestScrubbingP2PVsRegistry(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	const p2pAddr = "1.2.3.4:18142"
	const registryAddr = "5.6.7.8:18142"

	p2pNode, err := store.UpsertDiscoveredNode(ctx, p2pAddr, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert p2p node: %v", err)
	}
	registryNode, err := store.UpsertDiscoveredNode(ctx, registryAddr, storage.DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert registry node: %v", err)
	}

	getBody := func(url string) string {
		t.Helper()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return string(body)
	}

	// GET /nodes
	listBody := getBody(srv.URL + "/nodes")
	if strings.Contains(listBody, p2pAddr) {
		t.Errorf("GET /nodes body contains p2p node's address %q:\n%s", p2pAddr, listBody)
	}
	if !strings.Contains(listBody, registryAddr) {
		t.Errorf("GET /nodes body missing registry node's address %q:\n%s", registryAddr, listBody)
	}
	if !strings.Contains(listBody, `"has_ipv4":true`) {
		t.Errorf("GET /nodes body missing has_ipv4:true capability signal:\n%s", listBody)
	}

	// GET /nodes/{id} for each node
	p2pGetBody := getBody(fmt.Sprintf("%s/nodes/%s", srv.URL, p2pNode.ID))
	if strings.Contains(p2pGetBody, p2pAddr) {
		t.Errorf("GET /nodes/%s body contains its own address %q:\n%s", p2pNode.ID, p2pAddr, p2pGetBody)
	}
	if !strings.Contains(p2pGetBody, `"has_ipv4":true`) {
		t.Errorf("GET /nodes/%s body missing has_ipv4:true capability signal:\n%s", p2pNode.ID, p2pGetBody)
	}

	registryGetBody := getBody(fmt.Sprintf("%s/nodes/%s", srv.URL, registryNode.ID))
	if !strings.Contains(registryGetBody, registryAddr) {
		t.Errorf("GET /nodes/%s body missing its own address %q:\n%s", registryNode.ID, registryAddr, registryGetBody)
	}

	// Cross-check: the p2p node's address must not appear in the
	// registry node's response either, and vice versa is not asserted
	// (no reason the registry address would appear there).
	if strings.Contains(registryGetBody, p2pAddr) {
		t.Errorf("GET /nodes/%s body contains the OTHER (p2p) node's address %q:\n%s", registryNode.ID, p2pAddr, registryGetBody)
	}

	// GET /topology
	topologyBody := getBody(srv.URL + "/topology")
	if strings.Contains(topologyBody, p2pAddr) {
		t.Errorf("GET /topology body contains p2p node's address %q:\n%s", p2pAddr, topologyBody)
	}
	if !strings.Contains(topologyBody, registryAddr) {
		t.Errorf("GET /topology body missing registry node's address %q:\n%s", registryAddr, topologyBody)
	}
}

// TestListSubmissions asserts GET /submissions defaults to listing
// pending submissions and honors the status query param.
func TestListSubmissions(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	pending, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	resp, err := http.Get(srv.URL + "/submissions")
	if err != nil {
		t.Fatalf("GET /submissions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got []storage.PendingSubmission
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != pending.ID {
		t.Fatalf("got = %+v, want just %v", got, pending.ID)
	}
}

// TestApproveSubmission asserts approving a pending submission returns
// 200, creates the real node with DiscoverySourceRegistry, sets
// promoted_node_id, and kicks off the same async health-check goroutine
// that used to run directly from POST /nodes.
func TestApproveSubmission(t *testing.T) {
	height := int64(42)
	client := &fakeClient{info: map[string]collector.NodeInfo{
		"1.2.3.4:18142": {Reachable: true, Height: &height},
	}}
	srv, store := newTestServer(t, client)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "1.2.3.4:18142", strPtr("my node"), strPtr("pool-x"))
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/submissions/%s/approve", srv.URL, submission.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/approve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusOK, body)
	}

	var got struct {
		Node       storage.Node              `json:"node"`
		Submission storage.PendingSubmission `json:"submission"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Node.Address != "1.2.3.4:18142" {
		t.Errorf("node.address = %q, want %q", got.Node.Address, "1.2.3.4:18142")
	}
	if got.Node.DiscoverySource != storage.DiscoverySourceRegistry {
		t.Errorf("node.discovery_source = %q, want %q", got.Node.DiscoverySource, storage.DiscoverySourceRegistry)
	}
	if got.Node.Label == nil || *got.Node.Label != "my node" {
		t.Errorf("node.label = %v, want %q", got.Node.Label, "my node")
	}
	if owner, _ := got.Node.Tags["owner"].(string); owner != "pool-x" {
		t.Errorf("node.tags[owner] = %v, want %q", got.Node.Tags["owner"], "pool-x")
	}
	if got.Submission.Status != storage.SubmissionStatusApproved {
		t.Errorf("submission.status = %q, want %q", got.Submission.Status, storage.SubmissionStatusApproved)
	}
	if got.Submission.PromotedNodeID == nil || *got.Submission.PromotedNodeID != got.Node.ID {
		t.Errorf("submission.promoted_node_id = %v, want %v", got.Submission.PromotedNodeID, got.Node.ID)
	}

	// Persisted state must agree with the response.
	persisted, err := store.GetPendingSubmission(ctx, submission.ID)
	if err != nil {
		t.Fatalf("get pending submission: %v", err)
	}
	if persisted.Status != storage.SubmissionStatusApproved {
		t.Errorf("persisted status = %q, want %q", persisted.Status, storage.SubmissionStatusApproved)
	}

	// The async health-check kickoff should record a health check
	// shortly after the POST returns, without the POST itself having
	// waited on it — same assertion technique as the old
	// TestCreateNodeValid used before submissions required approval.
	deadline := time.Now().Add(2 * time.Second)
	for {
		history, err := store.GetNodeHistory(ctx, got.Node.ID, 1)
		if err != nil {
			t.Fatalf("get node history: %v", err)
		}
		if len(history) == 1 {
			if !history[0].Reachable {
				t.Errorf("expected async health check to report reachable = true")
			}
			if history[0].Height == nil || *history[0].Height != height {
				t.Errorf("height = %v, want %d", history[0].Height, height)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for async health-check kickoff to record a health check")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestApproveSubmissionNotPending asserts approving an already-decided
// submission (rejected here) errors rather than double-processing it.
func TestApproveSubmissionNotPending(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}
	if err := store.RejectPendingSubmission(ctx, submission.ID, nil); err != nil {
		t.Fatalf("reject: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/submissions/%s/approve", srv.URL, submission.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/approve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Fatalf("status = %d, want an error status (submission already reviewed)", resp.StatusCode)
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("len(nodes) = %d, want 0 (approving an already-rejected submission must not create a node)", len(nodes))
	}
}

// TestApproveSubmissionAddressBecameOptedIn asserts that if the address
// became publicly opted-in AFTER the submission was queued but BEFORE
// it's approved, approval is rejected with a conflict and the submission
// is left pending (not auto-rejected) for a human to explicitly decide.
func TestApproveSubmissionAddressBecameOptedIn(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	// Someone else's submission for the same address gets approved
	// first (simulated directly here), making it publicly opted-in.
	if _, err := store.UpsertDiscoveredNode(ctx, "a:1", storage.DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert competing node: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/submissions/%s/approve", srv.URL, submission.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/approve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}

	// The submission must remain pending — not auto-flipped to rejected.
	got, err := store.GetPendingSubmission(ctx, submission.ID)
	if err != nil {
		t.Fatalf("get pending submission: %v", err)
	}
	if got.Status != storage.SubmissionStatusPending {
		t.Errorf("status = %q, want %q (left for a human decision)", got.Status, storage.SubmissionStatusPending)
	}
}

// TestRejectSubmission asserts rejecting a pending submission returns
// 200, flips its status to rejected with the given reason, and never
// touches the nodes table.
func TestRejectSubmission(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/submissions/%s/reject", srv.URL, submission.ID),
		"application/json",
		bytes.NewBufferString(`{"reason":"looks like spam"}`),
	)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/reject: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got storage.PendingSubmission
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Status != storage.SubmissionStatusRejected {
		t.Errorf("status = %q, want %q", got.Status, storage.SubmissionStatusRejected)
	}
	if got.RejectionReason == nil || *got.RejectionReason != "looks like spam" {
		t.Errorf("rejection_reason = %v, want %q", got.RejectionReason, "looks like spam")
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("len(nodes) = %d, want 0 (rejection must never touch the nodes table)", len(nodes))
	}
}

// TestRejectSubmissionEmptyBody asserts an empty request body is
// accepted (no reason set) rather than erroring on EOF.
func TestRejectSubmissionEmptyBody(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/submissions/%s/reject", srv.URL, submission.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/reject: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusOK, body)
	}

	var got storage.PendingSubmission
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Status != storage.SubmissionStatusRejected {
		t.Errorf("status = %q, want %q", got.Status, storage.SubmissionStatusRejected)
	}
	if got.RejectionReason != nil {
		t.Errorf("rejection_reason = %v, want nil", got.RejectionReason)
	}
}

// TestRejectSubmissionMalformedBody asserts a genuinely malformed JSON
// body (not just empty) is a 400, distinguishing "no body" from "bad
// body".
func TestRejectSubmissionMalformedBody(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	submission, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/submissions/%s/reject", srv.URL, submission.ID),
		"application/json",
		bytes.NewBufferString(`{not json`),
	)
	if err != nil {
		t.Fatalf("POST /submissions/{id}/reject: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestCreateNodeProbeAsync asserts the connectivity probe kicked off by a
// successful POST /nodes submission never blocks the HTTP response (the
// whole "async, non-blocking" point), and that probe_attempted_at /
// probe_reachable do eventually land on the pending_submissions row once
// the (slow, fixture) probe finishes.
func TestCreateNodeProbeAsync(t *testing.T) {
	const addr = "203.0.113.10:18142"
	client := &fakeClient{
		info:  map[string]collector.NodeInfo{addr: {Reachable: true}},
		delay: 2 * time.Second,
	}
	srv, store := newTestServer(t, client)
	ctx := context.Background()

	body := `{"host":"203.0.113.10","port":18142}`
	start := time.Now()
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes: %v", err)
	}
	elapsed := time.Since(start)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusAccepted, body)
	}
	if elapsed >= 200*time.Millisecond {
		t.Fatalf("POST /nodes took %v, want well under the 2s probe delay (probe must be async)", elapsed)
	}

	var submission storage.PendingSubmission
	if err := json.NewDecoder(resp.Body).Decode(&submission); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if submission.ProbeAttemptedAt != nil {
		t.Errorf("probe_attempted_at = %v, want nil (probe hasn't finished yet)", submission.ProbeAttemptedAt)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		got, err := store.GetPendingSubmission(ctx, submission.ID)
		if err != nil {
			t.Fatalf("get pending submission: %v", err)
		}
		if got.ProbeAttemptedAt != nil {
			if got.ProbeReachable == nil || !*got.ProbeReachable {
				t.Errorf("probe_reachable = %v, want true", got.ProbeReachable)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for async probe to record a result")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestCreateNodeSSRFRejected asserts private/reserved-IP hosts are
// rejected with 400, while a real public IP and a syntactically valid
// onion host both still queue normally (202). Each case gets its own
// fresh server (and so its own fresh rate-limiter/lockout-tracker
// instance) so the SSRF-rejected cases' strikes don't accumulate into a
// shared lockout that would corrupt later cases in this same test.
func TestCreateNodeSSRFRejected(t *testing.T) {
	cases := []struct {
		name       string
		host       string
		port       int
		wantStatus int
	}{
		{"loopback v4", "127.0.0.1", 18142, http.StatusBadRequest},
		{"private v4 class C", "192.168.1.1", 18142, http.StatusBadRequest},
		{"private v4 class A", "10.0.0.1", 18142, http.StatusBadRequest},
		{"link-local v4", "169.254.1.1", 18142, http.StatusBadRequest},
		{"loopback v6", "::1", 18142, http.StatusBadRequest},
		{"public v4", "1.1.1.1", 18143, http.StatusAccepted},
		{"onion v3-shaped", "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion", 18144, http.StatusAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newTestServer(t, nil)
			body := fmt.Sprintf(`{"host":%q,"port":%d}`, tc.host, tc.port)
			resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
			if err != nil {
				t.Fatalf("POST /nodes: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				respBody, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, respBody)
			}
		})
	}
}

// TestCreateNodeRateLimit asserts the 6th submission from the same
// source IP within the limiter's window (burst 5) gets 429 with a
// Retry-After header, while the first 5 (distinct, otherwise-valid
// addresses) succeed normally.
func TestCreateNodeRateLimit(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{"host":"198.51.100.%d","port":18142}`, i+1)
		resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("POST /nodes (%d): %v", i, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("submission %d: status = %d, want %d (body: %s)", i, resp.StatusCode, http.StatusAccepted, respBody)
		}
	}

	// The 6th submission (still a distinct, otherwise-valid address)
	// exceeds the per-IP rate limit.
	body := `{"host":"198.51.100.6","port":18142}`
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes (6th): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusTooManyRequests, respBody)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Errorf("Retry-After header missing on 429 response")
	}
}

// TestCreateNodeInvalidSubmissionLockout asserts that repeated
// genuinely-invalid submissions (a reused private-IP host) from one
// source IP eventually flip from repeated 400s to a 429 lockout once the
// strike threshold is hit.
func TestCreateNodeInvalidSubmissionLockout(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	const body = `{"host":"127.0.0.1","port":18142}`

	// strikeThreshold is 4: the first 4 genuinely-invalid submissions
	// from this IP each get a plain 400 (and record a strike); the 4th
	// strike crosses the threshold and locks the IP out.
	for i := 0; i < 4; i++ {
		resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("POST /nodes (strike %d): %v", i, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("strike %d: status = %d, want %d (body: %s)", i, resp.StatusCode, http.StatusBadRequest, respBody)
		}
	}

	// The next submission — even a well-formed, valid one — is now
	// blocked by the lockout before validation even runs.
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(`{"host":"198.51.100.50","port":18142}`))
	if err != nil {
		t.Fatalf("POST /nodes (after lockout): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusTooManyRequests, respBody)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Errorf("Retry-After header missing on lockout 429 response")
	}
}

// TestPollNowCreatesNewNode asserts POST /nodes/poll-now against a
// brand-new host:port creates exactly one node, synchronously probes it,
// and returns the resulting node state (pubkey confirmed) in the
// response body.
func TestPollNowCreatesNewNode(t *testing.T) {
	const addr = "203.0.113.20:18142"
	pubkey := []byte{1, 2, 3, 4}
	client := &fakeClient{info: map[string]collector.NodeInfo{
		addr: {Reachable: true, PublicKey: pubkey},
	}}
	srv, store := newTestServer(t, client)
	ctx := context.Background()

	body := `{"host":"203.0.113.20","port":18142}`
	resp, err := http.Post(srv.URL+"/nodes/poll-now", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes/poll-now: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusOK, respBody)
	}

	var got struct {
		Node  storage.Node `json:"node"`
		Error string       `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Error != "" {
		t.Errorf("error = %q, want empty", got.Error)
	}
	if got.Node.Address != addr {
		t.Errorf("node.address = %q, want %q", got.Node.Address, addr)
	}
	if len(got.Node.PublicKey) == 0 {
		t.Errorf("node.public_key = %v, want the confirmed pubkey", got.Node.PublicKey)
	}

	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1", len(nodes))
	}
	if nodes[0].Address != addr {
		t.Errorf("nodes[0].address = %q, want %q", nodes[0].Address, addr)
	}
}

// TestPollNowReusesExistingNode asserts POST /nodes/poll-now against an
// already-known address reuses the existing node row (same ID) rather
// than creating a duplicate.
func TestPollNowReusesExistingNode(t *testing.T) {
	const addr = "203.0.113.21:18142"
	client := &fakeClient{info: map[string]collector.NodeInfo{
		addr: {Reachable: true},
	}}
	srv, store := newTestServer(t, client)
	ctx := context.Background()

	existing, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert existing node: %v", err)
	}

	before, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes before: %v", err)
	}

	body := fmt.Sprintf(`{"host":"203.0.113.21","port":18142}`)
	resp, err := http.Post(srv.URL+"/nodes/poll-now", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes/poll-now: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusOK, respBody)
	}

	var got struct {
		Node storage.Node `json:"node"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Node.ID != existing.ID {
		t.Errorf("node.id = %v, want existing node's id %v (must reuse, not duplicate)", got.Node.ID, existing.ID)
	}

	after, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		t.Fatalf("list nodes after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("len(nodes) = %d, want unchanged %d (no duplicate row created)", len(after), len(before))
	}
}

// TestPollNowSkipsSSRFValidation asserts POST /nodes/poll-now does NOT
// apply the SSRF/private-IP validation that POST /nodes does — it must
// accept a private-IP host and probe it (200, even when the probe
// itself fails/is unreachable), in contrast to POST /nodes rejecting the
// exact same host with a 400 and the SSRF error message.
func TestPollNowSkipsSSRFValidation(t *testing.T) {
	srv, _ := newTestServer(t, nil) // no fixture for this address -> GetInfo fails -> unreachable

	body := `{"host":"127.0.0.1","port":18142}`

	// Contrast: POST /nodes rejects this exact host with the SSRF
	// message.
	createResp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes: %v", err)
	}
	defer createResp.Body.Close()
	createBody, _ := io.ReadAll(createResp.Body)
	if createResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /nodes status = %d, want %d", createResp.StatusCode, http.StatusBadRequest)
	}
	if !strings.Contains(string(createBody), "private/reserved IP addresses are not allowed") {
		t.Errorf("POST /nodes body missing SSRF rejection message: %s", createBody)
	}

	// POST /nodes/poll-now must accept the same host.
	pollResp, err := http.Post(srv.URL+"/nodes/poll-now", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes/poll-now: %v", err)
	}
	defer pollResp.Body.Close()
	pollBody, _ := io.ReadAll(pollResp.Body)
	if pollResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /nodes/poll-now status = %d, want %d (body: %s)", pollResp.StatusCode, http.StatusOK, pollBody)
	}
	if strings.Contains(string(pollBody), "private/reserved IP addresses are not allowed") {
		t.Errorf("POST /nodes/poll-now body unexpectedly contains SSRF rejection message: %s", pollBody)
	}
}

// TestPollNowAcceptsPrivateIPv4 asserts POST /nodes/poll-now accepts a
// plain RFC1918 private IPv4 host (192.168.1.1, distinct from the
// loopback case covered by TestPollNowSkipsSSRFValidation above) and
// probes it (200), preserving the endpoint's deliberate SSRF exemption
// for admins who need to force-poll internal addresses.
func TestPollNowAcceptsPrivateIPv4(t *testing.T) {
	srv, _ := newTestServer(t, nil) // no fixture for this address -> GetInfo fails -> unreachable, still 200

	body := `{"host":"192.168.1.1","port":18142}`
	resp, err := http.Post(srv.URL+"/nodes/poll-now", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes/poll-now: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusOK, respBody)
	}
	if strings.Contains(string(respBody), "private/reserved IP addresses are not allowed") {
		t.Errorf("body unexpectedly contains SSRF rejection message: %s", respBody)
	}
}

// TestPollNowInvalid asserts empty-host, out-of-range-port, and
// malformed-host requests are rejected with 400 and the same error
// message text as handleCreateNode's equivalent checks (for the
// empty-host/bad-port cases) or validateHostSyntax's message (for the
// malformed-host case) — and that the malformed-host case does not
// create a node.
func TestPollNowInvalid(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	cases := []struct {
		name    string
		body    string
		wantMsg string
	}{
		{"missing host", `{"port":18142}`, "host is required"},
		{"port too small", `{"host":"1.2.3.4","port":0}`, "port must be between 1 and 65535"},
		{"port too large", `{"host":"1.2.3.4","port":65536}`, "port must be between 1 and 65535"},
		{
			"malformed onion host with leading slash",
			`{"host":"/wyow2dp6w2ff4u2kebklkmbzwlixyhjtza5bf3pt3oxnps5hcjn76iyd.onion","port":18142}`,
			"host must be a valid IP address or .onion address",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, err := store.ListNodes(ctx, storage.NodeFilter{})
			if err != nil {
				t.Fatalf("list nodes before: %v", err)
			}

			resp, err := http.Post(srv.URL+"/nodes/poll-now", "application/json", bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatalf("POST /nodes/poll-now: %v", err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusBadRequest, respBody)
			}
			if !strings.Contains(string(respBody), tc.wantMsg) {
				t.Errorf("body = %s, want it to contain %q", respBody, tc.wantMsg)
			}

			after, err := store.ListNodes(ctx, storage.NodeFilter{})
			if err != nil {
				t.Fatalf("list nodes after: %v", err)
			}
			if len(after) != len(before) {
				t.Errorf("len(nodes) = %d, want unchanged %d (rejected request must not create a node)", len(after), len(before))
			}
		})
	}
}

// TestCreateNodeQueueCap asserts that once the pending-submission queue
// reaches api.MaxPendingSubmissions, further submissions are rejected
// with 503 rather than growing the queue further. MaxPendingSubmissions
// is temporarily lowered so this test doesn't need to create 100 real
// rows.
func TestCreateNodeQueueCap(t *testing.T) {
	orig := api.MaxPendingSubmissions
	api.MaxPendingSubmissions = 3
	defer func() { api.MaxPendingSubmissions = orig }()

	srv, _ := newTestServer(t, nil)

	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"host":"198.51.100.%d","port":18150}`, i+10)
		resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("POST /nodes (%d): %v", i, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("submission %d: status = %d, want %d (body: %s)", i, resp.StatusCode, http.StatusAccepted, respBody)
		}
	}

	// The 4th submission is a distinct address too — it must be
	// rejected purely because the queue is full, not for any other
	// reason.
	body := `{"host":"198.51.100.13","port":18150}`
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes (over cap): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, http.StatusServiceUnavailable, respBody)
	}
}

// TestListNodesPaginationDefaults asserts GET /nodes with no query
// params applies a default page limit rather than returning every node
// — seeded well above the default limit to actually exercise that.
func TestListNodesPaginationDefaults(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	const seeded = 105 // > api's default limit of 100
	for i := 0; i < seeded; i++ {
		addr := fmt.Sprintf("10.0.%d.%d:18142", i/256, i%256)
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("upsert node %d: %v", i, err)
		}
	}

	resp, err := http.Get(srv.URL + "/nodes")
	if err != nil {
		t.Fatalf("GET /nodes: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Nodes   []api.PublicNode `json:"nodes"`
		Total   int              `json:"total"`
		Limit   int              `json:"limit"`
		Offset  int              `json:"offset"`
		HasMore bool             `json:"has_more"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Total != seeded {
		t.Fatalf("got.Total = %d, want %d", got.Total, seeded)
	}
	if len(got.Nodes) != 100 {
		t.Fatalf("len(got.Nodes) = %d, want 100 (default limit), not the full %d", len(got.Nodes), seeded)
	}
	if got.Limit != 100 {
		t.Fatalf("got.Limit = %d, want 100", got.Limit)
	}
	if got.Offset != 0 {
		t.Fatalf("got.Offset = %d, want 0", got.Offset)
	}
	if !got.HasMore {
		t.Fatal("got.HasMore = false, want true (105 seeded, only 100 returned)")
	}
}

// TestListNodesExplicitLimitOffset asserts GET /nodes?limit=X&offset=Y
// returns the correct page (in the same address-sorted order ListNodes
// always uses) and correctly reflects has_more at the boundary.
func TestListNodesExplicitLimitOffset(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	for _, addr := range []string{"n1:1", "n2:1", "n3:1", "n4:1", "n5:1"} {
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
	}

	resp, err := http.Get(srv.URL + "/nodes?limit=2&offset=2")
	if err != nil {
		t.Fatalf("GET /nodes?limit=2&offset=2: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Nodes   []api.PublicNode `json:"nodes"`
		Total   int              `json:"total"`
		Limit   int              `json:"limit"`
		Offset  int              `json:"offset"`
		HasMore bool             `json:"has_more"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("len(got.Nodes) = %d, want 2", len(got.Nodes))
	}
	if got.Total != 5 {
		t.Fatalf("got.Total = %d, want 5", got.Total)
	}
	if !got.HasMore {
		t.Error("got.HasMore = false, want true (offset 2 + limit 2 = 4 < total 5)")
	}

	// Last page: offset 4, limit 2 -> only 1 node left, HasMore false.
	resp2, err := http.Get(srv.URL + "/nodes?limit=2&offset=4")
	if err != nil {
		t.Fatalf("GET /nodes?limit=2&offset=4: %v", err)
	}
	defer resp2.Body.Close()
	var got2 struct {
		Nodes   []api.PublicNode `json:"nodes"`
		HasMore bool             `json:"has_more"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&got2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got2.Nodes) != 1 {
		t.Fatalf("len(got2.Nodes) = %d, want 1", len(got2.Nodes))
	}
	if got2.HasMore {
		t.Error("got2.HasMore = true, want false (last page)")
	}
}

// TestListNodesLimitClamp asserts a `?limit=` above the API's hard cap
// is silently clamped rather than erroring.
func TestListNodesLimitClamp(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	resp, err := http.Get(srv.URL + "/nodes?limit=100000")
	if err != nil {
		t.Fatalf("GET /nodes?limit=100000: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Limit int `json:"limit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Limit != 500 {
		t.Fatalf("got.Limit = %d, want 500 (clamped)", got.Limit)
	}
}

// TestListNodesInvalidOffset asserts a negative `?offset=` is rejected
// with 400.
func TestListNodesInvalidOffset(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	resp, err := http.Get(srv.URL + "/nodes?offset=-1")
	if err != nil {
		t.Fatalf("GET /nodes?offset=-1: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestTopologyDefaultCap asserts GET /topology with no params bounds the
// returned node set once the population exceeds the cap: it lowers the
// cap via a small `?limit=` override rather than needing 300+ real seeded
// nodes, to exercise the same capping code path handleTopology's default
// takes.
func TestTopologyDefaultCap(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	// hub is the best-connected node; leaf1/leaf2/leaf3 all connect only
	// to hub, so hub + any 2 of the leaves is a valid top-3.
	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	var leaves []uuid.UUID
	for _, addr := range []string{"leaf1:1", "leaf2:1", "leaf3:1"} {
		leaf, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		leaves = append(leaves, leaf.ID)
		if err := store.RecordPeerEdgeObservation(ctx, hub.ID, leaf.ID); err != nil {
			t.Fatalf("record edge hub->%s: %v", addr, err)
		}
	}

	// All 4 nodes exist; cap to 3 via an explicit ?limit= override (the
	// cheapest way to exercise the capping logic itself without seeding
	// 300+ real nodes for the production default).
	resp, err := http.Get(srv.URL + "/topology?limit=3")
	if err != nil {
		t.Fatalf("GET /topology?limit=3: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Fatalf("len(got.Nodes) = %d, want 3 (capped)", len(got.Nodes))
	}
	gotIDs := map[uuid.UUID]bool{}
	for _, n := range got.Nodes {
		gotIDs[n.ID] = true
	}
	if !gotIDs[hub.ID] {
		t.Errorf("got.Nodes missing hub (highest degree), gotIDs=%v", gotIDs)
	}
	for _, e := range got.Edges {
		if !gotIDs[e.FromNodeID] || !gotIDs[e.ToNodeID] {
			t.Errorf("edge %+v has an endpoint outside the returned node set", e)
		}
	}
}

// TestTopologyAllTrueUnbounded asserts GET /topology?all=true always
// returns every node/edge, ignoring the default cap.
func TestTopologyAllTrueUnbounded(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	for _, addr := range []string{"leaf1:1", "leaf2:1", "leaf3:1"} {
		leaf, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		if err := store.RecordPeerEdgeObservation(ctx, hub.ID, leaf.ID); err != nil {
			t.Fatalf("record edge hub->%s: %v", addr, err)
		}
	}

	resp, err := http.Get(srv.URL + "/topology?all=true")
	if err != nil {
		t.Fatalf("GET /topology?all=true: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 4 {
		t.Fatalf("len(got.Nodes) = %d, want 4 (unbounded)", len(got.Nodes))
	}
	if len(got.Edges) != 3 {
		t.Fatalf("len(got.Edges) = %d, want 3 (unbounded)", len(got.Edges))
	}
}

// TestTopologyDefaultMaxEdges asserts GET /topology with no params
// returns an edges array bounded by defaultTopologyMaxEdges. Seeding
// thousands of edges to actually exceed the 2500 default isn't practical
// for a fast/deterministic unit test, so this seeds a small dataset and
// asserts the invariant holds (it's trivially true here, but it
// exercises the same default-wiring code path as the production case;
// the max_edges/all=true tests above are what actually prove the cap
// truncates when exceeded).
func TestTopologyDefaultMaxEdges(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	for _, addr := range []string{"leaf1:1", "leaf2:1"} {
		leaf, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		if err := store.RecordPeerEdgeObservation(ctx, hub.ID, leaf.ID); err != nil {
			t.Fatalf("record edge hub->%s: %v", addr, err)
		}
	}

	resp, err := http.Get(srv.URL + "/topology")
	if err != nil {
		t.Fatalf("GET /topology: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 2500 mirrors api.defaultTopologyMaxEdges (unexported).
	const wantMaxEdges = 2500
	if len(got.Edges) > wantMaxEdges {
		t.Fatalf("len(got.Edges) = %d, want <= %d (default max_edges cap)", len(got.Edges), wantMaxEdges)
	}
}

// TestTopologyMaxEdgesCap asserts GET /topology?max_edges=N caps the
// returned edge count independently of the node cap: it seeds a small
// clique (more real edges between the nodes than the requested
// max_edges) and a generous ?limit= so every node survives the node cap,
// then checks the edge count is bounded by max_edges.
func TestTopologyMaxEdgesCap(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	// 4 nodes, pairwise connected: 6 undirected edges, more than the
	// max_edges=3 cap used below.
	var ids []uuid.UUID
	for _, addr := range []string{"clique-a:1", "clique-b:1", "clique-c:1", "clique-d:1"} {
		n, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		ids = append(ids, n.ID)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if err := store.RecordPeerEdgeObservation(ctx, ids[i], ids[j]); err != nil {
				t.Fatalf("record edge %v -> %v: %v", ids[i], ids[j], err)
			}
		}
	}

	resp, err := http.Get(srv.URL + "/topology?limit=100&max_edges=3")
	if err != nil {
		t.Fatalf("GET /topology?limit=100&max_edges=3: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 4 {
		t.Fatalf("len(got.Nodes) = %d, want 4 (node cap not exceeded)", len(got.Nodes))
	}
	if len(got.Edges) > 3 {
		t.Fatalf("len(got.Edges) = %d, want <= 3 (max_edges cap)", len(got.Edges))
	}
}

// TestTopologyAllTrueUnboundedEdges asserts GET /topology?all=true
// returns the full, uncapped edge set even when there are more edges
// than defaultTopologyMaxEdges would normally allow -- the escape hatch
// must be genuinely uncapped on both the node and edge dimensions, not
// just nodes.
func TestTopologyAllTrueUnboundedEdges(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	var ids []uuid.UUID
	for _, addr := range []string{"clique-a:1", "clique-b:1", "clique-c:1", "clique-d:1"} {
		n, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
		ids = append(ids, n.ID)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if err := store.RecordPeerEdgeObservation(ctx, ids[i], ids[j]); err != nil {
				t.Fatalf("record edge %v -> %v: %v", ids[i], ids[j], err)
			}
		}
	}

	resp, err := http.Get(srv.URL + "/topology?all=true")
	if err != nil {
		t.Fatalf("GET /topology?all=true: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Nodes []api.PublicNode   `json:"nodes"`
		Edges []storage.PeerEdge `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 4 {
		t.Fatalf("len(got.Nodes) = %d, want 4 (unbounded)", len(got.Nodes))
	}
	if len(got.Edges) != 6 {
		t.Fatalf("len(got.Edges) = %d, want 6 (unbounded, all pairwise edges present)", len(got.Edges))
	}
}

// TestTopologyInvalidMaxEdges asserts GET /topology?max_edges= rejects
// non-positive/non-numeric values with 400, mirroring ?limit='s
// validation.
func TestTopologyInvalidMaxEdges(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	for _, v := range []string{"0", "-1", "not-a-number"} {
		resp, err := http.Get(srv.URL + "/topology?max_edges=" + v)
		if err != nil {
			t.Fatalf("GET /topology?max_edges=%s: %v", v, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("max_edges=%s: status = %d, want %d", v, resp.StatusCode, http.StatusBadRequest)
		}
	}
}
