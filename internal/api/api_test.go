package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edges, nodes CASCADE"); err != nil {
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
}

func (f *fakeClient) GetPeers(ctx context.Context, addr string) ([]string, error) {
	return nil, nil
}

func (f *fakeClient) GetInfo(ctx context.Context, addr string) (collector.NodeInfo, error) {
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

func TestCreateNodeValid(t *testing.T) {
	height := int64(42)
	client := &fakeClient{info: map[string]collector.NodeInfo{
		"1.2.3.4:18142": {Reachable: true, Height: &height},
	}}
	srv, store := newTestServer(t, client)

	body := `{"host":"1.2.3.4","port":18142,"label":"my node","owner_tag":"pool-x"}`
	resp, err := http.Post(srv.URL+"/nodes", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /nodes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	var node storage.Node
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if node.Address != "1.2.3.4:18142" {
		t.Errorf("address = %q, want %q", node.Address, "1.2.3.4:18142")
	}
	if node.DiscoverySource != storage.DiscoverySourceRegistry {
		t.Errorf("discovery_source = %q, want %q", node.DiscoverySource, storage.DiscoverySourceRegistry)
	}
	if node.Label == nil || *node.Label != "my node" {
		t.Errorf("label = %v, want %q", node.Label, "my node")
	}
	if owner, _ := node.Tags["owner"].(string); owner != "pool-x" {
		t.Errorf("tags[owner] = %v, want %q", node.Tags["owner"], "pool-x")
	}

	// The async health-check kickoff should record a health check shortly
	// after the POST returns, without the POST itself having waited on it.
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		history, err := store.GetNodeHistory(ctx, node.ID, 1)
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

	if _, err := store.UpsertNode(ctx, storage.NodeInput{Address: "a:1", DiscoverySource: storage.DiscoverySourceP2P}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if _, err := store.UpsertNode(ctx, storage.NodeInput{Address: "b:2", DiscoverySource: storage.DiscoverySourceRegistry}); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	resp, err := http.Get(srv.URL + "/nodes")
	if err != nil {
		t.Fatalf("GET /nodes: %v", err)
	}
	defer resp.Body.Close()
	var all []storage.Node
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all) = %d, want 2", len(all))
	}

	resp2, err := http.Get(srv.URL + "/nodes?discovery_source=p2p_discovered")
	if err != nil {
		t.Fatalf("GET /nodes?discovery_source=...: %v", err)
	}
	defer resp2.Body.Close()
	var filtered []storage.Node
	if err := json.NewDecoder(resp2.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Address != "a:1" {
		t.Fatalf("filtered = %+v, want just a:1", filtered)
	}
}

func TestGetNode(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	node, err := store.UpsertNode(ctx, storage.NodeInput{Address: "a:1", DiscoverySource: storage.DiscoverySourceP2P})
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
	var got storage.Node
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

	node, err := store.UpsertNode(ctx, storage.NodeInput{Address: "a:1", DiscoverySource: storage.DiscoverySourceP2P})
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

func TestTopology(t *testing.T) {
	srv, store := newTestServer(t, nil)
	ctx := context.Background()

	a, err := store.UpsertNode(ctx, storage.NodeInput{Address: "a:1", DiscoverySource: storage.DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	b, err := store.UpsertNode(ctx, storage.NodeInput{Address: "b:2", DiscoverySource: storage.DiscoverySourceP2P})
	if err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	if err := store.UpsertPeerEdge(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("upsert edge: %v", err)
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
		Nodes []storage.Node     `json:"nodes"`
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
