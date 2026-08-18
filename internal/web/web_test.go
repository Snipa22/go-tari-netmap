package web_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
	"github.com/Snipa22/go-tari-netmap/internal/web"
)

// defaultTestDSN matches this sandbox's real Postgres 17 instance: unix
// socket at /workspace/pg-embed/sockets, port 5433, database "netmap",
// user "postgres", trust auth (no password). Mirrors
// internal/api/api_test.go's convention.
const defaultTestDSN = "postgres://postgres@localhost:5433/netmap?sslmode=disable&host=/workspace/pg-embed/sockets"

func testDSN() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return defaultTestDSN
}

// acquireTestDBLock takes a session-level Postgres advisory lock shared by
// every test package that exercises the real test database (storage, api,
// collector, web). `go test ./...` runs each package's tests as a
// separate process, potentially in parallel, but they all point at the
// same shared Postgres instance and truncate its tables — without this
// lock, two packages' tests running concurrently stomp on each other's
// data. The returned func releases the lock and must be registered as a
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
// the test to serialize access with the storage, api, and collector
// packages' tests against the same shared database.
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
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edge_observations, node_addresses, nodes CASCADE"); err != nil {
		pool.Close()
		t.Fatalf("truncate test tables: %v", err)
	}
	pool.Close()

	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestServer(t *testing.T, store storage.Store) *httptest.Server {
	t.Helper()
	handler, err := web.NewHandler(store)
	if err != nil {
		t.Fatalf("build web handler: %v", err)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func getBody(t *testing.T, url string) (int, string) {
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
	return resp.StatusCode, string(body)
}

// TestDashboardHidesP2PAddress asserts GET / returns 200 and never
// includes a p2p_discovered node's raw address in the rendered HTML —
// the whole point of this dispatch's privacy-aware redesign.
func TestDashboardHidesP2PAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const p2pAddr = "1.2.3.4:18142"
	if _, err := store.UpsertDiscoveredNode(ctx, p2pAddr, storage.DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert p2p node: %v", err)
	}

	srv := newTestServer(t, store)
	status, body := getBody(t, srv.URL+"/")
	if status != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", status, http.StatusOK)
	}
	if strings.Contains(body, p2pAddr) {
		t.Errorf("GET / body contains p2p node's raw address %q", p2pAddr)
	}
	if !strings.Contains(body, "unconfirmed") {
		t.Errorf("GET / body missing an unconfirmed badge for the placeholder node")
	}
}

// TestNodeDetailHidesP2PAddress asserts GET /nodes/{id} returns 200 and
// never includes the node's own raw address when it was only
// p2p_discovered, but does show a registry_submitted node's opted-in
// address.
func TestNodeDetailHidesP2PAddress(t *testing.T) {
	store := newTestStore(t)
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

	srv := newTestServer(t, store)

	status, p2pBody := getBody(t, srv.URL+"/nodes/"+p2pNode.ID.String())
	if status != http.StatusOK {
		t.Fatalf("GET /nodes/%s status = %d, want %d", p2pNode.ID, status, http.StatusOK)
	}
	if strings.Contains(p2pBody, p2pAddr) {
		t.Errorf("GET /nodes/%s body contains its own raw address %q", p2pNode.ID, p2pAddr)
	}

	status, registryBody := getBody(t, srv.URL+"/nodes/"+registryNode.ID.String())
	if status != http.StatusOK {
		t.Fatalf("GET /nodes/%s status = %d, want %d", registryNode.ID, status, http.StatusOK)
	}
	if !strings.Contains(registryBody, registryAddr) {
		t.Errorf("GET /nodes/%s body missing its opted-in address %q", registryNode.ID, registryAddr)
	}
	if strings.Contains(registryBody, p2pAddr) {
		t.Errorf("GET /nodes/%s body contains the OTHER (p2p) node's address %q", registryNode.ID, p2pAddr)
	}
}

// TestNodeDetailNotFound asserts an unknown node id 404s rather than
// panicking or erroring out the template.
func TestNodeDetailNotFound(t *testing.T) {
	store := newTestStore(t)
	srv := newTestServer(t, store)

	status, _ := getBody(t, srv.URL+"/nodes/00000000-0000-0000-0000-000000000000")
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want %d", status, http.StatusNotFound)
	}
}

// TestStaticStylesheetServed asserts the CSS route is wired up and
// returns a text/css response.
func TestStaticStylesheetServed(t *testing.T) {
	store := newTestStore(t)
	srv := newTestServer(t, store)

	resp, err := http.Get(srv.URL + "/static/style.css")
	if err != nil {
		t.Fatalf("GET /static/style.css: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/css") {
		t.Errorf("Content-Type = %q, want text/css prefix", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) == 0 {
		t.Error("expected non-empty CSS body")
	}
}
