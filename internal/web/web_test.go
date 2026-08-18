package web_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-netmap/internal/adminauth"
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
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE node_health, peer_edge_observations, node_addresses, pending_submissions, nodes CASCADE"); err != nil {
		pool.Close()
		t.Fatalf("truncate test tables: %v", err)
	}
	pool.Close()

	t.Cleanup(func() { _ = store.Close() })
	return store
}

// testAdminUser/testAdminPassword are the fixed HTTP Basic Auth
// credentials used by newTestServer (and every test that exercises an
// /admin/* route) — see newTestServerWithCreds below for the "not
// configured" variant used to prove the fail-closed-503 behavior.
const (
	testAdminUser     = "admin-test-user"
	testAdminPassword = "admin-test-password"
)

func newTestServer(t *testing.T, store storage.Store) *httptest.Server {
	t.Helper()
	return newTestServerWithCreds(t, store, adminauth.Credentials{Username: testAdminUser, Password: testAdminPassword})
}

// newTestServerWithCreds is like newTestServer but lets the caller
// control the admin credentials web.NewHandler is built with — in
// particular, passing a zero-value adminauth.Credentials{} simulates
// NETMAP_ADMIN_USER/NETMAP_ADMIN_PASSWORD being unset, which must fail
// closed (503) on every /admin/* route regardless of what's supplied on
// the request.
func newTestServerWithCreds(t *testing.T, store storage.Store, creds adminauth.Credentials) *httptest.Server {
	t.Helper()
	handler, err := web.NewHandler(store, creds)
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

// getBodyWithAuth is like getBody but sets an Authorization: Basic
// header (via req.SetBasicAuth) using the given user/pass before
// issuing the GET — used to exercise /admin/* routes, which require
// valid credentials.
func getBodyWithAuth(t *testing.T, url, user, pass string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", url, err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body), resp.Header
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

// TestDashboardTopPeeredIdentityIsLink asserts the dashboard's "top
// peered" panel renders each row's identity cell as a link to
// /nodes/{id}, matching the main Nodes table below it.
func TestDashboardTopPeeredIdentityIsLink(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hub, err := store.UpsertDiscoveredNode(ctx, "hub:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert hub: %v", err)
	}
	leaf, err := store.UpsertDiscoveredNode(ctx, "leaf:1", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert leaf: %v", err)
	}
	if err := store.RecordPeerEdgeObservation(ctx, hub.ID, leaf.ID); err != nil {
		t.Fatalf("record edge observation: %v", err)
	}

	srv := newTestServer(t, store)
	status, body := getBody(t, srv.URL+"/")
	if status != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, fmt.Sprintf(`href="/nodes/%s"`, hub.ID)) {
		t.Errorf("GET / body missing top-peered identity link for hub node %s", hub.ID)
	}
	if !strings.Contains(body, "(last 1h)") {
		t.Errorf("GET / body missing top-peered window label %q", "(last 1h)")
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

// TestDualStackBadgeShownForOptedInNodeWithBothTransports asserts a node
// with both a clearnet and an onion address known renders the
// "badge-dualstack" badge in both the dashboard (GET /) and the node
// detail page (GET /nodes/{id}). Two addresses attach to the same node by
// confirming the same pubkey at two different addresses, per
// TestUpsertConfirmedNodeAddsAddressToKnownPubkey's established pattern.
func TestDualStackBadgeShownForOptedInNodeWithBothTransports(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const clearnetAddr = "1.2.3.4:18142"
	const onionAddr = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142"
	pubkey := []byte("dual-stack-pubkey")

	node, err := store.UpsertConfirmedNode(ctx, clearnetAddr, pubkey, storage.DiscoverySourceRegistry)
	if err != nil {
		t.Fatalf("upsert confirmed clearnet: %v", err)
	}
	if _, err := store.UpsertConfirmedNode(ctx, onionAddr, pubkey, storage.DiscoverySourceRegistry); err != nil {
		t.Fatalf("upsert confirmed onion: %v", err)
	}

	srv := newTestServer(t, store)

	_, dashboardBody := getBody(t, srv.URL+"/")
	if !strings.Contains(dashboardBody, "badge-dualstack") {
		t.Errorf("GET / body missing badge-dualstack for a node with both clearnet and onion addresses")
	}

	_, detailBody := getBody(t, srv.URL+"/nodes/"+node.ID.String())
	if !strings.Contains(detailBody, "badge-dualstack") {
		t.Errorf("GET /nodes/%s body missing badge-dualstack for a node with both clearnet and onion addresses", node.ID)
	}
}

// TestDualStackBadgeHiddenForSingleTransportNodes asserts nodes with only
// clearnet or only onion addresses known never render "badge-dualstack"
// in either the dashboard or the node detail page.
func TestDualStackBadgeHiddenForSingleTransportNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	clearnetOnly, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", storage.DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert clearnet-only node: %v", err)
	}
	onionOnly, err := store.UpsertDiscoveredNode(ctx, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142", storage.DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert onion-only node: %v", err)
	}

	srv := newTestServer(t, store)

	_, dashboardBody := getBody(t, srv.URL+"/")
	if strings.Contains(dashboardBody, "badge-dualstack") {
		t.Errorf("GET / body contains badge-dualstack, but no node has both clearnet and onion addresses")
	}

	_, clearnetBody := getBody(t, srv.URL+"/nodes/"+clearnetOnly.ID.String())
	if strings.Contains(clearnetBody, "badge-dualstack") {
		t.Errorf("GET /nodes/%s body contains badge-dualstack for a clearnet-only node", clearnetOnly.ID)
	}

	_, onionBody := getBody(t, srv.URL+"/nodes/"+onionOnly.ID.String())
	if strings.Contains(onionBody, "badge-dualstack") {
		t.Errorf("GET /nodes/%s body contains badge-dualstack for an onion-only node", onionOnly.ID)
	}
}

// TestOptedInNodeWithMultipleAddressesShowsAllAddresses asserts an
// opted-in (registry_submitted or both) node with multiple known
// addresses (one clearnet, one onion) shows BOTH real address strings in
// the dashboard and node detail page bodies — the web-layer proof of
// Request 2's multi-address display.
func TestOptedInNodeWithMultipleAddressesShowsAllAddresses(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const clearnetAddr = "1.2.3.4:18142"
	const onionAddr = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142"
	pubkey := []byte("multi-addr-pubkey")

	node, err := store.UpsertConfirmedNode(ctx, clearnetAddr, pubkey, storage.DiscoverySourceBoth)
	if err != nil {
		t.Fatalf("upsert confirmed clearnet: %v", err)
	}
	if _, err := store.UpsertConfirmedNode(ctx, onionAddr, pubkey, storage.DiscoverySourceBoth); err != nil {
		t.Fatalf("upsert confirmed onion: %v", err)
	}

	srv := newTestServer(t, store)

	_, dashboardBody := getBody(t, srv.URL+"/")
	if !strings.Contains(dashboardBody, clearnetAddr) {
		t.Errorf("GET / body missing opted-in node's clearnet address %q", clearnetAddr)
	}
	if !strings.Contains(dashboardBody, onionAddr) {
		t.Errorf("GET / body missing opted-in node's onion address %q", onionAddr)
	}

	_, detailBody := getBody(t, srv.URL+"/nodes/"+node.ID.String())
	if !strings.Contains(detailBody, clearnetAddr) {
		t.Errorf("GET /nodes/%s body missing opted-in node's clearnet address %q", node.ID, clearnetAddr)
	}
	if !strings.Contains(detailBody, onionAddr) {
		t.Errorf("GET /nodes/%s body missing opted-in node's onion address %q", node.ID, onionAddr)
	}
}

// TestP2PNodeWithMultipleAddressesHidesAllAddresses asserts the privacy
// contract holds even when a p2p_discovered node has multiple known
// addresses recorded: neither address string appears anywhere in the
// dashboard or node detail page body.
func TestP2PNodeWithMultipleAddressesHidesAllAddresses(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const clearnetAddr = "1.2.3.4:18142"
	const onionAddr = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142"
	pubkey := []byte("p2p-multi-addr-pubkey")

	node, err := store.UpsertConfirmedNode(ctx, clearnetAddr, pubkey, storage.DiscoverySourceP2P)
	if err != nil {
		t.Fatalf("upsert confirmed clearnet: %v", err)
	}
	if _, err := store.UpsertConfirmedNode(ctx, onionAddr, pubkey, storage.DiscoverySourceP2P); err != nil {
		t.Fatalf("upsert confirmed onion: %v", err)
	}

	srv := newTestServer(t, store)

	_, dashboardBody := getBody(t, srv.URL+"/")
	if strings.Contains(dashboardBody, clearnetAddr) {
		t.Errorf("GET / body contains p2p node's clearnet address %q", clearnetAddr)
	}
	if strings.Contains(dashboardBody, onionAddr) {
		t.Errorf("GET / body contains p2p node's onion address %q", onionAddr)
	}

	_, detailBody := getBody(t, srv.URL+"/nodes/"+node.ID.String())
	if strings.Contains(detailBody, clearnetAddr) {
		t.Errorf("GET /nodes/%s body contains its own clearnet address %q", node.ID, clearnetAddr)
	}
	if strings.Contains(detailBody, onionAddr) {
		t.Errorf("GET /nodes/%s body contains its own onion address %q", node.ID, onionAddr)
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

// TestTopologyGraphPage asserts GET /topology returns 200, renders the
// vis-network CDN script tag and the graph container, and never embeds a
// p2p_discovered node's raw address into the page — the page fetches
// node/edge data client-side from the already-scrubbed GET /api/topology
// endpoint rather than the handler passing raw Go data into the template,
// but this test still guards against a future regression that starts
// passing node data server-side.
func TestTopologyGraphPage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const p2pAddr = "1.2.3.4:18142"
	if _, err := store.UpsertDiscoveredNode(ctx, p2pAddr, storage.DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert p2p node: %v", err)
	}

	srv := newTestServer(t, store)
	status, body := getBody(t, srv.URL+"/topology")
	if status != http.StatusOK {
		t.Fatalf("GET /topology status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, "vis-network") {
		t.Errorf("GET /topology body missing vis-network script tag")
	}
	if !strings.Contains(body, `id="graph"`) {
		t.Errorf("GET /topology body missing graph container element")
	}
	if !strings.Contains(body, "/api/topology") {
		t.Errorf("GET /topology body missing client-side fetch of /api/topology")
	}
	if strings.Contains(body, p2pAddr) {
		t.Errorf("GET /topology body contains p2p node's raw address %q", p2pAddr)
	}
}

// TestSubmissionsPage asserts GET /admin/submissions (with valid admin
// Basic Auth credentials) renders the pending submissions in a plain
// HTML table with approve/reject htmx buttons, and that a
// fully-reviewed (non-pending) submission does not show up.
func TestSubmissionsPage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const pendingAddr = "1.2.3.4:18142"
	pending, err := store.CreatePendingSubmission(ctx, pendingAddr, nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}
	rejected, err := store.CreatePendingSubmission(ctx, "5.6.7.8:18142", nil, nil)
	if err != nil {
		t.Fatalf("create rejected submission: %v", err)
	}
	if err := store.RejectPendingSubmission(ctx, rejected.ID, nil); err != nil {
		t.Fatalf("reject: %v", err)
	}

	srv := newTestServer(t, store)
	status, body, _ := getBodyWithAuth(t, srv.URL+"/admin/submissions", testAdminUser, testAdminPassword)
	if status != http.StatusOK {
		t.Fatalf("GET /admin/submissions status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, pendingAddr) {
		t.Errorf("GET /admin/submissions body missing pending submission's address %q", pendingAddr)
	}
	if !strings.Contains(body, fmt.Sprintf("/api/admin/submissions/%s/approve", pending.ID)) {
		t.Errorf("GET /admin/submissions body missing approve htmx action for %s", pending.ID)
	}
	if !strings.Contains(body, fmt.Sprintf("/api/admin/submissions/%s/reject", pending.ID)) {
		t.Errorf("GET /admin/submissions body missing reject htmx action for %s", pending.ID)
	}
	if strings.Contains(body, "5.6.7.8:18142") {
		t.Errorf("GET /admin/submissions body contains an already-rejected (non-pending) submission's address")
	}
	if !strings.Contains(body, "not yet probed") {
		t.Errorf("GET /admin/submissions body missing the not-yet-probed indicator for %s", pending.ID)
	}

	if err := store.RecordSubmissionProbeResult(ctx, pending.ID, false); err != nil {
		t.Fatalf("record submission probe result: %v", err)
	}
	_, body, _ = getBodyWithAuth(t, srv.URL+"/admin/submissions", testAdminUser, testAdminPassword)
	if !strings.Contains(body, "unreachable") {
		t.Errorf("GET /admin/submissions body missing unreachable probe result for %s", pending.ID)
	}
}

// TestAdminSubmissionsRequiresAuth asserts GET /admin/submissions 401s
// with a WWW-Authenticate header both when no Authorization header is
// supplied and when the wrong credentials are supplied, and only
// succeeds (200) with the correct configured credentials.
func TestAdminSubmissionsRequiresAuth(t *testing.T) {
	store := newTestStore(t)
	srv := newTestServer(t, store)

	// No Authorization header at all.
	status, _ := getBody(t, srv.URL+"/admin/submissions")
	if status != http.StatusUnauthorized {
		t.Errorf("no-auth GET /admin/submissions status = %d, want %d", status, http.StatusUnauthorized)
	}
	resp, err := http.Get(srv.URL + "/admin/submissions")
	if err != nil {
		t.Fatalf("GET /admin/submissions: %v", err)
	}
	resp.Body.Close()
	if wa := resp.Header.Get("WWW-Authenticate"); !strings.Contains(wa, "Basic") {
		t.Errorf("no-auth GET /admin/submissions WWW-Authenticate = %q, want it to contain %q", wa, "Basic")
	}

	// Wrong credentials.
	status, _, headers := getBodyWithAuth(t, srv.URL+"/admin/submissions", "wrong-user", "wrong-password")
	if status != http.StatusUnauthorized {
		t.Errorf("wrong-auth GET /admin/submissions status = %d, want %d", status, http.StatusUnauthorized)
	}
	if wa := headers.Get("WWW-Authenticate"); !strings.Contains(wa, "Basic") {
		t.Errorf("wrong-auth GET /admin/submissions WWW-Authenticate = %q, want it to contain %q", wa, "Basic")
	}

	// Correct credentials.
	status, _, _ = getBodyWithAuth(t, srv.URL+"/admin/submissions", testAdminUser, testAdminPassword)
	if status != http.StatusOK {
		t.Errorf("correct-auth GET /admin/submissions status = %d, want %d", status, http.StatusOK)
	}
}

// TestAdminSubmissionsDisabledWhenCredsNotConfigured asserts that when
// web.NewHandler is built with an unconfigured (zero-value)
// adminauth.Credentials — simulating NETMAP_ADMIN_USER/
// NETMAP_ADMIN_PASSWORD being unset — GET /admin/submissions returns 503
// for every request, even one supplying a plausible-looking guessed
// Basic Auth header, and specifically NOT 401/200/404.
func TestAdminSubmissionsDisabledWhenCredsNotConfigured(t *testing.T) {
	store := newTestStore(t)
	srv := newTestServerWithCreds(t, store, adminauth.Credentials{})

	// No Authorization header.
	status, _ := getBody(t, srv.URL+"/admin/submissions")
	if status != http.StatusServiceUnavailable {
		t.Errorf("no-auth GET /admin/submissions status = %d, want %d", status, http.StatusServiceUnavailable)
	}

	// A plausible guessed credential pair must not slip through.
	for _, creds := range [][2]string{{"admin", "admin"}, {"admin", ""}} {
		status, _, _ := getBodyWithAuth(t, srv.URL+"/admin/submissions", creds[0], creds[1])
		if status != http.StatusServiceUnavailable {
			t.Errorf("guessed-auth (%q/%q) GET /admin/submissions status = %d, want %d", creds[0], creds[1], status, http.StatusServiceUnavailable)
		}
		if status == http.StatusUnauthorized || status == http.StatusOK || status == http.StatusNotFound {
			t.Errorf("guessed-auth (%q/%q) GET /admin/submissions status = %d, must not be 401/200/404", creds[0], creds[1], status)
		}
	}
}

// TestNonAdminRoutesRemainUnauthenticated is the most important
// regression test in this dispatch: it proves the adminauth gate is
// wired ONLY around /admin/*, not more broadly, by hitting every
// non-admin web route with zero Authorization header (even though this
// test's server IS built with admin credentials configured) and
// asserting each still succeeds exactly as before.
func TestNonAdminRoutesRemainUnauthenticated(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	node, err := store.UpsertDiscoveredNode(ctx, "1.2.3.4:18142", storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	srv := newTestServer(t, store)

	status, _ := getBody(t, srv.URL+"/")
	if status != http.StatusOK {
		t.Errorf("no-auth GET / status = %d, want %d", status, http.StatusOK)
	}
	status, _ = getBody(t, srv.URL+"/topology")
	if status != http.StatusOK {
		t.Errorf("no-auth GET /topology status = %d, want %d", status, http.StatusOK)
	}
	status, _ = getBody(t, srv.URL+"/nodes/"+node.ID.String())
	if status != http.StatusOK {
		t.Errorf("no-auth GET /nodes/%s status = %d, want %d", node.ID, status, http.StatusOK)
	}
	status, _ = getBody(t, srv.URL+"/static/style.css")
	if status != http.StatusOK {
		t.Errorf("no-auth GET /static/style.css status = %d, want %d", status, http.StatusOK)
	}
}

// TestDashboardNodePagination asserts the dashboard's node table honors
// `?page=`, rendering the correct subset of nodes per page and the
// correct Prev/Next link presence/absence at the first/last page
// boundaries: page 1 must have no Prev link, and the last page must have
// no Next link.
func TestDashboardNodePagination(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Seed 5 nodes with addresses that sort n1..n5 (ListNodes orders by
	// address), and a small page size via ?limit= so 2 pages of 3 (3+2)
	// exercise the boundary without needing 50+ real nodes.
	for _, addr := range []string{"n1:1", "n2:1", "n3:1", "n4:1", "n5:1"} {
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil); err != nil {
			t.Fatalf("upsert %s: %v", addr, err)
		}
	}

	srv := newTestServer(t, store)

	// Page 1 of limit=3: n1, n2, n3. No Prev link, but a Next link (to
	// page 2) must be present.
	status, page1Body := getBody(t, srv.URL+"/?page=1&limit=3")
	if status != http.StatusOK {
		t.Fatalf("GET /?page=1&limit=3 status = %d, want %d", status, http.StatusOK)
	}
	if strings.Contains(page1Body, `href="/?page=0"`) {
		t.Errorf("GET /?page=1 body contains a Prev link to page 0, want none")
	}
	if !strings.Contains(page1Body, `href="/?page=2"`) {
		t.Errorf("GET /?page=1 body missing a Next link to page 2")
	}
	if !strings.Contains(page1Body, "Showing 1-3 of 5 nodes") {
		t.Errorf("GET /?page=1 body missing the range summary %q:\n%s", "Showing 1-3 of 5 nodes", page1Body)
	}

	// Page 2 of limit=3: n4, n5 (partial page). Must have a Prev link
	// back to page 1, and no Next link (this is the last page).
	status, page2Body := getBody(t, srv.URL+"/?page=2&limit=3")
	if status != http.StatusOK {
		t.Fatalf("GET /?page=2&limit=3 status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(page2Body, `href="/?page=1"`) {
		t.Errorf("GET /?page=2 body missing a Prev link to page 1")
	}
	if strings.Contains(page2Body, `href="/?page=3"`) {
		t.Errorf("GET /?page=2 body contains a Next link to page 3, want none (last page)")
	}
	if !strings.Contains(page2Body, "Showing 4-5 of 5 nodes") {
		t.Errorf("GET /?page=2 body missing the range summary %q:\n%s", "Showing 4-5 of 5 nodes", page2Body)
	}

	// The summary counts (Total nodes card) must reflect the WHOLE
	// population (5), not just the current page's size (3 or 2), on
	// every page.
	if !strings.Contains(page1Body, `<div class="card-value">5</div>`) {
		t.Errorf("GET /?page=1 body's Total nodes card doesn't show the whole population (5)")
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
