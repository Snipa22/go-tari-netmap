package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/Snipa22/go-tari-lib/p2p"
	rpcpkg "github.com/Snipa22/go-tari-lib/p2p/rpc"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// TestOnConnectionAcceptedIncrementsMetric through TestOnSubstreamProtocolDeclinedIncrementsMetric
// exercise the standalone callback functions wired directly as p2p.ResponderConfig's
// On*/observability fields in main.go (see metrics.go) -- each just increments/observes the
// matching Prometheus metric, so these are plain "call it, check the delta" unit tests, using
// testutil.ToFloat64 to read the counter/vec value before/after (a delta rather than an
// absolute assertion, since these are process-global prometheus.DefaultRegisterer metrics
// shared across every test in this package/run).

func TestOnConnectionAcceptedIncrementsMetric(t *testing.T) {
	before := testutil.ToFloat64(netmapP2PResponderConnectionsAccepted)
	onConnectionAccepted(testAddr("203.0.113.1:1"))
	after := testutil.ToFloat64(netmapP2PResponderConnectionsAccepted)
	if after != before+1 {
		t.Errorf("netmap_p2p_responder_connections_accepted_total = %v, want %v", after, before+1)
	}
}

func TestOnHandshakeResultIncrementsMetric(t *testing.T) {
	beforeSuccess := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("success"))
	beforeFailure := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("failure"))

	onHandshakeResult(testAddr("203.0.113.2:1"), true)
	onHandshakeResult(testAddr("203.0.113.3:1"), false)

	if got := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("success")); got != beforeSuccess+1 {
		t.Errorf("handshake_result_total{result=success} = %v, want %v", got, beforeSuccess+1)
	}
	if got := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("failure")); got != beforeFailure+1 {
		t.Errorf("handshake_result_total{result=failure} = %v, want %v", got, beforeFailure+1)
	}
}

func TestOnIdentityExchangeResultIncrementsMetric(t *testing.T) {
	beforeSuccess := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("success"))
	beforeFailure := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("failure"))

	onIdentityExchangeResult(testAddr("203.0.113.4:1"), true)
	onIdentityExchangeResult(testAddr("203.0.113.5:1"), false)

	if got := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("success")); got != beforeSuccess+1 {
		t.Errorf("identity_exchange_result_total{result=success} = %v, want %v", got, beforeSuccess+1)
	}
	if got := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("failure")); got != beforeFailure+1 {
		t.Errorf("identity_exchange_result_total{result=failure} = %v, want %v", got, beforeFailure+1)
	}
}

func TestOnGetPeersServedIncrementsMetricAndObservesHistogram(t *testing.T) {
	beforeServed := testutil.ToFloat64(netmapP2PResponderGetPeersServed)
	beforeSampleCount := histogramSampleCount(t, netmapP2PResponderGetPeersPeerCount)

	onGetPeersServed(testAddr("203.0.113.6:1"), 7)

	if got := testutil.ToFloat64(netmapP2PResponderGetPeersServed); got != beforeServed+1 {
		t.Errorf("get_peers_served_total = %v, want %v", got, beforeServed+1)
	}
	if got := histogramSampleCount(t, netmapP2PResponderGetPeersPeerCount); got != beforeSampleCount+1 {
		t.Errorf("get_peers_peer_count sample count = %d, want %d", got, beforeSampleCount+1)
	}
}

// histogramSampleCount reads a Histogram metric's cumulative observation count via its Write
// method (the same mechanism promhttp.Handler() itself uses internally to serialize a
// Histogram) -- testutil.CollectAndCount is NOT usable here since it counts distinct metric
// *series* (always 1 for an unlabeled Histogram), not the number of Observe calls.
func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("writing histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestOnSubstreamProtocolDeclinedIncrementsMetric(t *testing.T) {
	before := testutil.ToFloat64(netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues("t/msg/0.1"))
	onSubstreamProtocolDeclined(testAddr("203.0.113.7:1"), []byte("t/msg/0.1"))
	after := testutil.ToFloat64(netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues("t/msg/0.1"))
	if after != before+1 {
		t.Errorf("substream_protocol_declined_total{protocol=\"t/msg/0.1\"} = %v, want %v", after, before+1)
	}
}

// TestOnPeerIdentityIncrementsDBWriteMetrics exercises the DB-write result counters wired
// directly at dbBackedResponder's storage.Store call sites (responder.go), against the real
// test database (same pattern as responder_test.go's newTestStore).
func TestOnPeerIdentityIncrementsDBWriteMetrics(t *testing.T) {
	store := newTestStore(t)
	r := &dbBackedResponder{store: store, logf: t.Logf}

	beforeUpsertSuccess := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationUpsertNode, "success"))
	beforeHealthSuccess := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationRecordHealth, "success"))

	r.onPeerIdentity(testAddr("203.0.113.80:41000"), []byte{0x90}, &p2p.PeerInfo{
		RemoteStaticPubKey: []byte{0x90},
		Features:           p2p.FeaturesCommunicationNode,
	})

	if got := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationUpsertNode, "success")); got != beforeUpsertSuccess+1 {
		t.Errorf("db_write_result_total{operation=upsert_node,result=success} = %v, want %v", got, beforeUpsertSuccess+1)
	}
	if got := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationRecordHealth, "success")); got != beforeHealthSuccess+1 {
		t.Errorf("db_write_result_total{operation=record_health,result=success} = %v, want %v", got, beforeHealthSuccess+1)
	}
}

// failingStore wraps a real storage.Store, delegating every method to it except
// UpsertConfirmedNode, which always fails -- used to exercise onPeerIdentity's
// failure-path metric recording without needing to actually break the test database.
type failingStore struct {
	storage.Store
}

func (f *failingStore) UpsertConfirmedNode(ctx context.Context, address string, publicKey []byte, discoverySource storage.DiscoverySource) (storage.Node, error) {
	return storage.Node{}, errors.New("simulated UpsertConfirmedNode failure")
}

// TestOnPeerIdentityRecordsDBWriteFailureMetric exercises the failure-path branch of the
// upsert_node DB-write metric: a failing store must increment
// db_write_result_total{operation="upsert_node",result="failure"} and must NOT reach
// RecordHealthCheck at all (onPeerIdentity returns early on the first UpsertConfirmedNode
// failure, see responder.go).
func TestOnPeerIdentityRecordsDBWriteFailureMetric(t *testing.T) {
	store := newTestStore(t)
	r := &dbBackedResponder{store: &failingStore{Store: store}, logf: t.Logf}

	beforeUpsertFailure := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationUpsertNode, "failure"))
	beforeHealthSuccess := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationRecordHealth, "success"))

	r.onPeerIdentity(testAddr("203.0.113.81:41000"), []byte{0x91}, &p2p.PeerInfo{
		RemoteStaticPubKey: []byte{0x91},
		Features:           p2p.FeaturesCommunicationNode,
	})

	if got := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationUpsertNode, "failure")); got != beforeUpsertFailure+1 {
		t.Errorf("db_write_result_total{operation=upsert_node,result=failure} = %v, want %v", got, beforeUpsertFailure+1)
	}
	if got := testutil.ToFloat64(netmapP2PResponderDBWriteResult.WithLabelValues(dbOperationRecordHealth, "success")); got != beforeHealthSuccess {
		t.Errorf("db_write_result_total{operation=record_health,result=success} = %v, want unchanged %v (RecordHealthCheck must never be reached after a failed UpsertConfirmedNode)", got, beforeHealthSuccess)
	}
}

// TestServeWiresObservabilityCallbacksOverLoopback is this package's version of BRIEF4.md's
// "reuse this repo's existing p2p.Serve-over-loopback test pattern... dial with a fake client,
// assert the metric moved" testing requirement: it runs a REAL p2p.Serve loop configured with
// this binary's exact observability callbacks (mirroring main.go's cfg wiring) on a loopback
// listener, dials it with go-tari-lib's own probe clients, and asserts every metric these
// callbacks feed actually moved.
func TestServeWiresObservabilityCallbacksOverLoopback(t *testing.T) {
	store := newTestStore(t)
	responder := &dbBackedResponder{store: store, logf: t.Logf}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting loopback listener: %v", err)
	}
	defer listener.Close()

	staticKeypair, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		t.Fatalf("generating responder static keypair: %v", err)
	}

	cfg := p2p.ResponderConfig{
		StaticKeypair:               staticKeypair,
		OurFeatures:                 p2p.FeaturesCommunicationNode,
		PeerListProvider:            responder.peerListProvider,
		OnPeerIdentity:              responder.onPeerIdentity,
		Logf:                        t.Logf,
		OnConnectionAccepted:        onConnectionAccepted,
		OnHandshakeResult:           onHandshakeResult,
		OnIdentityExchangeResult:    onIdentityExchangeResult,
		OnGetPeersServed:            onGetPeersServed,
		OnSubstreamProtocolDeclined: onSubstreamProtocolDeclined,
	}

	beforeAccepted := testutil.ToFloat64(netmapP2PResponderConnectionsAccepted)
	beforeHandshakeSuccess := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("success"))
	beforeIdentitySuccess := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("success"))
	beforeGetPeersServed := testutil.ToFloat64(netmapP2PResponderGetPeersServed)
	beforeDeclined := testutil.ToFloat64(netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues(string(rpcpkg.BlockSyncProtocolID)))

	serveCtx, serveCancel := context.WithCancel(context.Background())
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- p2p.Serve(serveCtx, listener, cfg)
	}()
	defer func() {
		serveCancel()
		listener.Close()
		<-serveErrCh
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connection 1: a real get_peers round trip -- exercises accept, handshake success,
	// identity exchange success, and a served (empty, since no confirmed/reachable nodes
	// exist yet) get_peers response.
	if _, err := p2p.ProbeGetPeersWithOptions(ctx, listener.Addr().String(), rpcpkg.GetPeersRequest{N: 50}, p2p.ProbeOptions{}); err != nil {
		t.Fatalf("ProbeGetPeersWithOptions: %v", err)
	}

	// Connection 2: ProbeChainMetadataWithOptions negotiates t/blksync/1 -- a protocol this
	// responder does NOT support (it only serves t/dht/1, see responder.go/main.go) -- so this
	// exercises OnSubstreamProtocolDeclined without needing any unexported go-tari-lib helper.
	if _, err := p2p.ProbeChainMetadataWithOptions(ctx, listener.Addr().String(), p2p.ProbeOptions{}); err == nil {
		t.Fatalf("expected ProbeChainMetadataWithOptions to fail against a responder that only supports t/dht/1")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if testutil.ToFloat64(netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues(string(rpcpkg.BlockSyncProtocolID))) > beforeDeclined || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := testutil.ToFloat64(netmapP2PResponderConnectionsAccepted); got != beforeAccepted+2 {
		t.Errorf("connections_accepted_total = %v, want %v", got, beforeAccepted+2)
	}
	if got := testutil.ToFloat64(netmapP2PResponderHandshakeResult.WithLabelValues("success")); got != beforeHandshakeSuccess+2 {
		t.Errorf("handshake_result_total{result=success} = %v, want %v", got, beforeHandshakeSuccess+2)
	}
	if got := testutil.ToFloat64(netmapP2PResponderIdentityExchangeResult.WithLabelValues("success")); got != beforeIdentitySuccess+2 {
		t.Errorf("identity_exchange_result_total{result=success} = %v, want %v", got, beforeIdentitySuccess+2)
	}
	if got := testutil.ToFloat64(netmapP2PResponderGetPeersServed); got != beforeGetPeersServed+1 {
		t.Errorf("get_peers_served_total = %v, want %v (only connection 1 completed get_peers)", got, beforeGetPeersServed+1)
	}
	if got := testutil.ToFloat64(netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues(string(rpcpkg.BlockSyncProtocolID))); got != beforeDeclined+1 {
		t.Errorf("substream_protocol_declined_total{protocol=%q} = %v, want %v", rpcpkg.BlockSyncProtocolID, got, beforeDeclined+1)
	}
}

// TestHealthzOK exercises newMetricsServer's /healthz handler against a real, reachable test
// database: it must return 200 with {"status":"ok"}.
func TestHealthzOK(t *testing.T) {
	store := newTestStore(t)
	server := newMetricsServer(store)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body healthzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "ok" {
		t.Errorf("status field = %q, want %q", body.Status, "ok")
	}
}

// unreachableStore is a storage.Store whose Ping always fails -- used to exercise /healthz's
// 503 branch without needing to actually take down the test database.
type unreachableStore struct {
	storage.Store
}

func (u *unreachableStore) Ping(ctx context.Context) error {
	return errors.New("simulated: database unreachable")
}

// TestHealthzUnreachableDB exercises newMetricsServer's /healthz handler when store.Ping fails:
// it must return 503, never 200, and never touch the P2P listener (there is none in this test
// at all -- this endpoint's whole point is to be checkable independent of the P2P listener).
func TestHealthzUnreachableDB(t *testing.T) {
	store := newTestStore(t)
	server := newMetricsServer(&unreachableStore{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// TestMetricsEndpointServesPrometheusFormat exercises newMetricsServer's /metrics handler,
// confirming it serves promhttp.Handler()'s real Prometheus exposition format, including at
// least one of this binary's own custom metrics by name.
func TestMetricsEndpointServesPrometheusFormat(t *testing.T) {
	store := newTestStore(t)
	server := newMetricsServer(store)

	// Increment something first so its HELP/TYPE lines are guaranteed to be emitted (a fresh
	// CounterVec with zero label combinations used yet emits nothing).
	onConnectionAccepted(testAddr("203.0.113.90:1"))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !containsSubstring(body, "netmap_p2p_responder_connections_accepted_total") {
		t.Errorf("/metrics response missing netmap_p2p_responder_connections_accepted_total, got:\n%s", body)
	}
	if !containsSubstring(body, "go_goroutines") {
		t.Errorf("/metrics response missing the standard Go collector's go_goroutines (see metrics.go's doc comment on relying on the default collectors), got:\n%s", body)
	}
}

func containsSubstring(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
