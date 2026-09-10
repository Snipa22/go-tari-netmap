package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// This file wires this binary's own operational stats (see BRIEF4.md) into Prometheus
// client_golang, following the exact convention already established by this fleet's
// go-tari-observability-p2p/cmd/tari-p2p-exporter/main.go: gauge/counter vecs registered at
// package init via prometheus.MustRegister, served on a plain net/http server via
// promhttp.Handler() at /metrics plus a minimal /healthz.
//
// Every metric here is incremented at the SAME call sites that already produce an equivalent
// log line today (see responder.go and main.go) -- this file only adds the counting, it does
// not change what gets logged.

var (
	// netmapP2PResponderConnectionsAccepted counts every inbound TCP accept, before any
	// handshake attempt -- incremented from ResponderConfig.OnConnectionAccepted (see main.go),
	// which go-tari-lib's p2p.Serve calls immediately after listener.Accept() succeeds.
	netmapP2PResponderConnectionsAccepted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_connections_accepted_total",
		Help: "Total number of inbound TCP connections accepted, before any Noise_XX handshake attempt.",
	})

	// netmapP2PResponderHandshakeResult counts every Noise_XX handshake outcome, labeled
	// success/failure -- incremented from ResponderConfig.OnHandshakeResult.
	netmapP2PResponderHandshakeResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_handshake_result_total",
		Help: "Total number of Noise_XX handshake outcomes, labeled by result (success|failure).",
	}, []string{"result"})

	// netmapP2PResponderIdentityExchangeResult counts every identity-exchange outcome, labeled
	// success/failure -- incremented from ResponderConfig.OnIdentityExchangeResult. Only
	// connections that already completed a successful handshake reach this stage.
	netmapP2PResponderIdentityExchangeResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_identity_exchange_result_total",
		Help: "Total number of identity-exchange outcomes, labeled by result (success|failure).",
	}, []string{"result"})

	// netmapP2PResponderGetPeersServed counts every successful get_peers response sent,
	// regardless of how many peers it contained -- incremented from
	// ResponderConfig.OnGetPeersServed.
	netmapP2PResponderGetPeersServed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_get_peers_served_total",
		Help: "Total number of successful get_peers responses sent, regardless of how many peers each contained.",
	})

	// netmapP2PResponderGetPeersPeerCount observes how many peers were included in each
	// get_peers response -- lets an operator see whether the DB-backed peer list is actually
	// populated over time, or usually empty. Buckets span "definitely empty" (0) up through
	// this binary's own maxServedPeers ceiling (200).
	netmapP2PResponderGetPeersPeerCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "netmap_p2p_responder_get_peers_peer_count",
		Help:    "Number of peers included per get_peers response.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 150, maxServedPeers},
	})

	// netmapP2PResponderSubstreamProtocolDeclined counts every NOT_SUPPORTED response, labeled
	// by the requested protocol id string (e.g. "t/msg/0.1", "t/blksync/1") -- incremented from
	// ResponderConfig.OnSubstreamProtocolDeclined. Tells us exactly what real peers are trying
	// to do with us and confirms we're correctly declining the right things.
	netmapP2PResponderSubstreamProtocolDeclined = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_substream_protocol_declined_total",
		Help: "Total number of substream protocol negotiations we declined (NOT_SUPPORTED), labeled by the requested protocol id.",
	}, []string{"protocol"})

	// netmapP2PResponderDBWriteResult counts every UpsertConfirmedNode/RecordHealthCheck call
	// outcome, labeled by operation and result -- incremented from dbBackedResponder's own
	// storage.Store call sites (see responder.go). This is the most operationally important
	// metric here: a nonzero, growing connections_accepted_total with near-zero
	// identity_exchange_result_total{result="success"} means "nobody's really talking to us
	// yet"; growing identity-exchange successes with matching db_write successes means real
	// peers ARE finding and exchanging identity with us.
	netmapP2PResponderDBWriteResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "netmap_p2p_responder_db_write_result_total",
		Help: "Total number of storage.Store write outcomes, labeled by operation (upsert_node|record_health) and result (success|failure).",
	}, []string{"operation", "result"})
)

func init() {
	prometheus.MustRegister(
		netmapP2PResponderConnectionsAccepted,
		netmapP2PResponderHandshakeResult,
		netmapP2PResponderIdentityExchangeResult,
		netmapP2PResponderGetPeersServed,
		netmapP2PResponderGetPeersPeerCount,
		netmapP2PResponderSubstreamProtocolDeclined,
		netmapP2PResponderDBWriteResult,
	)
	// Deliberately NOT registering a custom uptime gauge: prometheus.DefaultRegisterer already
	// has collectors.NewProcessCollector/NewGoCollector registered by client_golang's own
	// package init() (see prometheus/registry.go), which is exactly what
	// go-tari-observability-p2p/cmd/tari-p2p-exporter/main.go also relies on implicitly (it
	// never registers its own uptime gauge either) -- process_start_time_seconds from the
	// default ProcessCollector already covers "how long has this process been up" without a
	// redundant metric.
}

// resultLabel returns the Prometheus "result" label value (see the *Result counter vecs above)
// for success, following this fleet's success|failure label convention.
func resultLabel(success bool) string {
	if success {
		return "success"
	}
	return "failure"
}

// The functions below are wired directly as the corresponding p2p.ResponderConfig callback
// fields in main.go (OnConnectionAccepted, OnHandshakeResult, OnIdentityExchangeResult,
// OnGetPeersServed, OnSubstreamProtocolDeclined) -- each just increments/observes the matching
// metric above at exactly the call site go-tari-lib/p2p.Serve already logs an equivalent line
// from (see that package's responder.go doc comments on these same callback fields).

func onConnectionAccepted(_ net.Addr) {
	netmapP2PResponderConnectionsAccepted.Inc()
}

func onHandshakeResult(_ net.Addr, success bool) {
	netmapP2PResponderHandshakeResult.WithLabelValues(resultLabel(success)).Inc()
}

func onIdentityExchangeResult(_ net.Addr, success bool) {
	netmapP2PResponderIdentityExchangeResult.WithLabelValues(resultLabel(success)).Inc()
}

func onGetPeersServed(_ net.Addr, peerCount int) {
	netmapP2PResponderGetPeersServed.Inc()
	netmapP2PResponderGetPeersPeerCount.Observe(float64(peerCount))
}

func onSubstreamProtocolDeclined(_ net.Addr, protocol []byte) {
	netmapP2PResponderSubstreamProtocolDeclined.WithLabelValues(string(protocol)).Inc()
}

// dbOperationUpsertNode and dbOperationRecordHealth are the "operation" label values for
// netmap_p2p_responder_db_write_result_total, matching BRIEF4.md's exact naming
// (operation="upsert_node|record_health").
const (
	dbOperationUpsertNode   = "upsert_node"
	dbOperationRecordHealth = "record_health"
)

// healthzTimeout bounds the store.Ping call /healthz makes -- BRIEF4.md specifies "a short
// timeout (e.g. 2s)"; process liveness alone (the process is up and serving HTTP at all) is
// never sufficient on its own, since a wedged/unreachable Postgres pool is exactly the kind of
// problem this endpoint exists to surface.
const healthzTimeout = 2 * time.Second

// healthzResponse is the minimal JSON body BRIEF4.md specifies for /healthz.
type healthzResponse struct {
	Status string `json:"status"`
}

// newMetricsServer builds the *http.Server this binary serves /metrics and /healthz from, on
// its own listener, entirely separate from the P2P listener (see main.go's -metrics-addr flag
// and the loud "internal-only, never the public interface" warnings there and at the bind call
// site). store is used only for /healthz's store.Ping(ctx) reachability check -- this endpoint
// never touches the P2P listener itself.
func newMetricsServer(store storage.Store) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthzTimeout)
		defer cancel()

		if err := store.Ping(ctx); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(healthzResponse{Status: "error: " + err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(healthzResponse{Status: "ok"})
	})
	return &http.Server{Handler: mux}
}
