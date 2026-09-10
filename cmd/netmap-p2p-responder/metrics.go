package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// This file wires this binary's own operational stats (see BRIEF4.md) into Prometheus
// client_golang, following the exact convention already established by this fleet's
// go-tari-observability-p2p/cmd/tari-p2p-exporter/main.go: gauge/counter vecs served on a plain
// net/http server via promhttp.Handler() at /metrics plus a minimal /healthz.
//
// Every metric here is incremented at the SAME call sites that already produce an equivalent
// log line today (see responder.go and main.go) -- this file only adds the counting, it does
// not change what gets logged.
//
// # Network-scoped metric names (see BRIEF5.md)
//
// Both mainnet and testnet deployments of this exact same binary get scraped into a single
// shared Prometheus backend, so every metric name emitted here MUST be prefixed by which
// network produced it: netmap_mainnet_p2p_responder_* on the mainnet deployment,
// netmap_testnet_p2p_responder_* on testnet -- Alex's exact instruction. That prefix is only
// known once the required -network flag has been parsed (see main.go), which happens in run(),
// AFTER package init() would already have run -- so, unlike the single fixed-name package-level
// `var (...)` + `func init() { prometheus.MustRegister(...) }` this file used to have, every
// metric here is now built by newResponderMetrics(network), called from run() after
// flag.Parse(), and threaded explicitly into main.go's ResponderConfig callback wiring and
// dbBackedResponder (see responder.go) instead of being referenced as package-level vars.
//
// newResponderMetrics also registers onto its OWN dedicated *prometheus.Registry, rather than
// the global prometheus.DefaultRegisterer: metric names are now a runtime value (dynamic per
// network), so two different responderMetrics instances constructed within the same process
// (this happens routinely in this package's own tests, which exercise both "mainnet" and
// "testnet" -- see metrics_test.go) would otherwise either collide (same network value used
// twice -- prometheus.MustRegister panics on a duplicate registration) or simply accumulate
// unboundedly on the shared global registry across every test in a single `go test` run. A
// dedicated registry per responderMetrics sidesteps both problems entirely and is exactly as
// correct for a single real, long-lived process (which constructs exactly one responderMetrics,
// once, for the lifetime of the process). collectors.NewProcessCollector/NewGoCollector are
// registered explicitly alongside our own metrics on that same dedicated registry, so /metrics
// still emits process_start_time_seconds/go_goroutines etc., exactly as before when those came
// for free from prometheus.DefaultRegisterer's own implicit registration.
type responderMetrics struct {
	network  string
	registry *prometheus.Registry

	// connectionsAccepted counts every inbound TCP accept, before any handshake attempt --
	// incremented from onConnectionAccepted (see ResponderConfig.OnConnectionAccepted in
	// main.go), which go-tari-lib's p2p.Serve calls immediately after listener.Accept()
	// succeeds.
	connectionsAccepted prometheus.Counter

	// handshakeResult counts every Noise_XX handshake outcome, labeled success/failure --
	// incremented from onHandshakeResult (ResponderConfig.OnHandshakeResult).
	handshakeResult *prometheus.CounterVec

	// identityExchangeResult counts every identity-exchange outcome, labeled success/failure
	// -- incremented from onIdentityExchangeResult (ResponderConfig.OnIdentityExchangeResult).
	// Only connections that already completed a successful handshake reach this stage.
	identityExchangeResult *prometheus.CounterVec

	// getPeersServed counts every successful get_peers response sent, regardless of how many
	// peers it contained -- incremented from onGetPeersServed (ResponderConfig.OnGetPeersServed).
	getPeersServed prometheus.Counter

	// getPeersPeerCount observes how many peers were included in each get_peers response --
	// lets an operator see whether the DB-backed peer list is actually populated over time, or
	// usually empty. Buckets span "definitely empty" (0) up through this binary's own
	// maxServedPeers ceiling (200).
	getPeersPeerCount prometheus.Histogram

	// substreamProtocolDeclined counts every NOT_SUPPORTED response, labeled by the requested
	// protocol id string (e.g. "t/msg/0.1", "t/blksync/1") -- incremented from
	// onSubstreamProtocolDeclined (ResponderConfig.OnSubstreamProtocolDeclined). Tells us
	// exactly what real peers are trying to do with us and confirms we're correctly declining
	// the right things.
	substreamProtocolDeclined *prometheus.CounterVec

	// dbWriteResult counts every UpsertConfirmedNode/RecordHealthCheck call outcome, labeled by
	// operation and result -- incremented from dbBackedResponder's own storage.Store call sites
	// (see responder.go). This is the most operationally important metric here: a nonzero,
	// growing connections_accepted_total with near-zero identity_exchange_result_total{result=
	// "success"} means "nobody's really talking to us yet"; growing identity-exchange successes
	// with matching db_write successes means real peers ARE finding and exchanging identity
	// with us.
	dbWriteResult *prometheus.CounterVec
}

// validMainnetOrTestnet is the exact, closed set of values the required -network flag (see
// main.go) accepts -- no default, fail fast on anything else, mirroring this binary's existing
// fail-fast pattern for -public-tcp-addr/-onion3-addr (see parseAdvertisedAddresses).
var validMainnetOrTestnet = map[string]bool{"mainnet": true, "testnet": true}

// validateNetwork validates the -network flag's value: it must be exactly "mainnet" or
// "testnet", with no default -- an empty or unrecognized value is a startup-time error, never
// silently defaulted, since every metric name this binary emits is derived from it (see
// responderMetrics' doc comment).
func validateNetwork(network string) error {
	if network == "" {
		return fmt.Errorf("-network is required (must be %q or %q)", "mainnet", "testnet")
	}
	if !validMainnetOrTestnet[network] {
		return fmt.Errorf("-network %q is invalid: must be %q or %q", network, "mainnet", "testnet")
	}
	return nil
}

// newResponderMetrics validates network (see validateNetwork) and builds + registers every
// metric this binary exposes, each named netmap_<network>_p2p_responder_... (see
// responderMetrics' doc comment for the full "why network-scoped, why its own registry"
// rationale). Called from run(), after flag.Parse() -- never from a package-level var/init(),
// since the network value isn't known until then.
func newResponderMetrics(network string) (*responderMetrics, error) {
	if err := validateNetwork(network); err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("netmap_%s_p2p_responder_", network)
	m := &responderMetrics{network: network, registry: prometheus.NewRegistry()}

	m.connectionsAccepted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prefix + "connections_accepted_total",
		Help: "Total number of inbound TCP connections accepted, before any Noise_XX handshake attempt.",
	})
	m.handshakeResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + "handshake_result_total",
		Help: "Total number of Noise_XX handshake outcomes, labeled by result (success|failure).",
	}, []string{"result"})
	m.identityExchangeResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + "identity_exchange_result_total",
		Help: "Total number of identity-exchange outcomes, labeled by result (success|failure).",
	}, []string{"result"})
	m.getPeersServed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prefix + "get_peers_served_total",
		Help: "Total number of successful get_peers responses sent, regardless of how many peers each contained.",
	})
	m.getPeersPeerCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prefix + "get_peers_peer_count",
		Help:    "Number of peers included per get_peers response.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 150, maxServedPeers},
	})
	m.substreamProtocolDeclined = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + "substream_protocol_declined_total",
		Help: "Total number of substream protocol negotiations we declined (NOT_SUPPORTED), labeled by the requested protocol id.",
	}, []string{"protocol"})
	m.dbWriteResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + "db_write_result_total",
		Help: "Total number of storage.Store write outcomes, labeled by operation (upsert_node|record_health) and result (success|failure).",
	}, []string{"operation", "result"})

	m.registry.MustRegister(
		m.connectionsAccepted,
		m.handshakeResult,
		m.identityExchangeResult,
		m.getPeersServed,
		m.getPeersPeerCount,
		m.substreamProtocolDeclined,
		m.dbWriteResult,
		// Deliberately registered explicitly here (see this struct's doc comment) rather than
		// relying on prometheus.DefaultRegisterer's own implicit package-init registration of
		// these two, since we're no longer using DefaultRegisterer at all -- process_start_
		// time_seconds/go_goroutines etc. must still show up on /metrics.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	return m, nil
}

// resultLabel returns the Prometheus "result" label value (see the *Result counter vecs above)
// for success, following this fleet's success|failure label convention.
func resultLabel(success bool) string {
	if success {
		return "success"
	}
	return "failure"
}

// The methods below are wired directly as the corresponding p2p.ResponderConfig callback fields
// in main.go (OnConnectionAccepted, OnHandshakeResult, OnIdentityExchangeResult,
// OnGetPeersServed, OnSubstreamProtocolDeclined) via closures over a single *responderMetrics
// built once in run() -- each just increments/observes the matching metric field above at
// exactly the call site go-tari-lib/p2p.Serve already logs an equivalent line from (see that
// package's responder.go doc comments on these same callback fields).

func (m *responderMetrics) onConnectionAccepted(_ net.Addr) {
	m.connectionsAccepted.Inc()
}

func (m *responderMetrics) onHandshakeResult(_ net.Addr, success bool) {
	m.handshakeResult.WithLabelValues(resultLabel(success)).Inc()
}

func (m *responderMetrics) onIdentityExchangeResult(_ net.Addr, success bool) {
	m.identityExchangeResult.WithLabelValues(resultLabel(success)).Inc()
}

func (m *responderMetrics) onGetPeersServed(_ net.Addr, peerCount int) {
	m.getPeersServed.Inc()
	m.getPeersPeerCount.Observe(float64(peerCount))
}

func (m *responderMetrics) onSubstreamProtocolDeclined(_ net.Addr, protocol []byte) {
	m.substreamProtocolDeclined.WithLabelValues(string(protocol)).Inc()
}

// dbOperationUpsertNode and dbOperationRecordHealth are the "operation" label values for
// dbWriteResult, matching BRIEF4.md's exact naming (operation="upsert_node|record_health").
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
// never touches the P2P listener itself. metrics' own dedicated registry (see
// responderMetrics' doc comment) backs /metrics via promhttp.HandlerFor, rather than
// promhttp.Handler() (which serves prometheus.DefaultGatherer/DefaultRegisterer) -- this binary
// no longer uses the default registry at all, see newResponderMetrics.
func newMetricsServer(store storage.Store, metrics *responderMetrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{}))
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
