// Command netmap is the entrypoint for the go-tari-netmap HTTP server.
//
// # Network flag (mainnet/testnet)
//
// This exact same binary is deployed unchanged to both a mainnet host (netmap.service) and a
// testnet host (netmap-testnet.service), and both get scraped into a single shared Prometheus
// backend -- so every metric this binary exposes at /metrics is prefixed by which network
// produced it (see metrics.go's doc comment). The required -network flag (values:
// mainnet|testnet, no default) drives that prefix; this binary fails fast at startup if it's
// unset or an unrecognized value:
//
//	-network mainnet   e.g. netmap.service on CT102 :8080
//	-network testnet    e.g. netmap-testnet.service on CT102 :8081
//
// # Metrics listener (-metrics-addr)
//
// This binary's existing -addr listener (e.g. 192.168.40.51:8080/:8081) is PROXIED -- Caddy
// sits in front of it to serve the public dashboard -- so /metrics and /healthz are served on
// a SEPARATE, opt-in -metrics-addr listener instead, exactly mirroring
// cmd/netmap-p2p-responder's own -metrics-addr (see metrics.go's doc comment for the full
// rationale and that binary's identical flag). Empty/unset (the default) disables it entirely.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Snipa22/go-tari-netmap/internal/adminauth"
	"github.com/Snipa22/go-tari-netmap/internal/api"
	"github.com/Snipa22/go-tari-netmap/internal/collector"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
	"github.com/Snipa22/go-tari-netmap/internal/web"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address (dashboard + JSON API) -- this is normally PROXIED (e.g. by Caddy) to serve the public dashboard; /metrics and /healthz are deliberately NOT served here, see -metrics-addr.")
	network := flag.String("network", "", "REQUIRED, no default: which deployed instance of this binary this is -- \"mainnet\" or \"testnet\". "+
		"Both networks' deployments of this exact same binary get scraped into one shared Prometheus backend, so every metric name this binary exposes at /metrics is prefixed netmap_<network>_collector_.../netmap_<network>_api_... "+
		"Fails fast at startup if unset or not exactly one of these two values. This is distinct from NETMAP_NETWORK_BYTE below, which selects the actual Tari wire-protocol network the collector's P2P client dials -- see that flag's doc comment.")
	metricsAddr := flag.String("metrics-addr", "", "address to serve Prometheus /metrics + /healthz on (default empty = disabled, opt-in). "+
		"CRITICAL: bind to an INTERNAL-ONLY address, e.g. 192.168.40.x:PORT or 127.0.0.1:PORT -- NEVER the address Caddy proxies -addr from. "+
		"This is a SEPARATE listener from -addr (which is proxied) -- if you set this at all, prefer a loopback-only address such as 127.0.0.1:9472; do NOT use a bare :PORT form (binds ALL interfaces).")
	flag.Parse()

	metrics, err := newNetmapMetrics(*network)
	if err != nil {
		log.Fatalf("netmap: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := storage.DSNFromEnv()
	store, err := storage.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to storage: %v", err)
	}
	defer store.Close()

	// The base schema migration must succeed for the binary to start; the
	// optional TimescaleDB hypertable step is allowed to fail (logged, not
	// fatal) — see internal/storage/migrations for why.
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}

	// Real go-tari-grpc-lib-backed client: talks to Tari base nodes over
	// gRPC. Dials fresh per-call (see grpcNodeClient's doc comment in
	// internal/collector/grpc_client.go for why that's fine here).
	grpcClient := collector.NewGRPCClient()

	// Real go-tari-lib/p2p-backed client: talks to Tari nodes over the
	// direct comms/RPC-over-P2P transport, independent of gRPC — see
	// p2pNodeClient's doc comment in internal/collector/p2p_client.go.
	//
	// NETMAP_SOCKS_PROXY_ADDR is the "host:port" address of a Tor SOCKS5
	// proxy (e.g. a local Tor daemon's SocksPort, typically
	// 127.0.0.1:9050), used to reach `.onion` Tari peers over this
	// transport. Empty/unset (the default) disables it, matching the
	// pre-existing zero-config behavior.
	socksProxyAddr := os.Getenv("NETMAP_SOCKS_PROXY_ADDR")

	// NETMAP_NETWORK_BYTE is a decimal string representation of the raw
	// Tari P2P wire network byte (e.g. "0" for MainNet, "38" for
	// Esmeralda since p2p.NetworkByteEsmeralda = 0x26 = 38 decimal),
	// letting a second deployed instance of this binary monitor a
	// different Tari network's peers. Empty/unset (the default) selects
	// MainNet (0x00), matching the pre-existing zero-config behavior.
	var networkByte byte
	if v := os.Getenv("NETMAP_NETWORK_BYTE"); v != "" {
		n, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			log.Fatalf("invalid NETMAP_NETWORK_BYTE %q: %v", v, err)
		}
		networkByte = byte(n)
	}
	p2pClient := collector.NewP2PClientWithOptions(socksProxyAddr, networkByte)

	c := collector.New(collector.Config{
		SeedNodes: parseSeedNodes(os.Getenv("NETMAP_SEED_NODES")),
		// 500ms between per-node dials within a single Discover/
		// PollConfirmed/PollUnconfirmed pass, so we don't hammer many
		// different nodes in rapid succession even though the overall
		// pass frequency is polite.
		// The collector package itself defaults DialJitter to zero (no
		// delay) so its own tests stay fast and deterministic; this
		// production default is set here instead — see
		// collector.Config.DialJitter's doc comment for why.
		DialJitter: 500 * time.Millisecond,
	})
	c.Storage = store
	c.GRPCClient = grpcClient
	c.P2PClient = p2pClient
	// OnPollResult feeds this process' own scheduled poll loops (PollConfirmed/
	// PollUnconfirmed/PollNeverContacted) into netmap_<network>_collector_poll_result_total
	// (see metrics.go's netmapMetrics.onPollResult/PollResultFunc) -- deliberately NOT wired
	// into api.NewRouter's own grpcClient/p2pClient below, since its ad hoc admin-triggered
	// probes (poll-now, submission approval) are a different, unrelated activity that must
	// not skew this metric.
	c.OnPollResult = metrics.onPollResult

	go func() {
		if err := c.Run(ctx); err != nil {
			log.Printf("collector: run error: %v", err)
		}
	}()

	// NETMAP_ADMIN_USER / NETMAP_ADMIN_PASSWORD gate every /admin/*
	// route (the submission review queue and the poll-now admin tool)
	// behind HTTP Basic Auth — see internal/adminauth.Wrap. If either
	// is empty/unset, the admin area fails closed (503 on every
	// request) rather than falling back to a default/blank credential
	// pair; see adminauth.Credentials.Configured's doc comment.
	adminCreds := adminauth.Credentials{
		Username: os.Getenv("NETMAP_ADMIN_USER"),
		Password: os.Getenv("NETMAP_ADMIN_PASSWORD"),
	}
	if !adminCreds.Configured() {
		log.Printf("NETMAP_ADMIN_USER/NETMAP_ADMIN_PASSWORD not both set — /admin routes are disabled (503)")
	}

	webHandler, err := web.NewHandler(store, adminCreds)
	if err != nil {
		log.Fatalf("failed to build web handler: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", webHandler)
	mux.Handle("/api/", http.StripPrefix("/api", api.NewRouter(store, grpcClient, p2pClient, adminCreds)))

	// instrumentHTTP wraps the whole dashboard+API mux above with httpRequestsTotal/
	// httpRequestDuration -- see metrics.go's doc comment. This mux is served on *addr (the
	// existing, Caddy-proxied listener) -- /metrics and /healthz are NEVER mounted here, see
	// the separate -metrics-addr listener below.
	srv := &http.Server{Addr: *addr, Handler: metrics.instrumentHTTP(mux)}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
	}()

	// -metrics-addr is a SEPARATE, opt-in, internal-only listener for /metrics + /healthz --
	// see this binary's own doc comment and metrics.go's doc comment for why this must never
	// share *addr's (Caddy-proxied) listener/mux. Empty (the default) disables it entirely,
	// including the periodic DB-derived-gauge refresh loop below -- a process with metrics
	// disabled issues none of these extra periodic queries at all.
	var metricsWG sync.WaitGroup
	if *metricsAddr != "" {
		metricsListener, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			log.Fatalf("netmap: listening on -metrics-addr %s: %v", *metricsAddr, err)
		}
		metricsServer := newMetricsServer(store, metrics)
		log.Printf("netmap: serving /metrics and /healthz on %s (internal-only -- never expose this address publicly)", metricsListener.Addr())

		metricsWG.Add(1)
		go func() {
			defer metricsWG.Done()
			if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("netmap: metrics server error: %v", err)
			}
		}()

		go func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = metricsServer.Shutdown(shutdownCtx)
		}()

		go metrics.runMetricsRefreshLoop(ctx, store)
	}

	log.Printf("go-tari-netmap listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	metricsWG.Wait()
}

// parseSeedNodes parses a comma-separated list of seed node addresses,
// trimming whitespace and dropping empty entries. An empty/unset raw value
// returns a nil (empty) seed list.
func parseSeedNodes(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seeds := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			seeds = append(seeds, p)
		}
	}
	return seeds
}
