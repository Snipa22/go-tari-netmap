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
	//
	// NETMAP_OWNED_GRPC_ADDRESSES scopes gRPC probing to an explicit allowlist of
	// "P2P address -> real gRPC address" pairs for nodes we own -- comma-separated
	// "p2pAddress=grpcAddress" entries, e.g.
	// "23.226.69.178:18189=23.226.69.178:18102,10.0.0.5:18189=10.0.0.5:18102". This exists
	// because a peer's real gRPC listen port is NEVER discoverable via Tari P2P peer
	// discovery, for ANY peer, arbitrary or our own -- tari_protos/network.proto's
	// Peer/Address messages and go-tari-lib's p2p.PeerInfo both carry no gRPC-port field
	// anywhere, only the P2P/comms wire address. See docs/grpc-port-scope.md for the full
	// finding and rationale. The only way this collector can ever know a node's real gRPC
	// address is out-of-band, operator-supplied config -- i.e. gRPC probing is only
	// meaningful for our own explicitly-configured/owned nodes, never for addresses
	// learned by walking the peer graph. Empty/unset (the default) disables this scoping
	// entirely: grpcClient dials whatever addr it's given, as-is, matching the
	// pre-existing (buggy, for non-owned nodes) zero-config behavior -- see
	// collector.NewGRPCClientWithAddressMap's doc comment for the nil-vs-populated-map
	// distinction.
	ownedGRPCAddresses := parseOwnedGRPCAddresses(os.Getenv("NETMAP_OWNED_GRPC_ADDRESSES"))
	grpcClient := collector.NewGRPCClientWithAddressMap(ownedGRPCAddresses)

	// Real go-tari-lib/p2p-backed client: talks to Tari nodes over the
	// direct comms/RPC-over-P2P transport, independent of gRPC — see
	// p2pNodeClient's doc comment in internal/collector/p2p_client.go.
	//
	// NETMAP_SOCKS_PROXY_ADDRS (plural) is a comma-separated list of Tor SOCKS5 proxy
	// "host:port" addresses (e.g. N independent local Tor daemon instances' SocksPorts,
	// typically 127.0.0.1:9100..127.0.0.1:9123 for N=24 in the real mainnet deploy -- this
	// binary does NOT assume any particular count, it just splits on comma, trims, and
	// drops empties, exactly like parseSeedNodes), used to reach `.onion` Tari peers over
	// this transport. Each dial's proxy is chosen deterministically per-address (see
	// collector.P2PShardIndex), letting collector.go's poll() bound P2P dial concurrency
	// PER REAL TOR INSTANCE instead of across all of them combined -- see
	// collector.maxP2PWorkersPerShard's doc comment for why: a single Tor instance
	// saturates catastrophically at high concurrent hidden-service circuit-build counts.
	//
	// NETMAP_SOCKS_PROXY_ADDR (singular, legacy) is kept working as a fallback for the
	// existing testnet deployment (netmap-testnet.service), which is explicitly OUT OF
	// SCOPE for sharding and must keep using its single existing Tor instance/env var
	// unchanged: if NETMAP_SOCKS_PROXY_ADDRS is unset/empty, this falls back to reading
	// NETMAP_SOCKS_PROXY_ADDR as a single-element list, preserving exact pre-existing
	// testnet behavior with zero config changes required on that host. Precedence is
	// plural-preferred, singular-fallback -- if BOTH happen to be set, plural wins and
	// singular is ignored entirely.
	//
	// Empty/unset (both variables): disables SOCKS entirely (dial directly), matching the
	// pre-existing zero-config behavior.
	socksProxyAddrs := parseSocksProxyAddrs(os.Getenv("NETMAP_SOCKS_PROXY_ADDRS"), os.Getenv("NETMAP_SOCKS_PROXY_ADDR"))

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
	p2pClient := collector.NewP2PClientWithShardedProxies(socksProxyAddrs, networkByte)

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

// parseOwnedGRPCAddresses parses NETMAP_OWNED_GRPC_ADDRESSES's raw value: a comma-separated
// list of "p2pAddress=grpcAddress" pairs (see grpcClient's construction above for the exact
// shape/example and the "why" -- gRPC ports are never P2P-discoverable, see
// docs/grpc-port-scope.md), mirroring parseSeedNodes' style -- trim whitespace, skip empty
// entries. An empty/unset raw value returns a nil map, which is the deliberate "feature not
// configured at all" sentinel collector.NewGRPCClientWithAddressMap distinguishes from a
// populated-but-missing-entry map -- see that constructor's doc comment. Each entry is split on
// the FIRST "=" via strings.Cut, since neither a P2P nor a gRPC "host:port" address can itself
// contain "="; a malformed entry (no "=", or an empty p2pAddress/grpcAddress after trimming) is
// logged and skipped rather than failing the whole binary at startup -- a typo in one pair
// should not take down gRPC probing for every other correctly-configured owned node.
func parseOwnedGRPCAddresses(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	out := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		p2pAddr, grpcAddr, ok := strings.Cut(pair, "=")
		p2pAddr = strings.TrimSpace(p2pAddr)
		grpcAddr = strings.TrimSpace(grpcAddr)
		if !ok || p2pAddr == "" || grpcAddr == "" {
			log.Printf("netmap: skipping malformed NETMAP_OWNED_GRPC_ADDRESSES entry %q (want \"p2pAddress=grpcAddress\")", pair)
			continue
		}
		out[p2pAddr] = grpcAddr
	}
	return out
}

// parseSocksProxyAddrs implements NETMAP_SOCKS_PROXY_ADDRS' plural-preferred/
// NETMAP_SOCKS_PROXY_ADDR-singular-fallback precedence described at this file's socksProxyAddrs
// construction site: if pluralRaw is non-empty, it is parsed exactly like parseSeedNodes
// (comma-separated, trimmed, empties dropped) and singularRaw is ignored entirely -- even if
// singularRaw also happens to be set. Otherwise, if singularRaw is non-empty, it is returned as
// a single-element slice, preserving the existing testnet deployment's exact zero-config
// (aside from that one variable) behavior with NO changes required on that host. If both are
// empty/unset, returns nil (no SOCKS proxy at all, dial directly).
func parseSocksProxyAddrs(pluralRaw, singularRaw string) []string {
	if pluralRaw != "" {
		return parseSeedNodes(pluralRaw)
	}
	if singularRaw != "" {
		return []string{singularRaw}
	}
	return nil
}
