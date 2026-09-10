// Command netmap-p2p-responder is a real, Postgres-backed inbound Tari P2P responder: it
// accepts real Tari peer connections (Noise_XX handshake + identity exchange, advertising
// COMMUNICATION_NODE features + real addresses, see go-tari-lib/p2p's Serve/ResponderConfig)
// and serves get_peers over `t/dht/1` with a REAL list of confirmed-good peers pulled from
// go-tari-netmap's own storage.Store — never a static/in-memory list.
//
// This supersedes go-tari-lib's throwaway cmd/p2p-responder-spike/main.go (see BRIEF3.md):
// that binary stored everything in a plain in-memory map, so nothing survived a restart and
// nothing was queryable. This binary moves that entrypoint logic here, into go-tari-netmap's
// own cmd/, since this is where the real storage.Store/Postgres dependency naturally lives —
// go-tari-lib is a pure protocol library and has (deliberately) zero storage dependency of its
// own; see BRIEF3.md's "Option A (probably cleaner)" for the full reasoning this binary follows.
//
// # Advertised addresses
//
// A COMMUNICATION_NODE peer that advertises ZERO addresses is exactly the case a real Tari
// node's `comms/dht/src/peer_validator.rs` PeerHasNoAddresses/PeerHasNoUsableAddresses checks
// reject — so, mirroring the spike binary this replaces, this binary REQUIRES at least one
// advertised address via two optional CLI flags (at least one of which must be set, or this
// binary fails fast at startup rather than silently running unreachable):
//
//	-public-tcp-addr /ip4/<public-ip>/tcp/<port>   e.g. -public-tcp-addr /ip4/203.0.113.7/tcp/18189
//	-onion3-addr     /onion3/<addr>:<port>          e.g. -onion3-addr /onion3/abc...xyz:18189
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/flynn/noise"

	"github.com/Snipa22/go-tari-lib/p2p"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("netmap-p2p-responder: %v", err)
	}
}

func run() error {
	var (
		addr          = flag.String("addr", ":18189", "TCP address to listen on (matches real Tari base node p2p port conventions by default)")
		keyPath       = flag.String("key", "", "path to a file holding our long-term Ristretto255 private key (32 raw bytes); if empty or the file doesn't exist, a fresh key is generated and, if -key was given, saved there for reuse across restarts")
		publicTCPAddr = flag.String("public-tcp-addr", "", "our own publicly-dialable clearnet multiaddr to advertise, e.g. /ip4/203.0.113.7/tcp/18189 (optional, but at least one of -public-tcp-addr/-onion3-addr is REQUIRED -- a COMMUNICATION_NODE peer with zero advertised addresses is rejected by real Tari nodes' peer validation)")
		onion3Addr    = flag.String("onion3-addr", "", "our own onion-v3 multiaddr to advertise, e.g. /onion3/<56-char-base32-addr>:18189 (optional, but at least one of -public-tcp-addr/-onion3-addr is REQUIRED, see -public-tcp-addr)")
		metricsAddr   = flag.String("metrics-addr", "", "address to serve Prometheus /metrics + /healthz on (default empty = disabled, opt-in like -public-tcp-addr/-onion3-addr). "+
			"CRITICAL: bind to an INTERNAL-ONLY address, e.g. 192.168.40.x:PORT or 127.0.0.1:PORT -- NEVER the public IP this binary also advertises via -public-tcp-addr. "+
			"If you set this at all, prefer a loopback-only address such as 127.0.0.1:9471; do NOT use a bare :PORT form (binds ALL interfaces, including the public one).")
	)
	flag.Parse()

	ourAddresses, err := parseAdvertisedAddresses(*publicTCPAddr, *onion3Addr)
	if err != nil {
		return err
	}

	staticKeypair, err := loadOrGenerateKeypair(*keyPath)
	if err != nil {
		return fmt.Errorf("setting up static keypair: %w", err)
	}
	log.Printf("main: our static public key: %x", staticKeypair.Public)
	log.Printf("main: advertising %d address(es): public-tcp=%q onion3=%q", len(ourAddresses), *publicTCPAddr, *onion3Addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Wire the DSN exactly the way cmd/netmap/main.go does: storage.DSNFromEnv()
	// (NETMAP_DATABASE_URL), never a hardcoded connection string or a different env var.
	dsn := storage.DSNFromEnv()
	store, err := storage.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connecting to storage: %w", err)
	}
	defer store.Close()

	// Safe to run against a fresh or partially-migrated DB, exactly like cmd/netmap/main.go.
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", *addr, err)
	}
	defer listener.Close()
	log.Printf("main: listening on %s", listener.Addr())

	go func() {
		<-ctx.Done()
		log.Printf("main: shutting down")
		listener.Close()
	}()

	var metricsWG sync.WaitGroup
	if *metricsAddr != "" {
		// CRITICAL: this listener MUST be internal-only (see -metrics-addr's own help text
		// above) -- it is a completely separate net.Listener/http.Server from the P2P listener
		// above, bound to whatever address the operator configured. This binary does not
		// enforce or validate that the given address isn't the public one; that's on the
		// operator, per the flag's help text -- but never default *metricsAddr to a bare
		// ":PORT" form yourself, and never wire it to *publicTCPAddr/listener.Addr() by
		// accident. If you're reading this because you're about to change this call: binding
		// 0.0.0.0 (or a public IP) here exposes internal metrics -- including per-substream
		// protocol/DB-write detail -- to the same public network this responder's P2P port is
		// deliberately exposed to; that is NOT the intent of this flag.
		metricsListener, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("listening on -metrics-addr %s: %w", *metricsAddr, err)
		}
		metricsServer := newMetricsServer(store)
		log.Printf("main: serving /metrics and /healthz on %s (internal-only -- never expose this address publicly)", metricsListener.Addr())

		metricsWG.Add(1)
		go func() {
			defer metricsWG.Done()
			if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("main: metrics server error: %v", err)
			}
		}()

		go func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = metricsServer.Shutdown(shutdownCtx)
		}()
	}

	responder := &dbBackedResponder{store: store, logf: log.Printf}

	cfg := p2p.ResponderConfig{
		StaticKeypair:               staticKeypair,
		OurFeatures:                 p2p.FeaturesCommunicationNode,
		OurAddresses:                ourAddresses,
		PeerListProvider:            responder.peerListProvider,
		OnPeerIdentity:              responder.onPeerIdentity,
		Logf:                        log.Printf,
		OnConnectionAccepted:        onConnectionAccepted,
		OnHandshakeResult:           onHandshakeResult,
		OnIdentityExchangeResult:    onIdentityExchangeResult,
		OnGetPeersServed:            onGetPeersServed,
		OnSubstreamProtocolDeclined: onSubstreamProtocolDeclined,
	}

	err = p2p.Serve(ctx, listener, cfg)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("serving: %w", err)
	}
	log.Printf("main: responder loop exited cleanly")
	metricsWG.Wait()
	return nil
}

// parseAdvertisedAddresses validates and encodes this binary's -public-tcp-addr/-onion3-addr
// flag values into the raw binary rust-multiaddr wire encoding p2p.ResponderConfig.OurAddresses
// expects (see go-tari-lib's p2p/multiaddr.go EncodeMultiaddrString doc comment for exactly why
// that, and not a UTF-8 string, is required). At least one of the two flags MUST be non-empty --
// fails fast with a clear error otherwise, rather than silently starting an unreachable
// COMMUNICATION_NODE peer (a real Tari node's comms/dht/src/peer_validator.rs
// PeerHasNoAddresses/PeerHasNoUsableAddresses checks reject a peer with zero advertised
// addresses).
func parseAdvertisedAddresses(publicTCPAddr, onion3Addr string) ([][]byte, error) {
	if publicTCPAddr == "" && onion3Addr == "" {
		return nil, fmt.Errorf("at least one of -public-tcp-addr or -onion3-addr must be set: a COMMUNICATION_NODE peer with no advertised addresses is rejected by real Tari nodes' peer validation and would start unreachable")
	}

	var out [][]byte
	if publicTCPAddr != "" {
		encoded, err := p2p.EncodeMultiaddrString(publicTCPAddr)
		if err != nil {
			return nil, fmt.Errorf("-public-tcp-addr %q: %w", publicTCPAddr, err)
		}
		out = append(out, encoded)
	}
	if onion3Addr != "" {
		encoded, err := p2p.EncodeMultiaddrString(onion3Addr)
		if err != nil {
			return nil, fmt.Errorf("-onion3-addr %q: %w", onion3Addr, err)
		}
		out = append(out, encoded)
	}
	return out, nil
}

// keyFileSize is the size, in bytes, of the file loadOrGenerateKeypair reads/writes: the 32-byte
// private scalar followed by the 32-byte public point, i.e. exactly noise.DHKey's two fields
// concatenated. Storing both (rather than just the private key and re-deriving the public key on
// load) avoids needing to export Ristretto255 scalar-base-multiplication from package p2p purely
// for this binary's key-persistence convenience.
const keyFileSize = 64

// loadOrGenerateKeypair loads a keypair from path (see keyFileSize) if path is non-empty and the
// file exists; otherwise it generates a fresh keypair and, if path is non-empty, saves it there
// (mode 0600) for reuse across restarts. An empty path always generates an ephemeral, unsaved
// keypair.
func loadOrGenerateKeypair(path string) (noise.DHKey, error) {
	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			if len(raw) != keyFileSize {
				return noise.DHKey{}, fmt.Errorf("key file %s has %d bytes, want %d", path, len(raw), keyFileSize)
			}
			return noise.DHKey{
				Private: append([]byte(nil), raw[:32]...),
				Public:  append([]byte(nil), raw[32:]...),
			}, nil
		} else if !os.IsNotExist(err) {
			return noise.DHKey{}, fmt.Errorf("reading key file %s: %w", path, err)
		}
	}

	keypair, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		return noise.DHKey{}, fmt.Errorf("generating a fresh keypair: %w", err)
	}

	if path != "" {
		raw := append(append([]byte(nil), keypair.Private...), keypair.Public...)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			return noise.DHKey{}, fmt.Errorf("saving fresh keypair to %s: %w", path, err)
		}
	}
	return keypair, nil
}
