// Package main implements netmap-p2p-seed-push: a one-shot, outbound-only Tari P2P tool that
// dials a list of real Tari peers (typically seed nodes) and performs a Noise_XX handshake +
// identity exchange advertising OUR OWN real, persistent identity (COMMUNICATION_NODE features,
// real advertised addresses) -- see BRIEF8.md for the full motivation: without this, nothing
// ever tells a real Tari peer that our inbound netmap-p2p-responder exists, since Tari's
// base_node gRPC surface has no "push a peer into someone's table" RPC; the only real mechanism
// is exactly what a real Tari node does naturally on startup -- dial its configured peer_seeds
// and let the responder's own comms/dht/src/connectivity code record us via the identity
// exchange it receives.
//
// This binary is deliberately push-only and one-shot: it does not serve get_peers, does not
// retry, does not run as a daemon, and does not persist anything -- it dials each target once,
// logs the outcome, and exits. It is meant to be invoked by hand (or later wrapped in a cron
// job) alongside the real, always-on netmap-p2p-responder this tool exists to advertise.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Snipa22/go-tari-lib/p2p"

	"github.com/Snipa22/go-tari-netmap/internal/p2pidentity"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("netmap-p2p-seed-push: %v", err)
	}
}

func run() error {
	var (
		keyPath       = flag.String("key", "", "REQUIRED: path to the SAME identity key file the responder we're seeding for uses (32 raw bytes, see internal/p2pidentity.LoadOrGenerateKeypair) -- MUST be the real persistent key, never a fresh/throwaway one, or every dial just registers a different, useless identity in each target's peer table")
		network       = flag.String("network", "", "REQUIRED: which Tari network to dial as -- \"mainnet\" or \"testnet\" -- selects the P2P wire network byte written before the Noise handshake (mainnet -> p2p.NetworkByteMainNet, testnet -> p2p.NetworkByteEsmeralda, mirroring cmd/netmap-p2p-responder's/cmd/netmap's own -network convention, though this flag drives the wire byte here rather than a metrics prefix)")
		publicTCPAddr = flag.String("public-tcp-addr", "", "our own publicly-dialable clearnet multiaddr to advertise, e.g. /ip4/203.0.113.7/tcp/18189 -- MUST be the exact same address the responder we're seeding for advertises (see its own startup log line). At least one of -public-tcp-addr/-onion3-addr is REQUIRED.")
		onion3Addr    = flag.String("onion3-addr", "", "our own onion-v3 multiaddr to advertise, e.g. /onion3/<56-char-base32-addr>:18189 -- MUST match the responder's own advertised onion3 address, if it has one. At least one of -public-tcp-addr/-onion3-addr is REQUIRED, see -public-tcp-addr.")
		targetsFlag   = flag.String("targets", "", "REQUIRED: target seed node addresses to dial -- either an inline comma-separated \"host:port\" list, or a path to a file with one (or comma-separated) target(s) per line (# comments allowed) -- see ParseTargets")
		socksProxy    = flag.String("socks-proxy-addr", "", "\"host:port\" of a local SOCKS5 proxy (e.g. a Tor daemon's SocksPort), used ONLY for .onion targets -- required if any -targets entry is a .onion address, no effect on clearnet targets (mirrors go-tari-lib/p2p.ProbeOptions.SocksProxyAddr's exact semantics)")
		concurrency   = flag.Int("concurrency", 8, "max number of targets dialed simultaneously -- kept modest (5-10) so this stays a good network citizen rather than a connection-flood source against real production seed nodes")
		dialTimeout   = flag.Duration("dial-timeout", 15*time.Second, "per-target timeout covering dial + Noise_XX handshake + identity exchange (10-15s is plenty for a healthy peer; a slow/unresponsive one should just be reported as a timeout, not hang the whole run)")
	)
	flag.Parse()

	if *keyPath == "" {
		return fmt.Errorf("-key is required: the responder's own real persistent identity key file, never a fresh/throwaway one")
	}
	networkByte, err := networkByteFor(*network)
	if err != nil {
		return err
	}
	ourAddresses, err := p2pidentity.ParseAdvertisedAddresses(*publicTCPAddr, *onion3Addr)
	if err != nil {
		return err
	}
	targets, err := ParseTargets(*targetsFlag)
	if err != nil {
		return err
	}

	staticKeypair, err := p2pidentity.LoadOrGenerateKeypair(*keyPath)
	if err != nil {
		return fmt.Errorf("loading identity key from -key %s: %w", *keyPath, err)
	}
	log.Printf("main: our static public key: %x", staticKeypair.Public)
	log.Printf("main: advertising %d address(es): public-tcp=%q onion3=%q", len(ourAddresses), *publicTCPAddr, *onion3Addr)
	log.Printf("main: dialing %d target(s) with concurrency=%d, dial-timeout=%s, network=%s (wire byte 0x%02x)",
		len(targets), *concurrency, *dialTimeout, *network, networkByte)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := PushConfig{
		StaticKeypair:  staticKeypair,
		NetworkByte:    networkByte,
		Features:       p2p.FeaturesCommunicationNode,
		Addresses:      ourAddresses,
		SocksProxyAddr: *socksProxy,
		DialTimeout:    *dialTimeout,
		Concurrency:    *concurrency,
		Logf:           log.Printf,
	}

	results := PushAll(ctx, targets, cfg)

	var succeeded, failed int
	for _, r := range results {
		if r.Err == nil {
			succeeded++
		} else {
			failed++
		}
	}
	log.Printf("main: done: %d/%d succeeded, %d failed", succeeded, len(results), failed)
	return nil
}

// networkByteFor maps the -network flag's two valid values to the real Tari P2P wire network
// byte InitiatorHandshake must write first (see go-tari-lib/p2p/handshake.go's NetworkByte*
// constants) -- "mainnet" maps to NetworkByteMainNet, and "testnet" maps to NetworkByteEsmeralda,
// mirroring cmd/netmap/main.go's own NETMAP_NETWORK_BYTE comment identifying Esmeralda as the
// real Tari testnet this repo's "testnet" deployments (e.g. CT132) actually monitor. Fails fast
// on anything else, matching cmd/netmap-p2p-responder's/cmd/netmap's own -network fail-fast
// convention (see their validMainnetOrTestnet checks) rather than silently defaulting.
func networkByteFor(network string) (byte, error) {
	switch network {
	case "mainnet":
		return p2p.NetworkByteMainNet, nil
	case "testnet":
		return p2p.NetworkByteEsmeralda, nil
	case "":
		return 0, fmt.Errorf("-network is required (must be %q or %q)", "mainnet", "testnet")
	default:
		return 0, fmt.Errorf("-network %q is invalid: must be %q or %q", network, "mainnet", "testnet")
	}
}
