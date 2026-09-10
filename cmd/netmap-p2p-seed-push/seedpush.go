package main

import (
	"context"
	"fmt"
	"time"

	"github.com/flynn/noise"
	"golang.org/x/sync/errgroup"

	"github.com/Snipa22/go-tari-lib/p2p"
)

// PushConfig configures PushOne/PushAll's outbound identity-exchange behavior -- everything a
// single dial needs that does NOT vary per-target (the target itself is passed separately).
type PushConfig struct {
	// StaticKeypair is OUR OWN long-term Ristretto255 identity keypair -- the SAME persistent
	// key the responder we're seeding for uses (see internal/p2pidentity.LoadOrGenerateKeypair
	// and this package's doc comment for why that's non-negotiable).
	StaticKeypair noise.DHKey

	// NetworkByte is the P2P wire network byte written before the Noise_XX handshake (see
	// go-tari-lib/p2p.InitiatorHandshake and this repo's networkByteFor).
	NetworkByte byte

	// Features/Addresses are advertised in our outgoing PeerIdentityMsg via
	// Session.ExchangeIdentityWithOptions -- Features should be p2p.FeaturesCommunicationNode
	// and Addresses should be the responder's own real advertised addresses (see
	// internal/p2pidentity.ParseAdvertisedAddresses), never a placeholder -- see BRIEF8.md.
	Features  uint32
	Addresses [][]byte

	// SocksProxyAddr, if non-empty, is used ONLY for `.onion` targets (see dialTarget).
	SocksProxyAddr string

	// DialTimeout bounds each individual target's dial + handshake + identity exchange, applied
	// via context.WithTimeout per target -- one slow/unresponsive target must never delay or
	// starve any other target's dial.
	DialTimeout time.Duration

	// Concurrency is the max number of targets dialed simultaneously (see PushAll).
	Concurrency int

	// Logf, if non-nil, receives printf-style per-target diagnostic tracing (dial start,
	// success/failure with pubkey/latency/error) -- this is the primary output an operator
	// running this tool by hand actually reads, not just the final summary count. A nil Logf
	// means no logging (used by tests that only care about the returned []PushResult).
	Logf func(format string, args ...interface{})
}

func (c PushConfig) logf(format string, args ...interface{}) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

// PushResult is the outcome of a single PushOne call.
type PushResult struct {
	Target string

	// PeerPubKey is the target's recovered 32-byte Ristretto255 static public key, set only on
	// success.
	PeerPubKey []byte

	// Latency is the wall-clock time PushOne took for dial+handshake+identity exchange, set
	// regardless of success/failure (useful for diagnosing "it eventually timed out" vs "it
	// failed instantly").
	Latency time.Duration

	// Err is nil on success, and describes exactly what failed (dial vs handshake vs identity
	// exchange) otherwise -- see PushOne.
	Err error
}

// PushOne dials target, performs the network-wire-byte + Noise_XX handshake (as the initiator)
// and identity exchange advertising cfg.Features/cfg.Addresses under cfg.StaticKeypair, then
// closes the connection -- this tool is push-only and does not need to serve get_peers or handle
// any further protocol once identity exchange completes (see this package's doc comment).
//
// The entire operation (dial + handshake + identity exchange) is bounded by cfg.DialTimeout via
// a context derived from ctx; a target that doesn't respond in time is reported as a timeout
// error, never left to hang the caller (see PushAll's bounded-concurrency loop).
func PushOne(ctx context.Context, target string, cfg PushConfig) PushResult {
	start := time.Now()
	cfg.logf("seed-push: %s: dialing", target)

	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	conn, err := dialTarget(dialCtx, target, cfg.SocksProxyAddr)
	if err != nil {
		result := PushResult{Target: target, Latency: time.Since(start), Err: fmt.Errorf("dialing: %w", err)}
		cfg.logf("seed-push: %s: FAILED dialing: %v (latency=%s)", target, err, result.Latency)
		return result
	}
	defer conn.Close()

	if deadline, ok := dialCtx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	session, err := p2p.InitiatorHandshake(dialCtx, conn, cfg.StaticKeypair, cfg.NetworkByte)
	if err != nil {
		result := PushResult{Target: target, Latency: time.Since(start), Err: fmt.Errorf("Noise_XX handshake: %w", err)}
		cfg.logf("seed-push: %s: FAILED handshake: %v (latency=%s)", target, err, result.Latency)
		return result
	}
	defer session.Close()

	info, err := session.ExchangeIdentityWithOptions(dialCtx, p2p.IdentityOptions{
		Features:  cfg.Features,
		Addresses: cfg.Addresses,
	})
	if err != nil {
		result := PushResult{Target: target, Latency: time.Since(start), Err: fmt.Errorf("identity exchange: %w", err)}
		cfg.logf("seed-push: %s: FAILED identity exchange: %v (latency=%s)", target, err, result.Latency)
		return result
	}

	result := PushResult{
		Target:     target,
		PeerPubKey: append([]byte(nil), session.PeerStaticKey...),
		Latency:    time.Since(start),
	}
	cfg.logf("seed-push: %s: OK pubkey=%x peer_features=%d peer_user_agent=%q latency=%s",
		target, session.PeerStaticKey, info.Features, info.UserAgent, result.Latency)
	return result
}

// PushAll runs PushOne against every target in targets, bounded to at most cfg.Concurrency
// simultaneous in-flight dials via an errgroup.Group with SetLimit -- mirroring this repo's own
// existing bounded-concurrency convention (internal/collector/collector.go's poll()) rather than
// dialing all targets at once (this tool must be a good network citizen against real production
// seed nodes, per BRIEF8.md's explicit constraint).
//
// Results are returned in the exact same order as targets, regardless of completion order (each
// worker writes to its own pre-allocated slice index, so no lock is needed) -- this makes the
// final report easy to read/diff against the input target list.
func PushAll(ctx context.Context, targets []string, cfg PushConfig) []PushResult {
	results := make([]PushResult, len(targets))

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	var g errgroup.Group
	g.SetLimit(concurrency)

	for i, target := range targets {
		i, target := i, target
		g.Go(func() error {
			results[i] = PushOne(ctx, target, cfg)
			return nil
		})
	}
	_ = g.Wait()
	return results
}
