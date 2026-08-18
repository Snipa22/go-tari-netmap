package collector

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Snipa22/go-tari-lib/p2p"
	pb "github.com/Snipa22/go-tari-lib/p2p/proto"
	rpcpkg "github.com/Snipa22/go-tari-lib/p2p/rpc"
)

// p2pDialTimeout bounds how long p2pNodeClient waits for a single probe
// (handshake + identity exchange + the actual RPC call) to complete before
// giving up on an addr. This is deliberately an alias to grpc_client.go's
// dialTimeout, not an independent value: the dial timeout is meant to be
// shared across both transports (real-world network/Tor-circuit latency
// applies equally to either), so bumping one without the other would just
// reintroduce the same too-short-for-onion-peers problem on this path.
// See dialTimeout's doc comment in grpc_client.go for the full reasoning
// (including the live-tested 60s-manual-vs-5s-production finding) behind
// its current 180s value.
const p2pDialTimeout = dialTimeout

// p2pProbeFuncs is the seam between p2pNodeClient and go-tari-lib/p2p's
// free functions (ProbeChainMetadata/ProbeGetPeers/Probe). go-tari-lib/p2p
// exposes no interface of its own to mock, so this package-private
// interface exists purely so tests can inject a fake implementation
// instead of making real P2P network calls — mirroring how grpcNodeClient
// injects dial/dialOpts for the same purpose.
type p2pProbeFuncs interface {
	probeChainMetadata(ctx context.Context, addr string, opts p2p.ProbeOptions) (*p2p.ChainMetadataInfo, error)
	probeGetPeers(ctx context.Context, addr string, req rpcpkg.GetPeersRequest, opts p2p.ProbeOptions) ([]*pb.PeerInfo, error)

	// probeIdentity performs a Noise_XX handshake against addr and
	// returns the peer's confirmed pubkey (PeerInfo.RemoteStaticPubKey),
	// recovered directly from the handshake rather than claimed by a
	// third party. See GetInfo's doc comment for why this is a second,
	// independent probe/dial from probeChainMetadata rather than
	// something bolted onto ChainMetadataInfo. It also takes
	// ProbeOptions for SOCKS support, same reasoning as the other two
	// probes.
	probeIdentity(ctx context.Context, addr string, opts p2p.ProbeOptions) (*p2p.PeerInfo, error)
}

// realP2PProbeFuncs is the default p2pProbeFuncs implementation, backed by
// the real go-tari-lib/p2p package.
type realP2PProbeFuncs struct{}

func (realP2PProbeFuncs) probeChainMetadata(ctx context.Context, addr string, opts p2p.ProbeOptions) (*p2p.ChainMetadataInfo, error) {
	return p2p.ProbeChainMetadataWithOptions(ctx, addr, opts)
}

func (realP2PProbeFuncs) probeGetPeers(ctx context.Context, addr string, req rpcpkg.GetPeersRequest, opts p2p.ProbeOptions) ([]*pb.PeerInfo, error) {
	return p2p.ProbeGetPeersWithOptions(ctx, addr, req, opts)
}

func (realP2PProbeFuncs) probeIdentity(ctx context.Context, addr string, opts p2p.ProbeOptions) (*p2p.PeerInfo, error) {
	return p2p.ProbeWithOptions(ctx, addr, opts)
}

// p2pNodeClient is the real go-tari-lib/p2p-backed NodeClient
// implementation. It talks to a Tari node's comms/RPC-over-P2P interface
// directly (Noise_XX handshake + identity exchange + a single RPC call per
// probe), rather than via gRPC — see grpcNodeClient in grpc_client.go for
// the gRPC-based counterpart.
type p2pNodeClient struct {
	// probes is normally realP2PProbeFuncs{}; overridable in tests to
	// inject a fake implementation so tests exercise GetInfo/GetPeers'
	// mapping logic without any real P2P network calls.
	probes p2pProbeFuncs

	// socksProxyAddr, if non-empty, is the "host:port" address of a
	// SOCKS5 proxy (e.g. a local Tor daemon's SocksPort) passed through
	// to probeChainMetadata/probeGetPeers/probeIdentity via
	// p2p.ProbeOptions, letting this client reach `.onion` Tari peers.
	// The zero value (empty string) preserves the exact pre-existing
	// zero-config behavior — see NewP2PClient/NewP2PClientWithSocksProxy.
	// probeIdentity is given this same treatment as
	// probeChainMetadata/probeGetPeers, for the same reason (reaching
	// onion peers) — see GetInfo's doc comment.
	socksProxyAddr string
}

// NewP2PClient returns a NodeClient backed by real go-tari-lib/p2p calls
// against Tari nodes over the comms/RPC-over-P2P transport. It takes no
// required args: each method probes the addr passed to it, per-call. The
// returned client dials directly (no SOCKS proxy) — see
// NewP2PClientWithSocksProxy for `.onion` peer support.
func NewP2PClient() NodeClient {
	return &p2pNodeClient{probes: realP2PProbeFuncs{}}
}

// NewP2PClientWithSocksProxy returns a NodeClient identical to
// NewP2PClient's, except probeChainMetadata/probeGetPeers/probeIdentity
// calls are given proxyAddr (a SOCKS5 proxy "host:port", e.g. a local Tor
// daemon's SocksPort) via p2p.ProbeOptions.SocksProxyAddr, letting this
// client reach `.onion` Tari peers. proxyAddr has no effect on
// non-`.onion` addresses — see p2p.ProbeOptions's doc comment in
// go-tari-lib.
func NewP2PClientWithSocksProxy(proxyAddr string) NodeClient {
	return &p2pNodeClient{probes: realP2PProbeFuncs{}, socksProxyAddr: proxyAddr}
}

// GetInfo implements NodeClient.
//
// Version comes from PeerInfo.UserAgent, returned by the same
// probeIdentity call already made below to recover PublicKey — so it
// costs nothing extra: ChainMetadataInfo (the result of
// go-tari-lib/p2p.ProbeChainMetadata) has no version/user-agent field, but
// probeIdentity's PeerInfo does, and that second dial is already being
// paid for PublicKey's sake (see below), so Version rides along for free.
// Like PublicKey, Version simply stays nil if probeIdentity fails.
//
// PeerIdentityUpdatedAt rides along the exact same way, from
// PeerInfo.IdentitySignature.UpdatedAt (converted from Unix seconds to a
// *time.Time) — the peer's own claim of when it last (re-)signed its P2P
// identity. IdentitySignature is nil if the peer sent no signature (per
// its own doc comment in go-tari-lib), in which case, like Version and
// PublicKey, PeerIdentityUpdatedAt is simply left nil.
//
// PublicKey IS worth that second dial: ChainMetadataInfo
// has no pubkey field at all (out of scope to add — that's inside
// go-tari-lib), so the only way to get addr's confirmed pubkey over this
// transport is a second, independent p2p.ProbeWithOptions call (a full
// Noise_XX handshake in its own right, recovering
// PeerInfo.RemoteStaticPubKey).
// This means GetInfo now makes two separate P2P dials/handshakes to the
// same addr on every call — an accepted, real limitation given
// go-tari-lib is out of scope to modify further to merge them into one
// round trip. The two probes are independent: if probeChainMetadata
// succeeds but probeIdentity fails (or vice versa), that does not fail
// the whole call — Reachable reflects whether probeChainMetadata
// succeeded (the existing behavior/contract other code depends on);
// PublicKey simply stays nil if probeIdentity failed, logged but
// non-fatal.
func (c *p2pNodeClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, p2pDialTimeout)
	defer cancel()

	meta, err := c.probes.probeChainMetadata(ctx, addr, p2p.ProbeOptions{SocksProxyAddr: c.socksProxyAddr})
	if err != nil {
		return NodeInfo{}, fmt.Errorf("p2p ProbeChainMetadata %s: %w", addr, err)
	}

	info := NodeInfo{
		Reachable: true,
	}

	if peerInfo, err := c.probes.probeIdentity(ctx, addr, p2p.ProbeOptions{SocksProxyAddr: c.socksProxyAddr}); err != nil {
		log.Printf("p2p GetInfo %s: probeIdentity failed (non-fatal, PublicKey left nil): %v", addr, err)
	} else {
		info.PublicKey = peerInfo.RemoteStaticPubKey
		if peerInfo.UserAgent != "" {
			v := peerInfo.UserAgent
			info.Version = &v
		}
		if peerInfo.IdentitySignature != nil && peerInfo.IdentitySignature.UpdatedAt != 0 {
			t := time.Unix(peerInfo.IdentitySignature.UpdatedAt, 0)
			info.PeerIdentityUpdatedAt = &t
		}
	}

	height := int64(meta.BestBlockHeight)
	info.Height = &height
	// Same reasoning as grpc_client.go's GetInfo: Tari's own
	// best-block-height IS its view of the chain tip once synced, and
	// get_chain_metadata doesn't expose a separate "network tip vs our
	// tip" split, so ChainTipHeight mirrors Height here rather than
	// being left nil.
	chainTip := height
	info.ChainTipHeight = &chainTip

	latencyMS := int(meta.Latency.Milliseconds())
	info.LatencyMS = &latencyMS

	// RxtHashrate/C29Hashrate/Sha3xHashrate are not derivable from
	// get_chain_metadata — left nil, same as the GRPC path.

	return info, nil
}

// GetPeers implements NodeClient.
func (c *p2pNodeClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, p2pDialTimeout)
	defer cancel()

	peers, err := c.probes.probeGetPeers(ctx, addr, p2p.DefaultGetPeersRequest(), p2p.ProbeOptions{SocksProxyAddr: c.socksProxyAddr})
	if err != nil {
		return nil, fmt.Errorf("p2p ProbeGetPeers %s: %w", addr, err)
	}

	seen := make(map[string]bool)
	var discovered []DiscoveredPeer
	for _, peer := range peers {
		var pubKey []byte
		if len(peer.PublicKey) > 0 {
			pubKey = peer.PublicKey
		}
		for _, claim := range peer.Claims {
			for _, a := range claim.Addresses {
				hostPort, ok := parsePeerAddress(a)
				if !ok {
					// Best-effort: skip unparseable addresses rather than
					// emitting garbage, same as grpc_client.go's GetPeers.
					continue
				}
				if seen[hostPort] {
					// A peer can advertise the same address across
					// multiple claims; dedupe before returning.
					continue
				}
				seen[hostPort] = true
				discovered = append(discovered, DiscoveredPeer{Address: hostPort, PublicKey: pubKey})
			}
		}
	}

	return discovered, nil
}
