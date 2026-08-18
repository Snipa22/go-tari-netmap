package collector

import (
	"context"
	"fmt"
	"log"

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
	// something bolted onto ChainMetadataInfo.
	probeIdentity(ctx context.Context, addr string) (*p2p.PeerInfo, error)
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

func (realP2PProbeFuncs) probeIdentity(ctx context.Context, addr string) (*p2p.PeerInfo, error) {
	return p2p.Probe(ctx, addr)
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
	// to probeChainMetadata/probeGetPeers via p2p.ProbeOptions, letting
	// this client reach `.onion` Tari peers. The zero value (empty
	// string) preserves the exact pre-existing zero-config behavior —
	// see NewP2PClient/NewP2PClientWithSocksProxy. probeIdentity is
	// deliberately NOT given this treatment (out of scope for this
	// field, see GetInfo's doc comment).
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
// NewP2PClient's, except probeChainMetadata/probeGetPeers calls are given
// proxyAddr (a SOCKS5 proxy "host:port", e.g. a local Tor daemon's
// SocksPort) via p2p.ProbeOptions.SocksProxyAddr, letting this client
// reach `.onion` Tari peers. proxyAddr has no effect on non-`.onion`
// addresses — see p2p.ProbeOptions's doc comment in go-tari-lib.
func NewP2PClientWithSocksProxy(proxyAddr string) NodeClient {
	return &p2pNodeClient{probes: realP2PProbeFuncs{}, socksProxyAddr: proxyAddr}
}

// GetInfo implements NodeClient.
//
// Version is intentionally left nil here: ChainMetadataInfo (the result of
// go-tari-lib/p2p.ProbeChainMetadata) has no version/user-agent field, and
// there is no cheap way to get one from the same call. p2p.Probe's
// PeerInfo.UserAgent is a separate call that requires a second full
// handshake/dial — not worth the extra round trip just for Version, given
// the GRPC path already covers Version on nodes that expose GRPC.
//
// PublicKey, unlike Version, IS worth that second dial: ChainMetadataInfo
// has no pubkey field at all (out of scope to add — that's inside
// go-tari-lib), so the only way to get addr's confirmed pubkey over this
// transport is a second, independent p2p.Probe call (a full Noise_XX
// handshake in its own right, recovering PeerInfo.RemoteStaticPubKey).
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

	if peerInfo, err := c.probes.probeIdentity(ctx, addr); err != nil {
		log.Printf("p2p GetInfo %s: probeIdentity failed (non-fatal, PublicKey left nil): %v", addr, err)
	} else {
		info.PublicKey = peerInfo.RemoteStaticPubKey
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
