package collector

import (
	"context"
	"fmt"

	"github.com/Snipa22/go-tari-lib/p2p"
	pb "github.com/Snipa22/go-tari-lib/p2p/proto"
	rpcpkg "github.com/Snipa22/go-tari-lib/p2p/rpc"
)

// p2pDialTimeout bounds how long p2pNodeClient waits for a single probe
// (handshake + identity exchange + the actual RPC call) to complete before
// giving up on an addr. Reuses the same value as grpc_client.go's
// dialTimeout for the same reason given there: unreachable/stale peers are
// the normal case for this collector, so a hung probe to one dead peer
// must not stall an entire discovery/poll pass.
const p2pDialTimeout = dialTimeout

// p2pProbeFuncs is the seam between p2pNodeClient and go-tari-lib/p2p's
// free functions (ProbeChainMetadata/ProbeGetPeers). go-tari-lib/p2p
// exposes no interface of its own to mock, so this package-private
// interface exists purely so tests can inject a fake implementation
// instead of making real P2P network calls — mirroring how grpcNodeClient
// injects dial/dialOpts for the same purpose.
type p2pProbeFuncs interface {
	probeChainMetadata(ctx context.Context, addr string) (*p2p.ChainMetadataInfo, error)
	probeGetPeers(ctx context.Context, addr string, req rpcpkg.GetPeersRequest) ([]*pb.PeerInfo, error)
}

// realP2PProbeFuncs is the default p2pProbeFuncs implementation, backed by
// the real go-tari-lib/p2p package.
type realP2PProbeFuncs struct{}

func (realP2PProbeFuncs) probeChainMetadata(ctx context.Context, addr string) (*p2p.ChainMetadataInfo, error) {
	return p2p.ProbeChainMetadata(ctx, addr)
}

func (realP2PProbeFuncs) probeGetPeers(ctx context.Context, addr string, req rpcpkg.GetPeersRequest) ([]*pb.PeerInfo, error) {
	return p2p.ProbeGetPeers(ctx, addr, req)
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
}

// NewP2PClient returns a NodeClient backed by real go-tari-lib/p2p calls
// against Tari nodes over the comms/RPC-over-P2P transport. It takes no
// required args: each method probes the addr passed to it, per-call.
func NewP2PClient() NodeClient {
	return &p2pNodeClient{probes: realP2PProbeFuncs{}}
}

// GetInfo implements NodeClient.
//
// Version is intentionally left nil here: ChainMetadataInfo (the result of
// go-tari-lib/p2p.ProbeChainMetadata) has no version/user-agent field, and
// there is no cheap way to get one from the same call. p2p.Probe's
// PeerInfo.UserAgent is a separate call that requires a second full
// handshake/dial — not worth the extra round trip just for Version, given
// the GRPC path already covers Version on nodes that expose GRPC.
func (c *p2pNodeClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, p2pDialTimeout)
	defer cancel()

	meta, err := c.probes.probeChainMetadata(ctx, addr)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("p2p ProbeChainMetadata %s: %w", addr, err)
	}

	info := NodeInfo{
		Reachable: true,
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
func (c *p2pNodeClient) GetPeers(ctx context.Context, addr string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, p2pDialTimeout)
	defer cancel()

	peers, err := c.probes.probeGetPeers(ctx, addr, p2p.DefaultGetPeersRequest())
	if err != nil {
		return nil, fmt.Errorf("p2p ProbeGetPeers %s: %w", addr, err)
	}

	seen := make(map[string]bool)
	var addrs []string
	for _, peer := range peers {
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
				addrs = append(addrs, hostPort)
			}
		}
	}

	return addrs, nil
}
