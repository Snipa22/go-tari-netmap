package collector

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
)

// dialTimeout bounds how long grpcNodeClient waits to establish a
// connection (and, transitively, to complete each RPC via the per-call
// context) before giving up on an addr. Unreachable nodes are a normal,
// expected case for this collector (peers come and go, seed lists go
// stale, etc.), so this must stay short: a hung dial to one dead peer
// must not stall an entire discovery/poll pass.
const dialTimeout = 5 * time.Second

// dialFunc is the shape of grpc.NewClient, extracted so tests can inject a
// bufconn-backed dialer without needing grpcNodeClient to hardcode real
// network dialing.
type dialFunc func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

// grpcNodeClient is the real go-tari-grpc-lib-backed NodeClient
// implementation. It talks to a Tari base node's gRPC BaseNode service.
//
// Connection lifecycle: it dials fresh per call and closes the connection
// via defer immediately after. This is the simplest correct option and is
// fine given the collector's poll cadence (PollIntervalGeneric/
// PollIntervalPoolOwned are minutes-to-hours apart per node, not a hot
// path) — the cost of a fresh TCP handshake + gRPC setup per call is
// negligible compared to that cadence, and it avoids having to manage a
// connection pool/cache with its own liveness and cleanup concerns.
type grpcNodeClient struct {
	// dial is normally grpc.NewClient; overridable in tests to substitute
	// a bufconn dialer so tests exercise real wire-level gRPC/protobuf
	// serialization against an in-process fake server instead of a real
	// network.
	dial dialFunc

	// dialOpts are extra grpc.DialOption values appended after the
	// default insecure-transport option. Tests use this to inject
	// grpc.WithContextDialer pointing at a bufconn listener.
	dialOpts []grpc.DialOption
}

// NewGRPCClient returns a NodeClient backed by real go-tari-grpc-lib/v3
// gRPC calls against Tari base nodes. It takes no required args: each
// method dials the addr passed to it, per-call.
func NewGRPCClient() NodeClient {
	return &grpcNodeClient{dial: grpc.NewClient}
}

// dialNode dials addr and returns a ready-to-use BaseNodeClient plus the
// underlying *grpc.ClientConn for the caller to Close via defer.
func (c *grpcNodeClient) dialNode(addr string) (tari_generated.BaseNodeClient, *grpc.ClientConn, error) {
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.dialOpts...)
	// Tari base-node gRPC is plaintext (insecure) by default; there is no
	// TLS story here to plug in short of a much larger change to how
	// nodes are configured/discovered, which is out of scope.
	conn, err := c.dial(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return tari_generated.NewBaseNodeClient(conn), conn, nil
}

// GetInfo implements NodeClient.
func (c *grpcNodeClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	start := time.Now()

	client, conn, err := c.dialNode(addr)
	if err != nil {
		return NodeInfo{}, err
	}
	defer conn.Close()

	tip, err := client.GetTipInfo(ctx, &tari_generated.Empty{})
	if err != nil {
		return NodeInfo{}, fmt.Errorf("GetTipInfo %s: %w", addr, err)
	}
	latency := time.Since(start)

	version, err := client.GetVersion(ctx, &tari_generated.Empty{})
	if err != nil {
		return NodeInfo{}, fmt.Errorf("GetVersion %s: %w", addr, err)
	}

	info := NodeInfo{
		Reachable: true,
	}

	// Identify recovers the node's own confirmed pubkey (as opposed to
	// GetPeers' Peer.PublicKey, which is a claim addr makes about its
	// peers, not about itself). This is best-effort/non-fatal: an
	// Identify failure must not affect Reachable/Height/Version, which
	// are already established by the calls above — only PublicKey is
	// left nil in that case.
	if identity, err := client.Identify(ctx, &tari_generated.Empty{}); err != nil {
		log.Printf("GetInfo %s: Identify failed (non-fatal, PublicKey left nil): %v", addr, err)
	} else {
		info.PublicKey = identity.GetPublicKey()
	}

	if tip.Metadata != nil {
		height := int64(tip.Metadata.BestBlockHeight)
		info.Height = &height
		// Tari doesn't expose a separate "network tip vs our tip" split
		// via GetTipInfo alone once initial sync is achieved — the
		// node's own best_block_height *is* its view of the chain tip.
		// ChainTipHeight is left equal to Height here rather than nil so
		// downstream consumers that assume both are populated for a
		// reachable node get consistent data; if a more authoritative
		// network-wide tip source is added later, prefer that instead.
		chainTip := height
		info.ChainTipHeight = &chainTip
	}

	if version.Version != "" {
		v := version.Version
		info.Version = &v
	}

	latencyMS := int(latency.Milliseconds())
	info.LatencyMS = &latencyMS

	// RxtHashrate/C29Hashrate/Sha3xHashrate are not derivable from
	// GetTipInfo/GetVersion — left nil per the task's explicit scope.
	_ = tip.InitialSyncAchieved
	_ = tip.BaseNodeState
	_ = tip.FailedCheckpoints

	return info, nil
}

// GetPeers implements NodeClient.
func (c *grpcNodeClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	client, conn, err := c.dialNode(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	resp, err := client.ListConnectedPeers(ctx, &tari_generated.Empty{})
	if err != nil {
		return nil, fmt.Errorf("ListConnectedPeers %s: %w", addr, err)
	}

	var peers []DiscoveredPeer
	for _, peer := range resp.ConnectedPeers {
		var pubKey []byte
		if len(peer.PublicKey) > 0 {
			pubKey = peer.PublicKey
		}
		for _, a := range peer.Addresses {
			hostPort, ok := parsePeerAddress(a.Address)
			if !ok {
				// Best-effort: skip unparseable addresses rather than
				// emitting garbage. Only drop the whole peer if it ends
				// up with zero parseable addresses (handled below by
				// simply not appending anything for it).
				continue
			}
			peers = append(peers, DiscoveredPeer{Address: hostPort, PublicKey: pubKey})
		}
	}

	return peers, nil
}

// parsePeerAddress attempts to extract a "host:port" string from a Tari
// peer Address's raw bytes.
//
// TODO(netmap): this needs validation against a real node's actual wire
// encoding — no live Tari node is reachable from this sandbox to confirm
// against. Two encodings are attempted, in order:
//  1. The bytes are simply the UTF-8 text of a human-readable multiaddr,
//     e.g. "/ip4/1.2.3.4/tcp/18189" or "/ip6/::1/tcp/18189".
//  2. The bytes are a binary-encoded multiaddr: a sequence of
//     (varint protocol code, protocol-specific value) pairs per the
//     multiaddr spec (https://github.com/multiformats/multiaddr). Only
//     ip4 (code 4, 4 bytes) + tcp (code 6, 2 bytes big-endian port) and
//     ip6 (code 41, 16 bytes) + tcp (code 6, 2 bytes big-endian port)
//     combinations are handled, since that covers the ip/tcp peer
//     addresses a base node advertises.
//
// If neither encoding yields a usable host:port, it returns ("", false)
// so the caller can skip the address without crashing or emitting
// garbage.
func parsePeerAddress(raw []byte) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}

	if hostPort, ok := parseTextMultiaddr(raw); ok {
		return hostPort, true
	}

	if hostPort, ok := parseBinaryMultiaddr(raw); ok {
		return hostPort, true
	}

	return "", false
}

// parseTextMultiaddr handles the case where raw is just the UTF-8 bytes of
// a standard human-readable multiaddr string, e.g.
// "/ip4/1.2.3.4/tcp/18189" or "/ip6/::1/tcp/18189" or "/dns4/host/tcp/port".
func parseTextMultiaddr(raw []byte) (string, bool) {
	s := string(raw)
	if !strings.HasPrefix(s, "/") {
		return "", false
	}
	parts := strings.Split(strings.Trim(s, "/"), "/")
	if len(parts) < 4 {
		return "", false
	}

	var host, port string
	for i := 0; i+1 < len(parts); i += 2 {
		proto, val := parts[i], parts[i+1]
		switch proto {
		case "ip4", "ip6", "dns4", "dns6", "dns":
			host = val
		case "tcp":
			port = val
		}
	}

	if host == "" || port == "" {
		return "", false
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}

// Binary multiaddr protocol codes relevant to Tari peer addresses. See
// https://github.com/multiformats/multicodec/blob/master/table.csv.
const (
	multiaddrProtoIP4 = 4
	multiaddrProtoTCP = 6
	multiaddrProtoIP6 = 41
)

// parseBinaryMultiaddr handles the case where raw is a binary-encoded
// multiaddr: a sequence of (varint protocol code, protocol-specific
// value bytes) segments. Only ip4/tcp and ip6/tcp combinations are
// extracted; anything else causes a graceful ("", false) return.
func parseBinaryMultiaddr(raw []byte) (string, bool) {
	var host, port string

	buf := raw
	for len(buf) > 0 {
		code, n := binary.Uvarint(buf)
		if n <= 0 {
			return "", false
		}
		buf = buf[n:]

		switch code {
		case multiaddrProtoIP4:
			if len(buf) < 4 {
				return "", false
			}
			host = net.IP(buf[:4]).String()
			buf = buf[4:]
		case multiaddrProtoIP6:
			if len(buf) < 16 {
				return "", false
			}
			host = net.IP(buf[:16]).String()
			buf = buf[16:]
		case multiaddrProtoTCP:
			if len(buf) < 2 {
				return "", false
			}
			port = strconv.Itoa(int(binary.BigEndian.Uint16(buf[:2])))
			buf = buf[2:]
		default:
			// Unknown/unsupported protocol segment — this best-effort
			// parser only understands ip4/ip6/tcp, so bail out rather
			// than guess at the segment's length and misparse the rest.
			return "", false
		}
	}

	if host == "" || port == "" {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}
