package collector

import (
	"context"
	"encoding/base32"
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
//     multiaddr spec (https://github.com/multiformats/multiaddr). ip4
//     (code 4, 4 bytes) + tcp (code 6, 2 bytes big-endian port), ip6
//     (code 41, 16 bytes) + tcp (code 6, 2 bytes big-endian port), and
//     the self-contained onion (code 444, 12 bytes) / onion3 (code 445,
//     37 bytes) segments are handled, since that covers the ip/tcp and
//     Tor onion-service peer addresses a base node advertises.
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
// "/ip4/1.2.3.4/tcp/18189" or "/ip6/::1/tcp/18189" or "/dns4/host/tcp/port"
// or "/onion3/<address>:<port>".
func parseTextMultiaddr(raw []byte) (string, bool) {
	s := string(raw)
	if !strings.HasPrefix(s, "/") {
		return "", false
	}
	parts := strings.Split(strings.Trim(s, "/"), "/")
	// Minimum viable text multiaddr is a single proto/value pair (as
	// used by the self-contained onion/onion3 segments below); ip4/ip6/
	// dns+tcp combinations need two pairs, but that's enforced by the
	// host/port emptiness check at the end of this function rather than
	// here.
	if len(parts) < 2 || len(parts)%2 != 0 {
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
		case "onion", "onion3":
			// Unlike ip4/ip6/dns+tcp, onion/onion3 are self-contained
			// segments: the value is "<address>:<port>", with the
			// address NOT including the ".onion" suffix (per
			// go-multiaddr's StringToBytes/onion3StB convention). Split
			// on the last ":" (the address itself never contains one)
			// to recover host/port, then lowercase the host (matching
			// the Tor/base32 casing convention used by the
			// binary-decoded path) and append ".onion" to keep the
			// returned host:port format consistent across both paths.
			idx := strings.LastIndex(val, ":")
			if idx < 0 {
				return "", false
			}
			onionHost, onionPort := val[:idx], val[idx+1:]
			if onionHost == "" || onionPort == "" {
				return "", false
			}
			host = strings.ToLower(onionHost) + ".onion"
			port = onionPort
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

	// multiaddrProtoOnion is 0x01bc, the legacy Tor v2 onion address
	// protocol code. Tor v2 onion services are deprecated network-side,
	// but old peer records may still reference them, so keep support.
	// See https://github.com/multiformats/multicodec/blob/master/table.csv.
	multiaddrProtoOnion = 444
	// multiaddrProtoOnion3 is 0x01bd, the Tor v3 onion address protocol
	// code — the current Tor onion service standard.
	// See https://github.com/multiformats/multicodec/blob/master/table.csv.
	multiaddrProtoOnion3 = 445
)

// onionHostLen and onion3HostLen are the raw host byte lengths for the
// onion/onion3 binary segments, per go-multiaddr's transcoders.go
// (onionValidate/onion3Validate): onion is 10 raw host bytes + 2 bytes
// big-endian port (12 bytes total); onion3 is 35 raw host bytes + 2
// bytes big-endian port (37 bytes total).
const (
	onionHostLen  = 10
	onion3HostLen = 35
)

// decodeOnionAddr decodes a self-contained onion/onion3 binary multiaddr
// segment value (raw) into a "host:port" string, per go-multiaddr's
// transcoders.go onionBtS/onion3BtS: raw must be exactly hostLen+2 bytes
// (hostLen raw host bytes followed by a 2-byte big-endian port); the host
// bytes are base32-encoded (encoding/base32, base32.StdEncoding),
// lowercased, and suffixed with ".onion" to reconstruct the textual onion
// address. Returns ("", false) on any length mismatch rather than
// panicking, since onion/onion3 segments are self-contained (host+port in
// a single segment) unlike ip4/ip6/tcp's separate-segment accumulation.
func decodeOnionAddr(raw []byte, hostLen int) (string, bool) {
	if len(raw) != hostLen+2 {
		return "", false
	}
	host := strings.ToLower(base32.StdEncoding.EncodeToString(raw[:hostLen])) + ".onion"
	port := strconv.Itoa(int(binary.BigEndian.Uint16(raw[hostLen : hostLen+2])))
	return net.JoinHostPort(host, port), true
}

// parseBinaryMultiaddr handles the case where raw is a binary-encoded
// multiaddr: a sequence of (varint protocol code, protocol-specific
// value bytes) segments. ip4/tcp, ip6/tcp, and the self-contained
// onion/onion3 segments are extracted; anything else causes a graceful
// ("", false) return.
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
		case multiaddrProtoOnion:
			// onion/onion3 are self-contained: unlike ip4/ip6/tcp
			// (separate segments accumulated across loop iterations
			// into host/port), the single segment's bytes encode BOTH
			// host and port. Decode and return immediately rather than
			// folding into the host/port accumulation above.
			return decodeOnionAddr(buf, onionHostLen)
		case multiaddrProtoOnion3:
			return decodeOnionAddr(buf, onion3HostLen)
		default:
			// Unknown/unsupported protocol segment — this best-effort
			// parser only understands ip4/ip6/tcp/onion/onion3, so bail
			// out rather than guess at the segment's length and
			// misparse the rest.
			return "", false
		}
	}

	if host == "" || port == "" {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}
