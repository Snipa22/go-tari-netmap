// Package netaddr holds shared IP-classification logic with no dependency on
// any other github.com/Snipa22/go-tari-netmap package. It exists purely to
// avoid an import cycle: internal/api already imports internal/collector
// (for collector.NodeClient/collector.ParsePeerAddress/collector.PollOnce),
// so internal/collector cannot import internal/api back. Both packages need
// the exact same SSRF-hardening private/loopback/link-local/multicast/
// unspecified IP check — internal/api applies it to publicly submitted
// addresses (POST /nodes) and self-claimed P2P-responder addresses, while
// internal/collector applies it to peer-graph-walk peer-reported addresses
// (a remote node's GetPeers response) — so the classification predicate
// itself lives here, in a leaf package both can import directly.
package netaddr

import "net"

// IsPrivateOrReservedIP reports whether ip is a private/loopback/
// link-local/multicast/unspecified address. This is the canonical
// SSRF-hardening classification shared by internal/api's
// IsPrivateOrReservedIP (which delegates here) and internal/collector's
// peerAddressAllowed: any address this service might end up dialing on its
// own (via its async health-check/probe machinery, or by walking a peer
// graph) must not be allowed to point back into private/reserved network
// space.
func IsPrivateOrReservedIP(ip net.IP) bool {
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}
