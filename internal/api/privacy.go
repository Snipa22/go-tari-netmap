package api

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// PublicNode is the privacy-scrubbed view of a storage.Node exposed via the
// JSON API. It is the single source of truth for what a node looks like to
// an API caller: real address strings (IPv4/IPv6/onion) are NEVER included
// for a node discovered purely by walking the peer graph
// (storage.DiscoverySourceP2P) — Address is nil in that case. Only nodes
// whose owner opted in via the public registry-submission endpoint
// (storage.DiscoverySourceRegistry or storage.DiscoverySourceBoth) have
// Address populated. The HasIPv4/HasIPv6/HasOnion capability booleans are
// always populated regardless of opt-in status, since knowing a node has
// (say) an onion address doesn't reveal what that address actually is.
//
// Node identity is PublicKey, not Address — Address (when present) is
// informational, not an identifier. A nil/empty PublicKey means the node is
// an unconfirmed placeholder (discovered by address alone, not yet
// successfully probed to confirm its real pubkey), not an error.
type PublicNode struct {
	ID              uuid.UUID               `json:"id"`
	PublicKey       []byte                  `json:"public_key,omitempty"`
	DiscoverySource storage.DiscoverySource `json:"discovery_source"`
	Tags            map[string]any          `json:"tags"`
	Label           *string                 `json:"label,omitempty"`
	FirstSeen       time.Time               `json:"first_seen"`
	LastSeen        time.Time               `json:"last_seen"`
	HasIPv4         bool                    `json:"has_ipv4"`
	HasIPv6         bool                    `json:"has_ipv6"`
	HasOnion        bool                    `json:"has_onion"`
	Address         *string                 `json:"address,omitempty"`
}

// PublicNodeDegree is the privacy-scrubbed view of a storage.NodeDegree
// exposed via GET /topology/top-peered. storage.NodeDegree carries a raw
// Address field, so it needs the same treatment as PublicNode rather than
// being returned as-is.
type PublicNodeDegree struct {
	NodeID    uuid.UUID `json:"node_id"`
	PublicKey []byte    `json:"public_key,omitempty"`
	HasIPv4   bool      `json:"has_ipv4"`
	HasIPv6   bool      `json:"has_ipv6"`
	HasOnion  bool      `json:"has_onion"`
	Address   *string   `json:"address,omitempty"`
	Degree    int       `json:"degree"`
	InDegree  int       `json:"in_degree"`
	OutDegree int       `json:"out_degree"`
}

// addrKind classifies the host portion of an address string for the
// purposes of the HasIPv4/HasIPv6/HasOnion capability booleans.
type addrKind int

const (
	addrKindUnknown addrKind = iota
	addrKindIPv4
	addrKindIPv6
	addrKindOnion
)

// classifyAddress splits address into host:port (via net.SplitHostPort)
// and classifies the host: a ".onion" suffix (case-insensitive) is onion;
// otherwise the host is parsed as an IP and classified IPv4 vs IPv6 by
// whether net.IP.To4 succeeds. Malformed addresses (SplitHostPort failure,
// or a host that's neither a valid IP nor ends in .onion) classify as
// addrKindUnknown rather than erroring — a single bad address entry must
// not fail the whole request.
func classifyAddress(address string) addrKind {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return addrKindUnknown
	}

	if strings.HasSuffix(strings.ToLower(host), ".onion") {
		return addrKindOnion
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return addrKindUnknown
	}
	if ip.To4() != nil {
		return addrKindIPv4
	}
	return addrKindIPv6
}

// addressStrings returns the set of raw address strings to classify for
// n: every entry in addrs if non-empty, otherwise falling back to n's own
// Address field (covers rows/callers that haven't loaded node_addresses).
func addressStrings(n storage.Node, addrs []storage.NodeAddress) []string {
	if len(addrs) == 0 {
		if n.Address == "" {
			return nil
		}
		return []string{n.Address}
	}

	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.Address
	}
	return out
}

// primaryAddress picks the single address to expose for n when it's
// opted in (see PublicNode.Address doc comment): n.Address if set,
// otherwise the first entry of addrs, otherwise "" (no known address).
func primaryAddress(n storage.Node, addrs []storage.NodeAddress) string {
	if n.Address != "" {
		return n.Address
	}
	if len(addrs) > 0 {
		return addrs[0].Address
	}
	return ""
}

// ScrubNode builds the PublicNode view of n given its known addresses
// addrs (pass the result of Store.ListNodeAddresses; an empty/nil slice is
// fine and falls back to n.Address for capability classification). This is
// the single place responsible for deciding whether a real address string
// reaches an API response — see the hard privacy requirement in this
// package's doc comment on PublicNode.
func ScrubNode(n storage.Node, addrs []storage.NodeAddress) PublicNode {
	pn := PublicNode{
		ID:              n.ID,
		PublicKey:       n.PublicKey,
		DiscoverySource: n.DiscoverySource,
		Tags:            n.Tags,
		Label:           n.Label,
		FirstSeen:       n.FirstSeen,
		LastSeen:        n.LastSeen,
	}

	for _, a := range addressStrings(n, addrs) {
		switch classifyAddress(a) {
		case addrKindIPv4:
			pn.HasIPv4 = true
		case addrKindIPv6:
			pn.HasIPv6 = true
		case addrKindOnion:
			pn.HasOnion = true
		}
	}

	if n.DiscoverySource == storage.DiscoverySourceRegistry || n.DiscoverySource == storage.DiscoverySourceBoth {
		if addr := primaryAddress(n, addrs); addr != "" {
			pn.Address = &addr
		}
	}

	return pn
}

// ScrubNodeDegree builds the PublicNodeDegree view of nd. Unlike ScrubNode,
// it needs to look up the full node (for DiscoverySource) and its
// addresses, since storage.NodeDegree only carries a raw Address string
// and a NodeID — hence the extra store.GetNode/store.ListNodeAddresses
// round trip per result. Acceptable N+1 given TopPeeredNodes results are
// capped at limit.
func ScrubNodeDegree(ctx context.Context, store storage.Store, nd storage.NodeDegree) (PublicNodeDegree, error) {
	n, err := store.GetNode(ctx, nd.NodeID)
	if err != nil {
		return PublicNodeDegree{}, err
	}
	addrs, err := store.ListNodeAddresses(ctx, nd.NodeID)
	if err != nil {
		return PublicNodeDegree{}, err
	}

	pn := ScrubNode(n, addrs)
	return PublicNodeDegree{
		NodeID:    nd.NodeID,
		PublicKey: pn.PublicKey,
		HasIPv4:   pn.HasIPv4,
		HasIPv6:   pn.HasIPv6,
		HasOnion:  pn.HasOnion,
		Address:   pn.Address,
		Degree:    nd.Degree,
		InDegree:  nd.InDegree,
		OutDegree: nd.OutDegree,
	}, nil
}
