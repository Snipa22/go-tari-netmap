package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// NodeCounts is the whole-population node count summary shared by the
// HTML dashboard (internal/web's dashboardCounts, built from this) and
// the JSON GET /v1/stats endpoint (handleStats below). It mirrors exactly
// what buildNodeTableData (internal/web/web.go) used to compute inline
// before this was factored out — same fields, same semantics.
type NodeCounts struct {
	Total    int
	P2P      int
	Registry int
	Both     int

	Confirmed       int
	Unconfirmed     int
	OnionCapable    int
	ClearnetCapable int
	ClearnetOnly    int
}

// ComputeNodeCounts computes NodeCounts from an already-fetched slice of
// nodes and their addresses (see storage.Store.ListNodeAddressesForNodes
// for addrsByNode's shape). It takes already-loaded data rather than
// fetching internally so callers that also need nodes/addrsByNode for
// something else (like internal/web's buildNodeTableData, which needs
// them to render per-row table data too) can share a single
// ListNodes/ListNodeAddressesForNodes round trip instead of querying
// twice; handleStats below, which has no other use for that data, fetches
// it itself just for this call.
//
// Confirmed/OnionCapable/ClearnetCapable/ClearnetOnly are derived via
// ScrubNode — the same single source of truth the rest of this
// package's privacy contract runs through — rather than reimplementing
// that classification here.
func ComputeNodeCounts(nodes []storage.Node, addrsByNode map[uuid.UUID][]storage.NodeAddress) NodeCounts {
	var counts NodeCounts

	for _, n := range nodes {
		counts.Total++
		switch n.DiscoverySource {
		case storage.DiscoverySourceP2P:
			counts.P2P++
		case storage.DiscoverySourceRegistry:
			counts.Registry++
		case storage.DiscoverySourceBoth:
			counts.Both++
		}

		pn := ScrubNode(n, addrsByNode[n.ID])
		if len(pn.PublicKey) > 0 {
			counts.Confirmed++
		} else {
			counts.Unconfirmed++
		}
		if pn.HasOnion {
			counts.OnionCapable++
		}
		if pn.HasIPv4 || pn.HasIPv6 {
			counts.ClearnetCapable++
		}
		if (pn.HasIPv4 || pn.HasIPv6) && !pn.HasOnion {
			counts.ClearnetOnly++
		}
	}

	return counts
}

// fetchNodeCounts is ComputeNodeCounts's fetch-then-compute convenience
// wrapper for callers (handleStats below) that have no other use for the
// underlying nodes/addrsByNode data and just want the whole-population
// counts.
func fetchNodeCounts(ctx context.Context, store storage.Store) (NodeCounts, error) {
	nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		return NodeCounts{}, err
	}

	nodeIDs := make([]uuid.UUID, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.ID
	}
	addrsByNode, err := store.ListNodeAddressesForNodes(ctx, nodeIDs)
	if err != nil {
		return NodeCounts{}, err
	}

	return ComputeNodeCounts(nodes, addrsByNode), nil
}

// statsResponse is the GET /v1/stats response body: the same whole-
// population node counts shown on the HTML dashboard (see NodeCounts),
// plus the network-height context also shown there. NetworkHeight is
// nullable (*int64), matching storage.Store.NetworkHeight's own return —
// it is genuinely unknown until at least one health check with a height
// has been recorded.
type statsResponse struct {
	TotalNodes         int `json:"total_nodes"`
	ConfirmedNodes     int `json:"confirmed_nodes"`
	UnconfirmedNodes   int `json:"unconfirmed_nodes"`
	P2PDiscovered      int `json:"p2p_discovered"`
	RegistryDiscovered int `json:"registry_discovered"`
	BothDiscovered     int `json:"both_discovered"`
	OnionCapable       int `json:"onion_capable"`
	ClearnetCapable    int `json:"clearnet_capable"`
	ClearnetOnly       int `json:"clearnet_only"`

	NetworkHeight          *int64 `json:"network_height"`
	NetworkHeightNodeCount int    `json:"network_height_node_count"`
}

// handleStats serves GET /v1/stats: a simple, read-only JSON snapshot of the
// whole node population's counts (same numbers as the HTML dashboard's
// summary cards, see internal/web's dashboardCounts) plus NetworkHeight
// context, for callers that want the dashboard's headline numbers without
// scraping HTML. No pagination/filtering — this is a single
// whole-population snapshot, not a list, same public trust level as the
// other non-admin routes in this file (no auth).
func handleStats(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		counts, err := fetchNodeCounts(r.Context(), store)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		height, heightNodeCount, err := store.NetworkHeight(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		writeJSON(w, http.StatusOK, statsResponse{
			TotalNodes:         counts.Total,
			ConfirmedNodes:     counts.Confirmed,
			UnconfirmedNodes:   counts.Unconfirmed,
			P2PDiscovered:      counts.P2P,
			RegistryDiscovered: counts.Registry,
			BothDiscovered:     counts.Both,
			OnionCapable:       counts.OnionCapable,
			ClearnetCapable:    counts.ClearnetCapable,
			ClearnetOnly:       counts.ClearnetOnly,

			NetworkHeight:          height,
			NetworkHeightNodeCount: heightNodeCount,
		})
	}
}
