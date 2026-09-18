package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// This file implements the trusted-collector ingestion channel: POST
// /internal/collectors/report. A remote collector satellite (see
// cmd/netmap-p2p-responder, retrofitted to run against internal/remotestore instead of a
// direct Postgres connection) batches up everything it observed since its last flush --
// confirmed nodes, placeholder-discovered nodes, health checks, and peer edges, all keyed by
// address rather than an internal node UUID (a satellite has no central node IDs of its own)
// -- and POSTs it here. The handler applies each item via the EXACT SAME storage.Store methods
// the local collector/responder already use when talking to Postgres directly
// (UpsertDiscoveredNode/UpsertConfirmedNode/RecordHealthCheck/RecordPeerEdgeObservation): this
// is a new entrypoint into the same storage logic, not a parallel write path.
//
// This is a fundamentally different trust model from POST /nodes (see handleCreateNode): that
// endpoint is reachable by untrusted public submitters and queues to pending_submissions for
// human review. This endpoint is a small, operator-configured, single-operator (Alex) trusted
// channel -- writes are applied immediately, with no review queue, gated by a simple
// per-collector API key (see collector_auth.go) rather than adminauth's HTTP Basic Auth (a
// distinct concern from the human-operated /admin/* area).

// collectorReportRequest is the POST /internal/collectors/report request body.
type collectorReportRequest struct {
	// SelfIdentity is this collector's OWN advertised P2P address(es) -- the address(es) it
	// advertises via its own -public-tcp-addr/-onion3-addr equivalent (see
	// internal/remotestore's Config.SelfAddresses) -- used ONLY to tag that specific node
	// row with tags.role = "collector" (see applyCollectorReport's tagging step). This must
	// be explicit, never inferred from ConfirmedNodes/DiscoveredNodes/PeerEdges below, even
	// if one of their addresses happens to coincide with a self-identity address.
	SelfIdentity []string `json:"self_identity"`

	// ConfirmedNodes are nodes this collector directly confirmed (a real pubkey recovered
	// from a handshake or gRPC probe) since its last flush.
	ConfirmedNodes []collectorReportConfirmedNode `json:"confirmed_nodes"`

	// DiscoveredNodes are placeholder addresses seen via a peer walk, not yet confirmed.
	DiscoveredNodes []collectorReportDiscoveredNode `json:"discovered_nodes"`

	// HealthChecks reference addresses, not internal node UUIDs -- this collector has no
	// central node IDs of its own, so the handler resolves address -> node ID server-side
	// via the same upsert calls used elsewhere in this repo (see resolveReportedNodeID).
	HealthChecks []collectorReportHealthCheck `json:"health_checks"`

	// PeerEdges are directed (from_address, to_address) pairs this collector observed via
	// its own peer-graph walk.
	PeerEdges []collectorReportPeerEdge `json:"peer_edges"`
}

type collectorReportConfirmedNode struct {
	Address string `json:"address"`
	// PublicKey is hex-encoded, matching this repo's other hex-over-the-wire pubkey
	// convention (see PublicSeedCandidate.PublicKey).
	PublicKey       string `json:"public_key"`
	DiscoverySource string `json:"discovery_source"`
}

type collectorReportDiscoveredNode struct {
	Address string `json:"address"`
}

type collectorReportHealthCheck struct {
	Address               string     `json:"address"`
	Reachable             bool       `json:"reachable"`
	ProbeSource           string     `json:"probe_source"`
	Height                *int64     `json:"height,omitempty"`
	ChainTipHeight        *int64     `json:"chain_tip_height,omitempty"`
	Version               *string    `json:"version,omitempty"`
	LatencyMS             *int       `json:"latency_ms,omitempty"`
	RxtHashrate           *float64   `json:"rxt_hashrate,omitempty"`
	C29Hashrate           *float64   `json:"c29_hashrate,omitempty"`
	Sha3xHashrate         *float64   `json:"sha3x_hashrate,omitempty"`
	PeerIdentityUpdatedAt *time.Time `json:"peer_identity_updated_at,omitempty"`
}

type collectorReportPeerEdge struct {
	FromAddress string `json:"from_address"`
	ToAddress   string `json:"to_address"`
}

// collectorReportResponse is the POST /internal/collectors/report response body: a count of
// how many of each batch item were successfully applied, for the reporting collector's own
// observability (see internal/remotestore, which logs these counts on a successful flush).
type collectorReportResponse struct {
	ConfirmedNodesApplied  int `json:"confirmed_nodes_applied"`
	DiscoveredNodesApplied int `json:"discovered_nodes_applied"`
	HealthChecksApplied    int `json:"health_checks_applied"`
	PeerEdgesApplied       int `json:"peer_edges_applied"`
	SelfIdentityTagged     int `json:"self_identity_tagged"`
}

// validCollectorReportDiscoverySources is the closed set of storage.DiscoverySource values a
// reported confirmed node may declare -- mirrors the CHECK constraint nodes.discovery_source
// itself enforces at the database level (see 0001_init.sql), just validated here up front so a
// malformed value 400s instead of surfacing as an opaque 500 from the storage layer.
var validCollectorReportDiscoverySources = map[string]bool{
	string(storage.DiscoverySourceP2P):      true,
	string(storage.DiscoverySourceRegistry): true,
	string(storage.DiscoverySourceBoth):     true,
}

// validCollectorReportProbeSources is the closed set of storage.ProbeSource values a reported
// health check may declare, mirroring validCollectorReportDiscoverySources' role for
// storage.ProbeSource's own CHECK constraint.
var validCollectorReportProbeSources = map[string]bool{
	string(storage.ProbeSourceGRPC): true,
	string(storage.ProbeSourceP2P):  true,
}

// validateCollectorReportRequest checks every item of req for well-formedness BEFORE any
// storage write is attempted -- see handleCollectorReport's doc comment for why this two-phase
// (validate everything, then apply) shape is used: a single malformed item must 400 the whole
// request rather than partially applying a batch and 500ing partway through.
func validateCollectorReportRequest(req collectorReportRequest) error {
	for i, addr := range req.SelfIdentity {
		if addr == "" {
			return fmt.Errorf("self_identity[%d]: address is required", i)
		}
	}

	for i, n := range req.ConfirmedNodes {
		if n.Address == "" {
			return fmt.Errorf("confirmed_nodes[%d]: address is required", i)
		}
		if n.PublicKey == "" {
			return fmt.Errorf("confirmed_nodes[%d]: public_key is required", i)
		}
		if _, err := hex.DecodeString(n.PublicKey); err != nil {
			return fmt.Errorf("confirmed_nodes[%d]: public_key is not valid hex: %w", i, err)
		}
		if !validCollectorReportDiscoverySources[n.DiscoverySource] {
			return fmt.Errorf("confirmed_nodes[%d]: invalid discovery_source %q", i, n.DiscoverySource)
		}
	}

	for i, n := range req.DiscoveredNodes {
		if n.Address == "" {
			return fmt.Errorf("discovered_nodes[%d]: address is required", i)
		}
	}

	for i, h := range req.HealthChecks {
		if h.Address == "" {
			return fmt.Errorf("health_checks[%d]: address is required", i)
		}
		if !validCollectorReportProbeSources[h.ProbeSource] {
			return fmt.Errorf("health_checks[%d]: invalid probe_source %q", i, h.ProbeSource)
		}
	}

	for i, e := range req.PeerEdges {
		if e.FromAddress == "" {
			return fmt.Errorf("peer_edges[%d]: from_address is required", i)
		}
		if e.ToAddress == "" {
			return fmt.Errorf("peer_edges[%d]: to_address is required", i)
		}
	}

	return nil
}

// handleCollectorReport applies a trusted remote collector satellite's batched observations
// via the same storage.Store methods the local collector/responder already use -- see this
// file's doc comment for the full contract and NewRouter's doc comment for how this route is
// authenticated/mounted.
//
// Validation happens in two phases: validateCollectorReportRequest checks every item for
// well-formedness up front, so a malformed payload 400s without partially applying anything;
// only once the whole batch validates does applyCollectorReport start issuing storage writes.
func handleCollectorReport(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req collectorReportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}

		if err := validateCollectorReportRequest(req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		resp, err := applyCollectorReport(r.Context(), store, req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// reportNodeResolver resolves an address to the node.ID storage.Store assigned it, upserting a
// placeholder (UpsertDiscoveredNode with DiscoverySourceP2P) the first time a given address is
// seen within a single applyCollectorReport call, and caching that ID for every subsequent
// reference to the same address later in the SAME request (e.g. a health check or peer edge
// referencing an address already resolved by an earlier confirmed_nodes/discovered_nodes
// entry) -- exactly one upsert call per distinct address per request, never a redundant one.
type reportNodeResolver struct {
	ctx        context.Context
	store      storage.Store
	addressIDs map[string]storage.Node
}

func newReportNodeResolver(ctx context.Context, store storage.Store) *reportNodeResolver {
	return &reportNodeResolver{ctx: ctx, store: store, addressIDs: make(map[string]storage.Node)}
}

// resolve returns the storage.Node for address, from this request's own cache if already
// resolved, otherwise via UpsertDiscoveredNode (which is always safe to call even if address
// turns out to already be confirmed elsewhere -- see UpsertDiscoveredNode's own doc comment:
// it never downgrades an existing confirmed node, it only bumps last_seen/merges
// discovery_source/tags).
func (rr *reportNodeResolver) resolve(address string) (storage.Node, error) {
	if n, ok := rr.addressIDs[address]; ok {
		return n, nil
	}
	n, err := rr.store.UpsertDiscoveredNode(rr.ctx, address, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		return storage.Node{}, fmt.Errorf("resolve address %s: %w", address, err)
	}
	rr.addressIDs[address] = n
	return n, nil
}

// remember records that address already resolves to n, without issuing any storage call --
// used right after a confirmed_nodes entry is applied via UpsertConfirmedNode, so a later
// reference to that same address (health_checks/peer_edges) reuses n.ID instead of resolving
// it again via UpsertDiscoveredNode.
func (rr *reportNodeResolver) remember(address string, n storage.Node) {
	rr.addressIDs[address] = n
}

// applyCollectorReport applies every item of a validated req via storage.Store, in this order:
//
//  1. ConfirmedNodes (UpsertConfirmedNode) -- remembered in resolver so later steps
//     referencing the same address (HealthChecks/PeerEdges) don't issue a redundant upsert.
//  2. DiscoveredNodes (UpsertDiscoveredNode), same remember-in-resolver treatment.
//  3. SelfIdentity tagging (UpsertDiscoveredNode with tags={"role":"collector"}) -- applied
//     AFTER 1/2 so that if a self-identity address happens to already have been resolved
//     above (e.g. it was also reported as a confirmed node), tagging merges onto that same
//     row rather than racing it; UpsertDiscoveredNode's tags-merge semantics (tags = tags ||
//     $2) mean this never clobbers a pubkey/discovery_source set by an earlier step. This
//     tagging is applied ONLY to addresses explicitly listed in req.SelfIdentity -- never to
//     any address that merely happens to also appear in ConfirmedNodes/DiscoveredNodes/
//     PeerEdges, per this endpoint's governing brief.
//  4. HealthChecks (RecordHealthCheck), resolving address -> node ID via the resolver (which
//     upserts a placeholder if the address wasn't already resolved by 1-3).
//  5. PeerEdges (RecordPeerEdgeObservation), same resolution as 4.
func applyCollectorReport(ctx context.Context, store storage.Store, req collectorReportRequest) (collectorReportResponse, error) {
	var resp collectorReportResponse
	resolver := newReportNodeResolver(ctx, store)

	for _, cn := range req.ConfirmedNodes {
		pubkey, err := hex.DecodeString(cn.PublicKey)
		if err != nil {
			// Already validated by validateCollectorReportRequest -- unreachable in
			// practice, but fail loudly rather than silently skip if it somehow isn't.
			return collectorReportResponse{}, fmt.Errorf("confirmed_nodes: re-decode public_key for %s: %w", cn.Address, err)
		}
		n, err := store.UpsertConfirmedNode(ctx, cn.Address, pubkey, storage.DiscoverySource(cn.DiscoverySource))
		if err != nil {
			return collectorReportResponse{}, fmt.Errorf("confirmed_nodes: upsert %s: %w", cn.Address, err)
		}
		resolver.remember(cn.Address, n)
		resp.ConfirmedNodesApplied++
	}

	for _, dn := range req.DiscoveredNodes {
		n, err := store.UpsertDiscoveredNode(ctx, dn.Address, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			return collectorReportResponse{}, fmt.Errorf("discovered_nodes: upsert %s: %w", dn.Address, err)
		}
		resolver.remember(dn.Address, n)
		resp.DiscoveredNodesApplied++
	}

	// SelfIdentity tagging: see this function's doc comment for why this is a distinct,
	// explicit step rather than something folded into the loops above -- tags.role =
	// "collector" is applied ONLY to addresses named here, regardless of whether they also
	// appear (by coincidence or otherwise) in ConfirmedNodes/DiscoveredNodes/PeerEdges.
	for _, addr := range req.SelfIdentity {
		if _, err := store.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, map[string]any{"role": collectorRoleTagValue}, nil); err != nil {
			return collectorReportResponse{}, fmt.Errorf("self_identity: tag %s: %w", addr, err)
		}
		resp.SelfIdentityTagged++
	}

	for _, hc := range req.HealthChecks {
		n, err := resolver.resolve(hc.Address)
		if err != nil {
			return collectorReportResponse{}, fmt.Errorf("health_checks: %w", err)
		}
		if err := store.RecordHealthCheck(ctx, storage.HealthCheckInput{
			NodeID:                n.ID,
			Reachable:             hc.Reachable,
			ProbeSource:           storage.ProbeSource(hc.ProbeSource),
			Height:                hc.Height,
			ChainTipHeight:        hc.ChainTipHeight,
			Version:               hc.Version,
			LatencyMS:             hc.LatencyMS,
			RxtHashrate:           hc.RxtHashrate,
			C29Hashrate:           hc.C29Hashrate,
			Sha3xHashrate:         hc.Sha3xHashrate,
			PeerIdentityUpdatedAt: hc.PeerIdentityUpdatedAt,
		}); err != nil {
			return collectorReportResponse{}, fmt.Errorf("health_checks: record for %s: %w", hc.Address, err)
		}
		resp.HealthChecksApplied++
	}

	for _, pe := range req.PeerEdges {
		fromNode, err := resolver.resolve(pe.FromAddress)
		if err != nil {
			return collectorReportResponse{}, fmt.Errorf("peer_edges: %w", err)
		}
		toNode, err := resolver.resolve(pe.ToAddress)
		if err != nil {
			return collectorReportResponse{}, fmt.Errorf("peer_edges: %w", err)
		}
		if err := store.RecordPeerEdgeObservation(ctx, fromNode.ID, toNode.ID); err != nil {
			return collectorReportResponse{}, fmt.Errorf("peer_edges: record %s -> %s: %w", pe.FromAddress, pe.ToAddress, err)
		}
		resp.PeerEdgesApplied++
	}

	return resp, nil
}
