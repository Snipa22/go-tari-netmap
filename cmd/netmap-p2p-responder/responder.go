package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-lib/p2p"
	pb "github.com/Snipa22/go-tari-lib/p2p/proto"

	"github.com/Snipa22/go-tari-netmap/internal/collector"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// dbCallTimeout bounds every individual storage.Store call this binary makes from
// ResponderConfig's callbacks. Both callbacks are invoked from goroutines managed entirely by
// go-tari-lib/p2p's Serve (see its doc comment) — a hung Postgres query must never hang or leak
// those goroutines indefinitely.
const dbCallTimeout = 10 * time.Second

// knownGoodWindow is the "known good peers" recency window BRIEF3.md's governing requirement
// ("we want to let the communication happen long enough that we can give them a list of known
// good peers") specifies: only nodes with at least one reachable=true health check within the
// last hour are ever served, mirroring storage.NodeFilter.ReachableSince's exact intended use
// for this scenario.
const knownGoodWindow = time.Hour

// maxServedPeers caps the number of peers ever returned from a single get_peers response,
// applied on top of (not instead of) go-tari-lib/rpc.ServeGetPeers' own server-side N/MaxClaims/
// MaxAddressesPerClaim bound enforcement against the REQUESTING peer's own ask — this is this
// binary's own independent ceiling on the SOURCE list peerListProvider hands to Serve, so a
// get_peers request with N=0 ("give me everything") still can't make this responder hand out an
// unbounded list from the DB. Matches the throwaway spike binary's prior cap of the same value.
const maxServedPeers = 200

// dbBackedResponder holds the real storage.Store dependency behind ResponderConfig's two
// callbacks (OnPeerIdentity/PeerListProvider), replacing go-tari-lib's throwaway in-memory
// peerStore (cmd/p2p-responder-spike/main.go) with real Postgres persistence — see this
// package's main.go doc comment and BRIEF3.md for the full rationale.
type dbBackedResponder struct {
	store storage.Store

	// logf is normally log.Printf; overridable in tests so failures are visible via t.Logf
	// instead of stdout.
	logf func(format string, args ...interface{})

	// metrics holds this process' own dedicated *responderMetrics (see metrics.go) --
	// threaded in explicitly rather than referenced as a package-level var, since metric
	// names are network-scoped and only built once -network is parsed (see main.go's run()
	// and newResponderMetrics).
	metrics *responderMetrics
}

// onPeerIdentity implements ResponderConfig.OnPeerIdentity: on every successful Noise_XX
// handshake + identity exchange, it records the peer as a confirmed node (we have a real
// confirmed pubkey recovered directly from the handshake, per UpsertConfirmedNode's contract —
// this is NOT a third-party claim) and records one health-check row for it.
//
// remoteAddr — the raw TCP connection's remote address (the client's OS-assigned ephemeral
// source port, e.g. "104.161.20.146:49456") — is NEVER recorded as a claimed, dialable address.
// It is only evidence that some connection from this peer happened, which the health-check row
// below already captures via node.ID linkage; treating it as an advertised address would let a
// probe/monitoring client that claims zero real addresses pollute node_addresses with garbage
// that peerListProvider could later serve back out to real peers as if it were dialable (see
// BRIEF6.md for the full incident writeup and the confirmed-live garbage this produced).
//
// The only addresses ever recorded are genuinely self-claimed ones: every entry in
// identity.Addresses, decoded from its raw rust-multiaddr binary encoding via
// collector.ParsePeerAddress (the exact same decoder already proven against gRPC/P2P GetPeers
// claims elsewhere in this repo — see grpc_client.go's parsePeerAddress).
//
//   - If the peer claims at least one address, the first successfully-decoded one is used as
//     the UpsertConfirmedNode call that ensures the node row exists (case (a)/(b)/(c)/(d) in its
//     doc comment in internal/storage/storage.go, depending on prior state), and every
//     subsequent claimed address gets its own UpsertConfirmedNode call (case (b):
//     "known pubkey, new address" is a no-op-beyond-ensuring-the-row, so looping it once per
//     address is exactly BRIEF3.md's "address-add call once per claimed address", not a
//     reinvention of its multi-address handling).
//   - If the peer claims ZERO addresses (e.g. a bare probe/monitoring client), the node row is
//     still ensured via UpsertConfirmedNodeByPubKey — pubkey identity alone, no address, no
//     node_addresses row — so RecordHealthCheck below still has a real node.ID to link against.
//
// ResponderConfig.OnPeerIdentity's own signature has no context.Context or error return (it's a
// plain synchronous reporting callback — see its doc comment in go-tari-lib/p2p/responder.go),
// so this method derives its own bounded context per call; a failed/slow DB write here is logged
// (never panics) and simply means this one identity exchange's data doesn't make it into
// storage — it must never take down the calling connection goroutine.
func (r *dbBackedResponder) onPeerIdentity(remoteAddr net.Addr, peerStaticKey []byte, identity *p2p.PeerInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), dbCallTimeout)
	defer cancel()

	// Decode every genuinely self-claimed address up front, deduplicating so a peer that
	// (redundantly) repeats the same claim twice doesn't issue two identical DB calls.
	var claimed []string
	seen := make(map[string]bool, len(identity.Addresses))
	for _, raw := range identity.Addresses {
		addr, ok := collector.ParsePeerAddress(raw)
		if !ok || seen[addr] {
			// Skip unparseable claims, best-effort, matching parsePeerAddress's existing
			// "skip rather than emit garbage" convention elsewhere in this repo.
			continue
		}
		seen[addr] = true
		claimed = append(claimed, addr)
	}

	var node storage.Node
	var err error
	if len(claimed) > 0 {
		node, err = r.store.UpsertConfirmedNode(ctx, claimed[0], peerStaticKey, storage.DiscoverySourceP2P)
	} else {
		node, err = r.store.UpsertConfirmedNodeByPubKey(ctx, peerStaticKey, storage.DiscoverySourceP2P)
	}
	if err != nil {
		r.metrics.dbWriteResult.WithLabelValues(dbOperationUpsertNode, resultLabel(false)).Inc()
		r.logf("netmap-p2p-responder: UpsertConfirmedNode(pubkey=%x) failed: %v", peerStaticKey, err)
		return
	}
	r.metrics.dbWriteResult.WithLabelValues(dbOperationUpsertNode, resultLabel(true)).Inc()

	for _, addr := range remainingClaims(claimed) {
		if _, err := r.store.UpsertConfirmedNode(ctx, addr, peerStaticKey, storage.DiscoverySourceP2P); err != nil {
			r.metrics.dbWriteResult.WithLabelValues(dbOperationUpsertNode, resultLabel(false)).Inc()
			r.logf("netmap-p2p-responder: UpsertConfirmedNode(%s, self-claimed address) failed: %v", addr, err)
			continue
		}
		r.metrics.dbWriteResult.WithLabelValues(dbOperationUpsertNode, resultLabel(true)).Inc()
	}

	var version *string
	if identity.UserAgent != "" {
		v := identity.UserAgent
		version = &v
	}

	// PeerIdentityUpdatedAt: the peer's own self-reported identity-signature timestamp,
	// converted from Unix seconds to a *time.Time — mirroring the exact conversion idiom
	// internal/collector/p2p_client.go's GetInfo already uses for the same field.
	var peerIdentityUpdatedAt *time.Time
	if identity.IdentitySignature != nil && identity.IdentitySignature.UpdatedAt != 0 {
		t := time.Unix(identity.IdentitySignature.UpdatedAt, 0)
		peerIdentityUpdatedAt = &t
	}

	if err := r.store.RecordHealthCheck(ctx, storage.HealthCheckInput{
		NodeID:                node.ID,
		Reachable:             true,
		ProbeSource:           storage.ProbeSourceP2P,
		Version:               version,
		PeerIdentityUpdatedAt: peerIdentityUpdatedAt,
	}); err != nil {
		r.metrics.dbWriteResult.WithLabelValues(dbOperationRecordHealth, resultLabel(false)).Inc()
		r.logf("netmap-p2p-responder: RecordHealthCheck(%s) failed: %v", node.ID, err)
		return
	}
	r.metrics.dbWriteResult.WithLabelValues(dbOperationRecordHealth, resultLabel(true)).Inc()
}

// remainingClaims returns every element of claimed after the first (the first was already
// consumed by the UpsertConfirmedNode call above that ensures the node row exists). A plain
// claimed[1:] would panic when claimed is empty (the zero-claimed-addresses case, where
// UpsertConfirmedNodeByPubKey was used instead and this loop has nothing left to do) since a
// low bound of 1 exceeds a zero-length slice's length.
func remainingClaims(claimed []string) []string {
	if len(claimed) == 0 {
		return nil
	}
	return claimed[1:]
}

// peerListProvider implements ResponderConfig.PeerListProvider: it serves REAL confirmed-good
// peers from storage — exactly BRIEF3.md's governing requirement ("we want to let the
// communication happen long enough that we can give them a list of known good peers") — rather
// than a static/in-memory list.
//
// The DB query (storage.NodeFilter{Confirmed: true, ReachableSince: now-1h}) is the SOURCE list;
// go-tari-lib/rpc.ServeGetPeers still independently enforces the requesting peer's own N/
// MaxClaims/MaxAddressesPerClaim bounds server-side on top of this (see its doc comment) — this
// function's own maxServedPeers cap (applied via NodeFilter.Limit) is a further, independent
// ceiling on the source list itself, so a request with N=0 ("give me everything") still can't
// walk away with an unbounded response.
//
// A non-nil error here is exactly what ResponderConfig.PeerListProvider's doc comment describes
// as "no peers available right now": go-tari-lib/p2p.Serve logs it and serves an EMPTY list
// rather than crashing or hanging the substream/connection — see PeerListProvider's own doc
// comment in go-tari-lib/p2p/responder.go.
func (r *dbBackedResponder) peerListProvider(ctx context.Context) ([]*pb.PeerInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, dbCallTimeout)
	defer cancel()

	confirmed := true
	since := time.Now().Add(-knownGoodWindow)
	nodes, err := r.store.ListNodes(ctx, storage.NodeFilter{
		Confirmed:      &confirmed,
		ReachableSince: &since,
		Limit:          maxServedPeers,
	})
	if err != nil {
		return nil, fmt.Errorf("listing known-good confirmed nodes: %w", err)
	}
	if len(nodes) == 0 {
		return nil, nil
	}

	nodeIDs := make([]uuid.UUID, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.ID
	}
	addrsByNode, err := r.store.ListNodeAddressesForNodes(ctx, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("batch-listing node addresses: %w", err)
	}

	peers := make([]*pb.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		if len(n.PublicKey) == 0 {
			// Should not happen given Confirmed: true (public_key IS NOT NULL), but guard
			// against it anyway rather than serving a peer entry with no identity at all.
			continue
		}

		var addrs [][]byte
		for _, a := range addrsByNode[n.ID] {
			encoded, ok := encodeStoredAddress(a.Address)
			if !ok {
				// Best-effort: skip an address this binary can't re-encode into the wire
				// multiaddr format (e.g. IPv6 -- go-tari-lib's EncodeMultiaddrString only
				// implements /ip4/.../tcp/... and /onion3/...:... today) rather than emit
				// garbage on the wire.
				continue
			}
			addrs = append(addrs, encoded)
		}
		if len(addrs) == 0 {
			// A peer entry with zero addresses is useless to whoever we serve it to (they
			// have nothing to dial) -- skip it, same reasoning as
			// PeerHasNoAddresses/PeerHasNoUsableAddresses rejecting it on the other end.
			continue
		}

		peers = append(peers, &pb.PeerInfo{
			PublicKey: n.PublicKey,
			Claims: []*pb.PeerIdentityClaim{
				{
					Addresses:    addrs,
					PeerFeatures: p2p.FeaturesCommunicationNode,
				},
			},
		})
	}
	return peers, nil
}

// encodeStoredAddress converts a storage-shaped "host:port" address (see
// internal/storage.NodeAddress.Address -- e.g. "1.2.3.4:18189" or
// "abc...xyz.onion:18189", the same convention internal/collector's parsePeerAddress/
// classifyAddress already use throughout this repo) back into the raw rust-multiaddr binary
// wire encoding go-tari-lib/p2p.EncodeMultiaddrString produces, for use in an outgoing
// pb.PeerIdentityClaim.Addresses entry.
//
// Only what EncodeMultiaddrString itself supports can be re-encoded: /ip4/.../tcp/... and
// /onion3/...:... (see go-tari-lib/p2p/multiaddr.go's doc comment -- IPv6 is not implemented
// there today). Anything else, including a stored IPv6 address, returns (nil, false) rather
// than silently dropping/garbling it -- callers skip such addresses (see peerListProvider).
func encodeStoredAddress(address string) ([]byte, bool) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, false
	}

	if strings.HasSuffix(strings.ToLower(host), ".onion") {
		onionAddr := strings.TrimSuffix(strings.ToLower(host), ".onion")
		encoded, err := p2p.EncodeMultiaddrString(fmt.Sprintf("/onion3/%s:%s", onionAddr, port))
		if err != nil {
			return nil, false
		}
		return encoded, true
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return nil, false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		// IPv6 -- not supported by go-tari-lib's EncodeMultiaddrString today.
		return nil, false
	}
	encoded, err := p2p.EncodeMultiaddrString(fmt.Sprintf("/ip4/%s/tcp/%s", ip4.String(), port))
	if err != nil {
		return nil, false
	}
	return encoded, true
}
