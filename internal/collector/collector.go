// Package collector polls Tari nodes and discovers peers by walking the
// peer graph starting from a set of seed nodes.
//
// Two independent transports are supported for both discovery and
// polling: a gRPC-backed NodeClient (go-tari-grpc-lib, see grpc_client.go)
// and a P2P-backed NodeClient (go-tari-lib/p2p, see p2p_client.go). Either,
// both, or neither may be configured on a Collector; each is probed
// independently so that, e.g., a node reachable only over one transport
// still contributes data.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// PollIntervalGeneric is the minimum interval between polls of a discovered/generic node.
// Enforced for politeness — polling more aggressively risks looking like abuse to the
// wider Tari network.
const PollIntervalGeneric = 2 * time.Hour

// PollIntervalPoolOwned is the poll interval for nodes explicitly tagged as pool-owned.
// TODO(netmap): placeholder value — needs confirmation from the pool-ops team on the
// actual desired cadence before this ships.
const PollIntervalPoolOwned = 5 * time.Minute

// PollIntervalUnconfirmed is the poll interval for unconfirmed placeholder
// nodes (Node.PublicKey == nil), discovered via peer-walk but not yet
// directly probed. These are polled more frequently than
// PollIntervalGeneric so that a successful direct probe — and the
// resulting merge into the real confirmed node it belongs to (e.g. when
// the same real node advertises both a clearnet and onion address) —
// happens sooner, without polling so aggressively that it violates
// node-politeness norms.
const PollIntervalUnconfirmed = 15 * time.Minute

// DiscoveryIntervalGeneric is the minimum interval between discovery-walk
// dials of an already-known generic node. Enforced for the same
// politeness reasons as PollIntervalGeneric.
const DiscoveryIntervalGeneric = 6 * time.Hour

// DiscoveryIntervalPoolOwned is the discovery-walk cooldown for nodes
// explicitly tagged as pool-owned — our own infra, walked more often
// since we want fresher topology data for it specifically.
const DiscoveryIntervalPoolOwned = 30 * time.Minute

// defaultTickInterval is how often Run checks which known nodes are due for
// a poll when Collector.TickInterval is unset. It is independent of
// PollIntervalGeneric/PollIntervalPoolOwned, which govern per-node poll
// cadence — this just controls the granularity of that check.
const defaultTickInterval = 5 * time.Minute

// NodeInfo is the subset of a Tari base node's health/sync-status info
// needed to record a health check.
type NodeInfo struct {
	Reachable      bool
	PublicKey      []byte
	Height         *int64
	ChainTipHeight *int64
	Version        *string
	LatencyMS      *int
	RxtHashrate    *float64
	C29Hashrate    *float64
	Sha3xHashrate  *float64

	// PeerIdentityUpdatedAt is the peer's own self-reported
	// identity-signature timestamp — see storage.HealthCheck's field of
	// the same name for the full rationale. Only ever populated by the
	// P2P transport (see p2p_client.go's GetInfo); the gRPC path has no
	// equivalent concept and always leaves this nil.
	PeerIdentityUpdatedAt *time.Time
}

// DiscoveredPeer is one address+pubkey pairing reported by a directly
// connected peer during a GetPeers call. PublicKey may be nil if the peer
// entry had no usable pubkey. Note that this pubkey is a claim the CURRENT
// node (the one being walked) makes about its peer — it is NOT something
// WE ourselves have confirmed by directly probing that peer, so it must
// not be treated as a confirmed identity (see discoverWith's doc comment
// in this file for why storage only ever records these as
// UpsertDiscoveredNode placeholders, never UpsertConfirmedNode).
type DiscoveredPeer struct {
	Address   string
	PublicKey []byte
}

// NodeClient is the interface the collector uses to talk to Tari base
// nodes. This models the subset of go-tari-grpc-lib's base-node client
// (ListConnectedPeers/GetPeers plus a health/sync-status call) the
// collector needs.
type NodeClient interface {
	// GetPeers returns addr's directly connected peers.
	GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error)

	// GetInfo returns addr's current health/sync-status info.
	GetInfo(ctx context.Context, addr string) (NodeInfo, error)
}

// ErrNotConnected is returned by stubClient's methods. It is a real no-op
// placeholder (not a fake-success mock), so the collector compiles and runs
// safely with no real network calls by default, until go-tari-grpc-lib is
// wired in as a real dependency.
var ErrNotConnected = errors.New("collector: not yet connected to a real Tari node client")

// stubClient is the default NodeClient implementation used until
// go-tari-grpc-lib is wired in. It performs no network I/O.
type stubClient struct{}

// NewStubClient returns the default, no-op NodeClient.
func NewStubClient() NodeClient {
	return &stubClient{}
}

func (s *stubClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	return nil, ErrNotConnected
}

func (s *stubClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	return NodeInfo{}, ErrNotConnected
}

// Config holds the collector's configuration.
type Config struct {
	// SeedNodes are the addresses of the seed nodes used to bootstrap peer
	// discovery by walking the Tari peer graph.
	SeedNodes []string

	// DialJitter is the delay inserted before each per-node dial within a
	// single Discover or Poll pass (only immediately before a dial that's
	// actually about to happen — nodes skipped by a due()/dueForDiscovery
	// cooldown check don't incur it), to avoid hammering many different
	// nodes in rapid succession even when overall pass frequency is
	// polite.
	//
	// Zero (the Go zero value, and thus the default for any Collector
	// that doesn't set this explicitly, including every existing test in
	// this package) means no delay at all — the collector package
	// intentionally does NOT substitute a nonzero default for zero, so
	// that tests stay fully test-controllable and fast by default.
	// Production callers that want the "don't hammer nodes" behavior
	// (500ms, per this package's recommended default) must set this
	// field explicitly; see cmd/netmap/main.go.
	DialJitter time.Duration
}

// Collector polls Tari nodes and discovers peers.
type Collector struct {
	cfg Config

	// Storage persists discovered nodes, peer edges, and health checks.
	Storage storage.Store

	// GRPCClient talks to Tari base nodes over go-tari-grpc-lib's gRPC
	// BaseNode service. Optional/nilable: if nil, the gRPC probe is
	// skipped entirely for both Discover and Poll, rather than erroring.
	GRPCClient NodeClient

	// P2PClient talks to Tari nodes over go-tari-lib/p2p's direct
	// comms/RPC-over-P2P transport. Optional/nilable: if nil, the P2P
	// probe is skipped entirely for both Discover and Poll, rather than
	// erroring. A Collector with only one of GRPCClient/P2PClient set
	// still works correctly, just without data from the other transport.
	P2PClient NodeClient

	// TickInterval governs both how often Run checks which known nodes
	// are due for a poll, and how often Run kicks off a fresh discovery
	// pass. The two run on independent tickers/goroutines (see Run) but
	// share this same cadence value for simplicity — there's no need for
	// separate configuration since a discovery pass that's still running
	// when its next tick fires simply doesn't overlap with itself (Run
	// waits for the previous Discover call to return before scheduling
	// off the next tick), and the same is true for Poll. Defaults to
	// defaultTickInterval if unset. Kept short and independent of the
	// (much longer) per-node poll cadence so tests don't need to wait an
	// hour for anything.
	TickInterval time.Duration

	mu            sync.Mutex
	nextPoll      map[string]time.Time // address -> next poll due time
	nextDiscovery map[string]time.Time // discoveryCooldownKey(transport, address) -> next discovery-walk due time
}

// New returns a new Collector for the given config. GRPCClient and
// P2PClient are left nil (network calls are opt-in): set Storage
// (required) and at least one of GRPCClient/P2PClient (recommended, but
// not required — a Collector with neither set just does nothing on
// Discover/Poll rather than panicking) before calling Run, Discover, or
// Poll.
func New(cfg Config) *Collector {
	return &Collector{
		cfg:           cfg,
		nextPoll:      make(map[string]time.Time),
		nextDiscovery: make(map[string]time.Time),
	}
}

// Run starts the collector's discovery and poll loops. Discovery and
// polling run on independent goroutines/tickers so that a slow or
// never-ending Discover pass (a synchronous BFS over the real peer graph,
// with real network dials — this can take minutes against the real
// mainnet, or longer as the network grows) cannot starve Poll, which is
// what actually produces the health-check data the rest of this tool is
// for. Both goroutines share Storage and the NodeClients, and both
// observe ctx cancellation independently. Run blocks until both have
// exited (via a sync.WaitGroup) and returns nil on clean shutdown.
//
// Discover() only ever touches Storage and the NodeClients — never
// c.nextPoll — so running it concurrently with Poll() introduces no new
// data race: c.nextPoll access is already guarded by c.mu for
// Poll-vs-Poll safety (due/setNextPoll), and Discover never reads or
// writes it.
func (c *Collector) Run(ctx context.Context) error {
	tick := c.TickInterval
	if tick <= 0 {
		tick = defaultTickInterval
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.runDiscoverLoop(ctx, tick)
	}()

	go func() {
		defer wg.Done()
		c.runPollLoop(ctx, tick)
	}()

	wg.Wait()
	return nil
}

// runDiscoverLoop runs Discover once immediately, then on every tick,
// until ctx is cancelled. It runs entirely independently of runPollLoop.
func (c *Collector) runDiscoverLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.Discover(ctx); err != nil {
		log.Printf("collector: discovery pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Discover(ctx); err != nil {
				log.Printf("collector: discovery pass error: %v", err)
			}
		}
	}
}

// runPollLoop runs Poll once immediately, then on every tick, until ctx
// is cancelled. It runs entirely independently of runDiscoverLoop, so a
// slow/hanging Discover pass never delays or starves polling.
func (c *Collector) runPollLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.Poll(ctx); err != nil {
		log.Printf("collector: poll pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Poll(ctx); err != nil {
				log.Printf("collector: poll pass error: %v", err)
			}
		}
	}
}

// Discover walks the peer graph starting from Config.SeedNodes, deduping
// visited addresses per transport, and records discovered nodes and edges
// in Storage. GRPCClient and P2PClient (if non-nil) are each walked as a
// separate, independent pass over the same seed nodes — one transport's
// walk failing/erroring must not abort the other's.
func (c *Collector) Discover(ctx context.Context) error {
	if c.Storage == nil {
		return errors.New("collector: Storage is not configured")
	}

	if c.GRPCClient != nil {
		c.discoverWith(ctx, c.GRPCClient, "grpc")
	}
	if c.P2PClient != nil {
		c.discoverWith(ctx, c.P2PClient, "p2p")
	}

	return nil
}

// discoverWith walks the peer graph starting from Config.SeedNodes via
// client.GetPeers, deduping visited addresses within this pass, and
// records discovered nodes and edges in Storage. transportLabel is used
// only for log messages, to distinguish which transport's walk a given
// log line came from when both are configured.
//
// Discovered nodes/edges are recorded with DiscoverySourceP2P regardless
// of which NodeClient transport (gRPC or P2P-RPC) performed the walk —
// see DiscoverySource's doc comment in models.go: it means "discovered by
// walking the peer graph" broadly, not the P2P-RPC transport specifically.
// This is orthogonal to ProbeSource, which does track which transport a
// given health check came from.
//
// Every address popped off the queue is still upserted into Storage
// unconditionally via UpsertDiscoveredNode — recording that a node
// exists/was seen is cheap and not the politeness concern. What IS gated
// by a per-node discovery cooldown (dueForDiscovery/setNextDiscovery,
// mirroring due/setNextPoll) is the client.GetPeers call itself:
// re-dialing a node to ask for its current peer list is the
// expensive/impolite part, so a node not yet due for re-discovery is
// upserted (recording it was seen this pass) but not dialed, and its
// peers are therefore not (re)enqueued from it this pass. This cooldown
// is a separate, cross-pass/cross-call concept from the visited map
// above, which only dedupes within a single discoverWith call to prevent
// infinite loops/reprocessing.
//
// The cooldown key is scoped per-transport (transportLabel+addr), not
// just addr: gRPC and P2P-RPC are independent network dials/connections
// to the same address, so one transport's walk having just dialed addr
// must not block the other transport's independent walk of that same
// addr within the same (or a concurrent) Discover() call — see
// TestDiscoverWalksBothTransportsIndependently, which specifically
// exercises this.
//
// Every node upserted here — the node being walked and every peer it
// reports — goes through UpsertDiscoveredNode, never UpsertConfirmedNode,
// even for peer entries that carry a DiscoveredPeer.PublicKey. That
// pubkey is a claim the node being walked makes about its peer; WE have
// not ourselves directly, successfully probed that peer to confirm it —
// only PollOnce's GetInfo path (a real direct probe) is allowed to call
// UpsertConfirmedNode. Treating a peer-reported pubkey as confirmed here
// would be an easy mistake (the data is right there!) but would let a
// single misbehaving/malicious peer plant an arbitrary pubkey-to-address
// binding into storage without ever being probed itself.
func (c *Collector) discoverWith(ctx context.Context, client NodeClient, transportLabel string) {
	visited := make(map[string]bool)
	queue := append([]string{}, c.cfg.SeedNodes...)
	now := time.Now()

	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]
		if visited[addr] {
			continue
		}
		visited[addr] = true

		fromNode, err := c.Storage.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			log.Printf("collector: [%s] upsert node %s: %v", transportLabel, addr, err)
			continue
		}

		cooldownKey := discoveryCooldownKey(transportLabel, addr)
		if !c.dueForDiscovery(cooldownKey, now) {
			continue
		}

		if jitter := c.dialJitter(); jitter > 0 {
			time.Sleep(jitter)
		}

		peers, err := client.GetPeers(ctx, addr)
		if err != nil {
			log.Printf("collector: [%s] get peers for %s: %v", transportLabel, addr, err)
			continue
		}
		c.setNextDiscovery(cooldownKey, now.Add(c.discoveryInterval(fromNode)))

		for _, peer := range peers {
			// See this method's doc comment: peer.PublicKey is
			// intentionally not passed through to storage here — only
			// address-based discovery is recorded from a peer-walk hop.
			toNode, err := c.Storage.UpsertDiscoveredNode(ctx, peer.Address, storage.DiscoverySourceP2P, nil, nil)
			if err != nil {
				log.Printf("collector: [%s] upsert node %s: %v", transportLabel, peer.Address, err)
				continue
			}

			if err := c.Storage.RecordPeerEdgeObservation(ctx, fromNode.ID, toNode.ID); err != nil {
				log.Printf("collector: [%s] record edge observation %s -> %s: %v", transportLabel, addr, peer.Address, err)
			}

			if !visited[peer.Address] {
				queue = append(queue, peer.Address)
			}
		}
	}
}

// discoveryCooldownKey builds the nextDiscovery map key for a given
// transport+address pair. See discoverWith's doc comment for why this is
// scoped per-transport rather than just per-address.
func discoveryCooldownKey(transportLabel, addr string) string {
	return transportLabel + ":" + addr
}

// Poll checks all known nodes and, for those whose next-poll time is due,
// calls PollOnce (which independently attempts GRPCClient and P2PClient,
// whichever are non-nil).
func (c *Collector) Poll(ctx context.Context) error {
	if c.Storage == nil {
		return errors.New("collector: Storage is not configured")
	}

	nodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		return fmt.Errorf("collector: list nodes: %w", err)
	}

	now := time.Now()
	for _, n := range nodes {
		if !c.due(n.Address, now) {
			continue
		}
		c.setNextPoll(n.Address, now.Add(c.pollInterval(n)))
		if jitter := c.dialJitter(); jitter > 0 {
			time.Sleep(jitter)
		}
		if err := PollOnce(ctx, c.GRPCClient, c.P2PClient, c.Storage, n); err != nil {
			log.Printf("collector: poll %s: %v", n.Address, err)
		}
	}
	return nil
}

// PollOnce performs a single synchronous health check of node via
// grpcClient and p2pClient — whichever of the two are non-nil — and
// records each result independently via store. Each attempt (gRPC, P2P)
// is wrapped in its own error handling: one failing does not skip or
// abort the other. It is exported so callers outside the collector's own
// scheduled loop (e.g. the API's async health-check kickoff for a freshly
// submitted node) can trigger the same check-and-record logic without
// waiting for the next scheduled poll pass.
//
// The returned error, if any, is a joined combination of errors from
// recording the health checks (not from the probes themselves — a probe
// error is expected/normal for an unreachable node and is itself recorded
// as a Reachable: false row, not surfaced as an error here); it exists
// purely so call sites can log a combined failure, not for control flow.
func PollOnce(ctx context.Context, grpcClient, p2pClient NodeClient, store storage.Store, node storage.Node) error {
	var errs []error

	if grpcClient != nil {
		if err := pollOnceWithSource(ctx, grpcClient, store, node, storage.ProbeSourceGRPC); err != nil {
			errs = append(errs, fmt.Errorf("grpc probe %s: %w", node.Address, err))
		}
	}
	if p2pClient != nil {
		if err := pollOnceWithSource(ctx, p2pClient, store, node, storage.ProbeSourceP2P); err != nil {
			errs = append(errs, fmt.Errorf("p2p probe %s: %w", node.Address, err))
		}
	}

	return errors.Join(errs...)
}

// pollOnceWithSource performs a single health check of node via client and
// records the result via store, tagged with probeSource. If client.GetInfo
// itself fails (the node is unreachable over this transport — a normal,
// expected case, not a bug), an unreachable row is still recorded so
// history reflects the failed check rather than silently having no data
// point; the GetInfo error itself is not returned in that case, only any
// error from the RecordHealthCheck call.
//
// If GetInfo succeeds and yields a confirmed PublicKey, this is a real,
// direct probe of node.Address — exactly the case UpsertConfirmedNode
// exists for — so it is called before recording the health check, and the
// (possibly different, if this triggered a merge) surviving node's ID is
// used for the recorded HealthCheckInput.NodeID rather than the original
// node.ID: if node.Address was a placeholder that just got merged into an
// already-confirmed node under a different id, the health check must be
// recorded against the surviving node, not the now-deleted placeholder.
// If info.PublicKey is nil/empty (GetInfo succeeded but didn't yield a
// pubkey), the health check falls back to node.ID as before.
func pollOnceWithSource(ctx context.Context, client NodeClient, store storage.Store, node storage.Node, probeSource storage.ProbeSource) error {
	info, err := client.GetInfo(ctx, node.Address)
	if err != nil {
		return store.RecordHealthCheck(ctx, storage.HealthCheckInput{
			NodeID:      node.ID,
			Reachable:   false,
			ProbeSource: probeSource,
		})
	}

	nodeID := node.ID
	if len(info.PublicKey) > 0 {
		confirmed, err := store.UpsertConfirmedNode(ctx, node.Address, info.PublicKey, storage.DiscoverySourceP2P)
		if err != nil {
			log.Printf("collector: upsert confirmed node %s: %v", node.Address, err)
		} else {
			nodeID = confirmed.ID
		}
	}

	return store.RecordHealthCheck(ctx, storage.HealthCheckInput{
		NodeID:                nodeID,
		Reachable:             info.Reachable,
		ProbeSource:           probeSource,
		Height:                info.Height,
		ChainTipHeight:        info.ChainTipHeight,
		Version:               info.Version,
		LatencyMS:             info.LatencyMS,
		RxtHashrate:           info.RxtHashrate,
		C29Hashrate:           info.C29Hashrate,
		Sha3xHashrate:         info.Sha3xHashrate,
		PeerIdentityUpdatedAt: info.PeerIdentityUpdatedAt,
	})
}

// pollInterval returns the poll cadence for n based on whether it is an
// unconfirmed placeholder node or tagged pool-owned.
func (c *Collector) pollInterval(n storage.Node) time.Duration {
	if n.PublicKey == nil {
		return PollIntervalUnconfirmed
	}
	if isPoolOwned(n) {
		return PollIntervalPoolOwned
	}
	return PollIntervalGeneric
}

// discoveryInterval returns the discovery-walk cooldown for n based on
// whether it is tagged pool-owned, mirroring pollInterval.
func (c *Collector) discoveryInterval(n storage.Node) time.Duration {
	if isPoolOwned(n) {
		return DiscoveryIntervalPoolOwned
	}
	return DiscoveryIntervalGeneric
}

// dialJitter returns the effective per-dial delay to use before a real
// network dial. It is simply Config.DialJitter with no implicit default
// substitution — see Config.DialJitter's doc comment for why: this keeps
// the collector package's behavior fully test-controllable (zero by
// default), with any nonzero default (e.g. 500ms) being a production
// wiring decision made by the caller (see cmd/netmap/main.go), not a
// collector-package behavior.
func (c *Collector) dialJitter() time.Duration {
	return c.cfg.DialJitter
}

// isPoolOwned reports whether n's tags mark it as pool-owned.
func isPoolOwned(n storage.Node) bool {
	v, ok := n.Tags["pool_owned"]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func (c *Collector) due(addr string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	next, ok := c.nextPoll[addr]
	if !ok {
		return true
	}
	return !now.Before(next)
}

func (c *Collector) setNextPoll(addr string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextPoll[addr] = t
}

// dueForDiscovery reports whether the cooldown for the given
// discoveryCooldownKey has elapsed (or it has never been discovery-walked
// before), mirroring due.
func (c *Collector) dueForDiscovery(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	next, ok := c.nextDiscovery[key]
	if !ok {
		return true
	}
	return !now.Before(next)
}

// setNextDiscovery records the next discovery-walk due time for the given
// discoveryCooldownKey, mirroring setNextPoll. It is guarded by the same
// c.mu as nextPoll — both maps belong to the same Collector and neither
// Discover nor Poll needs to hold the lock for long, so a second mutex
// would add no real benefit.
func (c *Collector) setNextDiscovery(key string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextDiscovery[key] = t
}
