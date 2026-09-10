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

	"golang.org/x/sync/errgroup"

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

// PollIntervalLikelyDead is the poll interval for unconfirmed placeholder
// nodes (Node.PublicKey == nil) that have failed 3+ consecutive probes
// with zero successes — "likely dead" per the same heuristic as web.go's
// computeLikelyDead (see collectorLikelyDead in this file). Such nodes are
// checked on roughly daily instead of every PollIntervalUnconfirmed,
// since they are overwhelmingly likely to be permanently-gone gossip
// ghosts: Tari's gossip protocol has no upstream expiry mechanism, so a
// peer-walk keeps re-reporting addresses of nodes that will never come
// back. This is a backoff, not a permanent skip — a genuinely revived
// node is still polled, just less often, so it will eventually be
// rediscovered as reachable.
const PollIntervalLikelyDead = 24 * time.Hour

// PollIntervalNeverContacted is the poll cadence pollInterval falls back
// to for an unconfirmed placeholder node that is past all three of the
// NewNodeCheckpoint1/2/3 escalating checkpoints (age >= NewNodeCheckpoint3)
// yet STILL has zero recorded health checks at all — i.e. it has never
// once been through PollOnce, successfully or not (see
// pollOnceWithSource's doc comment: even a failed probe records a row,
// so zero rows really does mean zero attempts). This only happens when
// poll backlog/volume, not node age, is the reason it hasn't been probed
// yet — exactly the scenario the never-contacted poll queue exists to
// fix (see PollNeverContacted's doc comment and
// Collector.NeverContactedTickInterval).
//
// This is set equal to PollIntervalUnconfirmed/NewNodeCheckpoint1 (15min)
// for now — the two constants are intentionally kept separate rather
// than reusing PollIntervalUnconfirmed directly, since what actually
// delivers the "walk the network more aggressively" improvement for
// never-contacted nodes is PollNeverContacted running on its own
// short, independent tick (NeverContactedTickInterval) so a large
// unconfirmed backlog can never delay a never-contacted node's first
// probe attempt — not a shorter per-node cadence once it IS being
// checked. Keeping this as its own named constant leaves room to tune
// it independently later (e.g. if a genuinely different value turns out
// to matter) without touching the unrelated PollIntervalUnconfirmed
// steady-state cadence.
const PollIntervalNeverContacted = NewNodeCheckpoint1

// NewNodeCheckpoint1, NewNodeCheckpoint2, and NewNodeCheckpoint3 are
// AGE-SINCE-storage.Node.FirstSeen thresholds (NOT fixed retry counts, and
// NOT offsets from each other/from "now") that make up an escalating
// fast-checkpoint schedule for brand-new unconfirmed nodes
// (Node.PublicKey == nil): such a node is polled at ~15min, then ~30min,
// then ~60min after it was first seen, so that a real, live node gets a
// few close-together chances to be directly probed (and thus confirmed)
// soon after discovery, without polling so aggressively that it violates
// node-politeness norms.
//
// A node graduates out of this schedule the instant it confirms
// (PublicKey != nil) — see pollInterval's doc comment, this happens by
// construction with no extra code needed. A node that is still
// unconfirmed once all three checkpoints have elapsed (age >=
// NewNodeCheckpoint3) falls back to the existing
// PollIntervalLikelyDead/PollIntervalUnconfirmed logic, unchanged.
const (
	NewNodeCheckpoint1 = 15 * time.Minute
	NewNodeCheckpoint2 = 30 * time.Minute
	NewNodeCheckpoint3 = 60 * time.Minute
)

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

// defaultNeverContactedTickInterval is how often Run checks which
// never-contacted nodes (see PollNeverContacted) are due for their very
// first probe attempt, when Collector.NeverContactedTickInterval is
// unset. It defaults to a shorter cadence than defaultTickInterval (and
// is deliberately NOT derived from it, unlike UnconfirmedTickInterval's
// default) since this queue is explicitly the highest-priority/
// fastest-cadence of the three poll loops — see PollNeverContacted's doc
// comment — and must not inherit whatever (possibly much longer) tick
// the confirmed loop happens to be configured with.
const defaultNeverContactedTickInterval = 1 * time.Minute

// DiscoveryPassDeadline bounds how long a single discoverWith pass (the
// general-population BFS over the known peer graph, started fresh from
// Config.SeedNodes) is allowed to run before it's cut short. Named and
// exported (unlike a plain unexported constant) so the relationship to
// defaultTickInterval below is explicit and can be referenced from
// elsewhere if needed, rather than silently drifting if
// defaultTickInterval is ever changed without this being revisited.
//
// defaultTickInterval (5 minutes) is how often Run kicks off a fresh
// Discover pass; DiscoveryPassDeadline is set comfortably under that —
// 4 minutes — so discoverWith always returns control to its caller
// before the next tick fires, even when the walk can't finish covering
// the whole graph in a single pass. Before this existed, an unbounded
// `for len(queue) > 0` BFS over tens of thousands of nodes could run
// for hours against the real mainnet graph, which meant a node near the
// front of the queue (see discoverWith's per-node cooldown) had to wait
// for the ENTIRE walk to finish before being revisited — starving
// Config.SeedNodes and any pool-owned node of their intended
// DiscoveryIntervalPoolOwned cadence. DiscoverOwned (see its doc
// comment) is the primary fix for owned/seed nodes specifically; this
// deadline is the complementary fix for the general population, so a
// slow/huge graph can never again delay the next Discover tick
// indefinitely, regardless of which nodes happen to be at the front of
// the queue.
const DiscoveryPassDeadline = 4 * time.Minute

// ownedDiscoveryWorkers bounds the number of concurrent in-flight
// GetPeers calls within a single DiscoverOwned pass, mirroring
// maxPollWorkers' errgroup.SetLimit pattern in poll() but sized very
// differently: there are only ever a handful of owned/seed nodes in
// production (unlike the tens of thousands of generic nodes
// maxPollWorkers is sized for), so serializing them behind each other's
// dial timeouts (up to dialTimeout, 180s, each — see
// grpc_client.go/p2p_client.go) is exactly the kind of avoidable
// head-of-line delay DiscoverOwned exists to eliminate. 16 is a
// deliberate, named choice within this repo's suggested 10-20-worker
// range for this loop — comfortably more than the expected owned/seed
// population size, so every owned/seed node's GetPeers call can be
// genuinely in flight at once every pass, with no queueing at all in
// the common case.
const ownedDiscoveryWorkers = 16

// maxPollWorkers bounds the number of concurrent in-flight PollOnce calls
// within a single poll() pass (shared by PollConfirmed, PollUnconfirmed,
// and PollNeverContacted). Each PollOnce dial can take up to dialTimeout
// (see grpc_client.go/p2p_client.go, currently 180s), and with tens of
// thousands of tracked nodes, fully sequential dialing cannot remotely
// keep up with the poll cadences above — see this repo's
// collector-concurrency-brief for the full rationale. Originally raised
// to 100 as a deliberate step up from strictly-sequential (1) per
// Alex's request to "walk the network more aggressively"; raised again
// to 250 per updated guidance from Alex, for the same reason — still a
// fixed, bounded worst case (never one goroutine per due node,
// unbounded), just a bigger deliberate number.
const maxPollWorkers = 250

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
	// single Discover, PollConfirmed, or PollUnconfirmed pass (only
	// immediately before a dial that's actually about to happen — nodes
	// skipped by a due()/dueForDiscovery cooldown check don't incur it),
	// to avoid hammering many different nodes in rapid succession even
	// when overall pass frequency is polite.
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
	// skipped entirely for Discover, PollConfirmed, and PollUnconfirmed,
	// rather than erroring.
	GRPCClient NodeClient

	// P2PClient talks to Tari nodes over go-tari-lib/p2p's direct
	// comms/RPC-over-P2P transport. Optional/nilable: if nil, the P2P
	// probe is skipped entirely for Discover, PollConfirmed, and
	// PollUnconfirmed, rather than erroring. A Collector with only one
	// of GRPCClient/P2PClient set still works correctly, just without
	// data from the other transport.
	P2PClient NodeClient

	// TickInterval governs how often Run checks which known confirmed
	// nodes are due for a poll, and how often Run kicks off a fresh
	// discovery pass. These run on independent tickers/goroutines (see
	// Run) but share this same cadence value for simplicity — there's
	// no need for separate configuration since a discovery pass that's
	// still running when its next tick fires simply doesn't overlap
	// with itself (Run waits for the previous Discover call to return
	// before scheduling off the next tick), and the same is true for
	// PollConfirmed. Defaults to defaultTickInterval if unset. Kept
	// short and independent of the (much longer) per-node poll cadence
	// so tests don't need to wait an hour for anything.
	//
	// The unconfirmed-node poll loop (see UnconfirmedTickInterval) is
	// deliberately NOT governed by this field — it is fully independent
	// so that unconfirmed-node volume/backlog can never affect the
	// confirmed loop's cadence, and vice versa.
	TickInterval time.Duration

	// UnconfirmedTickInterval governs how often Run checks which known
	// unconfirmed placeholder nodes (Node.PublicKey == nil) are due for
	// a poll, mirroring TickInterval but for the separate, explicitly
	// lower-priority unconfirmed poll loop (see runUnconfirmedPollLoop).
	// Optional: defaults to the same effective tick as TickInterval
	// (itself defaulting to defaultTickInterval) when left unset/<= 0,
	// so existing callers that don't care about tuning the two loops'
	// cadences independently see no behavior change. Set this
	// explicitly only if the unconfirmed loop's cadence needs to differ
	// from the confirmed loop's.
	UnconfirmedTickInterval time.Duration

	// NeverContactedTickInterval governs how often Run checks which
	// never-contacted nodes (Node.PublicKey == nil AND zero recorded
	// health checks ever — see PollNeverContacted) are due for their
	// very first probe attempt, mirroring TickInterval/
	// UnconfirmedTickInterval but for the third, explicitly
	// highest-priority poll loop (see runNeverContactedPollLoop).
	// Optional: unlike UnconfirmedTickInterval, this does NOT default
	// to TickInterval's (possibly long) effective value when left
	// unset/<= 0 — it defaults to the shorter
	// defaultNeverContactedTickInterval instead, since a
	// never-contacted node getting its first probe attempt as soon as
	// possible after discovery is the explicit "walk the network more
	// aggressively" goal this queue exists for (see PollNeverContacted's
	// doc comment) — it must not be tied to however long the confirmed
	// loop's cadence happens to be configured.
	NeverContactedTickInterval time.Duration

	// OwnedDiscoveryTickInterval governs how often Run checks
	// Config.SeedNodes and every pool-owned node for a fresh peer-walk,
	// via the independent DiscoverOwned loop (see runOwnedDiscoverLoop)
	// — this is a distinct concept from TickInterval/
	// UnconfirmedTickInterval/NeverContactedTickInterval above (which
	// all govern POLL loops, i.e. health-check dials) and from
	// DiscoveryIntervalPoolOwned (which is the per-node discovery-walk
	// cooldown enforced WITHIN a single DiscoverOwned/Discover pass,
	// not how often Run kicks off a fresh pass). Optional: mirroring
	// NeverContactedTickInterval's optional-field-with-sensible-default
	// pattern, this defaults to DiscoveryIntervalPoolOwned itself (30
	// minutes) when left unset/<= 0 — the natural default tick for a
	// loop whose entire purpose is guaranteeing DiscoveryIntervalPoolOwned's
	// intended cadence for owned/seed nodes actually gets honored (see
	// DiscoverOwned's doc comment for the starvation bug this loop
	// fixes). Set this explicitly only if the owned-discovery loop's
	// tick cadence needs to differ from DiscoveryIntervalPoolOwned
	// itself — e.g. in tests, which use a short interval so they don't
	// need to wait 30 minutes for anything.
	OwnedDiscoveryTickInterval time.Duration

	// OnPollResult is an OPTIONAL observer invoked once per individual probe attempt made by
	// this Collector's own scheduled poll loops (PollConfirmed/PollUnconfirmed/
	// PollNeverContacted, via poll()) -- see PollResultFunc's doc comment for exactly what
	// "success" means. Nil (the zero value, and the default for every existing caller/test)
	// means no observer at all -- poll() behaves identically either way, this is purely an
	// observability hook. Wired from cmd/netmap/main.go to a Prometheus counter (see
	// cmd/netmap/metrics.go); this package itself takes no direct Prometheus dependency.
	// Deliberately NOT invoked for ad-hoc PollOnce calls made outside this Collector's own
	// loops (e.g. internal/api's admin poll-now/submission-approval call sites) -- those
	// call PollOnce directly, not through this Collector, and have no access to this field.
	// Also deliberately NOT invoked from DiscoverOwned/discoverOwnedWith/walkNodePeers --
	// those paths call client.GetPeers (a peer-list walk), never PollOnce/GetInfo, so there
	// is no "poll result" to observe there; OnPollResult's PollResultFunc signature reports a
	// probeSource/success pair that is specifically PollOnce's GetInfo-probe semantics (see
	// pollOnceWithSource), not discovery's.
	OnPollResult PollResultFunc

	mu            sync.Mutex
	nextPoll      map[string]time.Time // address -> next poll due time
	nextDiscovery map[string]time.Time // discoveryCooldownKey(transport, address) -> next discovery-walk due time
}

// New returns a new Collector for the given config. GRPCClient and
// P2PClient are left nil (network calls are opt-in): set Storage
// (required) and at least one of GRPCClient/P2PClient (recommended, but
// not required — a Collector with neither set just does nothing on
// Discover/PollConfirmed/PollUnconfirmed rather than panicking) before
// calling Run, Discover, PollConfirmed, or PollUnconfirmed.
func New(cfg Config) *Collector {
	return &Collector{
		cfg:           cfg,
		nextPoll:      make(map[string]time.Time),
		nextDiscovery: make(map[string]time.Time),
	}
}

// Run starts the collector's discovery and poll loops. Discovery, the
// confirmed-node poll loop, and the unconfirmed-node poll loop each run
// on their own independent goroutine/ticker so that none can starve
// another:
//
//   - a slow or never-ending Discover pass (a synchronous BFS over the
//     real peer graph, with real network dials — this can take minutes
//     against the real mainnet, or longer as the network grows, though
//     now bounded per-pass by DiscoveryPassDeadline — see discoverWith's
//     doc comment) cannot starve either poll loop, which is what
//     actually produces the health-check data the rest of this tool is
//     for.
//   - the unconfirmed-node poll loop (PollUnconfirmed) cannot starve the
//     confirmed-node poll loop (PollConfirmed), even when the unconfirmed
//     node population vastly outnumbers confirmed nodes (in production,
//     ~243:1) — confirmed nodes must be polled reliably every tick
//     regardless of unconfirmed-queue volume/backlog. This is the core
//     fix this split exists for; see PollConfirmed/PollUnconfirmed's doc
//     comments.
//   - the never-contacted poll loop (PollNeverContacted) cannot be
//     starved by, or starve, either of the other two — it runs on its
//     own goroutine/ticker just like the other two, so a never-contacted
//     node gets its first probe attempt on its own fast, independent
//     cadence (see NeverContactedTickInterval) regardless of how large
//     the confirmed or unconfirmed backlogs are. See PollNeverContacted's
//     doc comment for why this queue exists as a THIRD, distinct
//     category rather than folding into PollUnconfirmed.
//   - the owned-discovery loop (DiscoverOwned) cannot be starved by, or
//     starve, any of the other four — it runs on its own
//     goroutine/ticker (see OwnedDiscoveryTickInterval/
//     runOwnedDiscoverLoop), so Config.SeedNodes and every pool-owned
//     node get a fresh peer-walk on their own short, independent
//     cadence regardless of how large the general-population peer
//     graph is or how long the general Discover BFS takes. This is the
//     primary fix for the owned/seed discovery-starvation bug: without
//     it, an owned/seed node — walked once at the very start of
//     discoverWith's BFS — had to wait for the ENTIRE unbounded walk to
//     finish before being revisited, defeating
//     DiscoveryIntervalPoolOwned's intended cadence. See DiscoverOwned's
//     doc comment for the full rationale.
//
// All five goroutines share Storage and the NodeClients, and all
// observe ctx cancellation independently. Run blocks until all five have
// exited (via a sync.WaitGroup) and returns nil on clean shutdown.
//
// Discover() and DiscoverOwned() only ever touch Storage and the
// NodeClients — never c.nextPoll — so running them concurrently with the
// poll loops introduces no new data race. c.nextPoll access is already
// guarded by c.mu, which is generic over address keys and thus safe for
// concurrent due/setNextPoll access from all three poll loops
// simultaneously — PollConfirmed, PollUnconfirmed, and
// PollNeverContacted only ever touch disjoint address keys (a node
// belongs to exactly one of the three categories at any given time —
// see PollNeverContacted's doc comment), but the mutex makes this safe
// even so. c.nextDiscovery is shared between Discover and DiscoverOwned
// (both key it identically via discoveryCooldownKey) and is likewise
// guarded by c.mu — a shared owned/seed address being discovery-walked
// concurrently by both loops is a race on WHICH of the two happens to
// perform that particular dial, never a data race, and either outcome
// is a correct, complete discovery-walk of that address.
func (c *Collector) Run(ctx context.Context) error {
	tick := c.TickInterval
	if tick <= 0 {
		tick = defaultTickInterval
	}

	unconfirmedTick := c.UnconfirmedTickInterval
	if unconfirmedTick <= 0 {
		unconfirmedTick = tick
	}

	neverContactedTick := c.NeverContactedTickInterval
	if neverContactedTick <= 0 {
		neverContactedTick = defaultNeverContactedTickInterval
	}

	ownedDiscoveryTick := c.OwnedDiscoveryTickInterval
	if ownedDiscoveryTick <= 0 {
		ownedDiscoveryTick = DiscoveryIntervalPoolOwned
	}

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()
		c.runDiscoverLoop(ctx, tick)
	}()

	go func() {
		defer wg.Done()
		c.runPollLoop(ctx, tick)
	}()

	go func() {
		defer wg.Done()
		c.runUnconfirmedPollLoop(ctx, unconfirmedTick)
	}()

	go func() {
		defer wg.Done()
		c.runNeverContactedPollLoop(ctx, neverContactedTick)
	}()

	go func() {
		defer wg.Done()
		c.runOwnedDiscoverLoop(ctx, ownedDiscoveryTick)
	}()

	wg.Wait()
	return nil
}

// runDiscoverLoop runs Discover once immediately, then on every tick,
// until ctx is cancelled. It runs entirely independently of runPollLoop
// and runUnconfirmedPollLoop.
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

// runPollLoop runs PollConfirmed once immediately, then on every tick,
// until ctx is cancelled. It runs entirely independently of
// runDiscoverLoop and runUnconfirmedPollLoop, so neither a slow/hanging
// Discover pass nor unconfirmed-node poll volume/backlog ever delays or
// starves polling of confirmed nodes.
func (c *Collector) runPollLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.PollConfirmed(ctx); err != nil {
		log.Printf("collector: confirmed poll pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.PollConfirmed(ctx); err != nil {
				log.Printf("collector: confirmed poll pass error: %v", err)
			}
		}
	}
}

// runUnconfirmedPollLoop runs PollUnconfirmed once immediately, then on
// every tick, until ctx is cancelled. It runs entirely independently of
// runDiscoverLoop and runPollLoop, on its own ticker (see Run's
// unconfirmedTick) — this loop is explicitly lower-priority than
// runPollLoop: it never blocks or starves the confirmed loop in any way
// (no shared per-tick budget, no shared blocking lock held across a
// dial; the two loops' only shared state is c.nextPoll/c.mu, and
// PollConfirmed/PollUnconfirmed only ever touch disjoint address keys).
func (c *Collector) runUnconfirmedPollLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.PollUnconfirmed(ctx); err != nil {
		log.Printf("collector: unconfirmed poll pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.PollUnconfirmed(ctx); err != nil {
				log.Printf("collector: unconfirmed poll pass error: %v", err)
			}
		}
	}
}

// runNeverContactedPollLoop runs PollNeverContacted once immediately,
// then on every tick, until ctx is cancelled. It runs entirely
// independently of runDiscoverLoop, runPollLoop, and
// runUnconfirmedPollLoop, on its own ticker (see Run's
// neverContactedTick) — this loop is explicitly the HIGHEST-priority of
// the three poll loops (see PollNeverContacted's doc comment): it must
// never be blocked or starved by either of the other two, and its own
// (typically much smaller) never-contacted node population must never
// be crowded out by however large the confirmed or unconfirmed backlogs
// are. The three loops' only shared state is c.nextPoll/c.mu, and
// PollConfirmed/PollUnconfirmed/PollNeverContacted only ever touch
// disjoint address keys (a node belongs to exactly one of the three
// categories at any given time).
func (c *Collector) runNeverContactedPollLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.PollNeverContacted(ctx); err != nil {
		log.Printf("collector: never-contacted poll pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.PollNeverContacted(ctx); err != nil {
				log.Printf("collector: never-contacted poll pass error: %v", err)
			}
		}
	}
}

// runOwnedDiscoverLoop runs DiscoverOwned once immediately, then on
// every tick, until ctx is cancelled. It runs entirely independently of
// runDiscoverLoop, runPollLoop, runUnconfirmedPollLoop, and
// runNeverContactedPollLoop, on its own ticker (see Run's
// ownedDiscoveryTick) — this is the loop that actually guarantees
// DiscoveryIntervalPoolOwned's intended cadence for Config.SeedNodes and
// every pool-owned node, regardless of how long the general-population
// discoverWith BFS (run by runDiscoverLoop) takes. See DiscoverOwned's
// doc comment for the full starvation-bug rationale.
func (c *Collector) runOwnedDiscoverLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	if err := c.DiscoverOwned(ctx); err != nil {
		log.Printf("collector: owned discovery pass error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.DiscoverOwned(ctx); err != nil {
				log.Printf("collector: owned discovery pass error: %v", err)
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
//
// The walk is bounded by DiscoveryPassDeadline: ctx is wrapped in its
// own context.WithTimeout derived from the ctx passed in (so an outer
// cancellation still propagates immediately — it is never lost), and
// the loop checks ctx.Done() on every iteration, logging (at INFO-ish
// level, not as an error — running out of pass budget on a
// large/slow-to-respond graph is an expected, normal outcome, not a
// bug) and returning cleanly if the deadline is hit before the queue
// drains. See DiscoveryPassDeadline's doc comment for why this exists:
// without it, an unbounded BFS over a large peer graph could run for
// hours, silently delaying every node still queued — historically
// including owned/seed nodes, which is why they now also get their own
// independent DiscoverOwned loop rather than relying on this deadline
// alone to get revisited promptly.
func (c *Collector) discoverWith(ctx context.Context, client NodeClient, transportLabel string) {
	ctx, cancel := context.WithTimeout(ctx, DiscoveryPassDeadline)
	defer cancel()

	visited := make(map[string]bool)
	queue := append([]string{}, c.cfg.SeedNodes...)
	now := time.Now()

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			log.Printf("collector: [%s] discovery pass cut short by DiscoveryPassDeadline with %d node(s) still queued", transportLabel, len(queue))
			return
		default:
		}

		addr := queue[0]
		queue = queue[1:]
		if visited[addr] {
			continue
		}
		visited[addr] = true

		peers := c.walkNodePeers(ctx, client, transportLabel, addr, now)
		for _, peer := range peers {
			if !visited[peer.Address] {
				queue = append(queue, peer.Address)
			}
		}
	}
}

// walkNodePeers performs the actual discovery-walk work for a single
// addr: it upserts addr into Storage, and — gated by the same per-
// transport discovery cooldown (dueForDiscovery/setNextDiscovery) as
// always — calls client.GetPeers and records every reported peer as a
// discovered node plus a peer-edge observation from addr to it, exactly
// mirroring discoverWith's original inline per-node logic (including
// the doc-comment-documented rule that peer-reported pubkeys are never
// passed to UpsertDiscoveredNode/never trigger UpsertConfirmedNode —
// see discoverWith's doc comment above for the full rationale, which
// this helper does not repeat).
//
// It is shared by discoverWith's multi-hop BFS (which uses the returned
// peers to keep enqueuing/walking outward) and DiscoverOwned's flat,
// single-level fan-out (which deliberately ignores the returned peers —
// see DiscoverOwned's doc comment for why it only cares about addr's
// own peer list being fresh, not further traversal from it). now is
// passed in (rather than called fresh here) so that every node
// processed within the same discoverWith/discoverOwnedWith pass shares
// one consistent timestamp for discoveryInterval's cooldown math, as
// before this extraction.
func (c *Collector) walkNodePeers(ctx context.Context, client NodeClient, transportLabel, addr string, now time.Time) []DiscoveredPeer {
	fromNode, err := c.Storage.UpsertDiscoveredNode(ctx, addr, storage.DiscoverySourceP2P, nil, nil)
	if err != nil {
		log.Printf("collector: [%s] upsert node %s: %v", transportLabel, addr, err)
		return nil
	}

	cooldownKey := discoveryCooldownKey(transportLabel, addr)
	if !c.dueForDiscovery(cooldownKey, now) {
		return nil
	}

	if jitter := c.dialJitter(); jitter > 0 {
		time.Sleep(jitter)
	}

	peers, err := client.GetPeers(ctx, addr)
	if err != nil {
		log.Printf("collector: [%s] get peers for %s: %v", transportLabel, addr, err)
		return nil
	}
	c.setNextDiscovery(cooldownKey, now.Add(c.discoveryInterval(fromNode)))

	for _, peer := range peers {
		// See discoverWith's doc comment: peer.PublicKey is
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
	}

	return peers
}

// discoveryCooldownKey builds the nextDiscovery map key for a given
// transport+address pair. See discoverWith's doc comment for why this is
// scoped per-transport rather than just per-address.
func discoveryCooldownKey(transportLabel, addr string) string {
	return transportLabel + ":" + addr
}

// DiscoverOwned walks ONLY Config.SeedNodes plus every node in Storage
// tagged pool-owned (per isPoolOwned's exact predicate — see
// storage.NodeFilter.Owned's doc comment for how this is expressed as a
// SQL/JSONB clause), fanning out CONCURRENTLY across a small bounded
// worker pool (see ownedDiscoveryWorkers) rather than walking a queue
// one hop at a time like discoverWith's general-population BFS.
//
// This exists to fix a specific starvation bug: discoverWith's BFS
// visits Config.SeedNodes (and thus any pool-owned node reachable from
// them) exactly once, right at the start of its walk, and then — since
// it's a single synchronous, unbounded `for len(queue) > 0` loop over
// the ENTIRE peer graph — cannot revisit them again until the whole
// walk finishes, which in production can take hours (see
// DiscoveryPassDeadline's doc comment for the complementary fix on the
// general-population side). This defeats DiscoveryIntervalPoolOwned's
// intended ~30-minute cadence for exactly the nodes production cares
// most about having fresh topology data for.
//
// Unlike discoverWith, this is deliberately a FLAT, single-level
// fan-out, not a multi-hop queue: DiscoverOwned only cares about
// Config.SeedNodes'/owned nodes' own direct peer lists being fresh, not
// deep graph traversal from them (that's still discoverWith's job, on
// its own independent cadence). Each worker still goes through the same
// walkNodePeers helper discoverWith uses — same upsert/edge-recording
// semantics, same per-transport discovery cooldown
// (dueForDiscovery/setNextDiscovery) gating the actual GetPeers call,
// same peer-reported-pubkeys-never-confirmed rule — just without
// enqueuing the returned peers for further traversal.
//
// GRPCClient and P2PClient (if non-nil) are each walked as a separate,
// independent pass over the same deduped address set, exactly mirroring
// Discover's structure.
func (c *Collector) DiscoverOwned(ctx context.Context) error {
	if c.Storage == nil {
		return errors.New("collector: Storage is not configured")
	}

	if c.GRPCClient != nil {
		if err := c.discoverOwnedWith(ctx, c.GRPCClient, "grpc"); err != nil {
			return err
		}
	}
	if c.P2PClient != nil {
		if err := c.discoverOwnedWith(ctx, c.P2PClient, "p2p"); err != nil {
			return err
		}
	}

	return nil
}

// discoverOwnedWith fans out walkNodePeers calls, bounded to at most
// ownedDiscoveryWorkers concurrent in-flight GetPeers calls via an
// errgroup.Group with SetLimit — mirroring poll()'s existing
// maxPollWorkers pattern exactly, just with a much smaller pool sized
// for a much smaller (owned/seed-only) node set. Every worker always
// returns nil (errors are logged inline by walkNodePeers itself,
// exactly as discoverWith does), so g.Wait()'s return value can never
// itself report an error — it is returned anyway for symmetry with
// ownedAddresses' error return, not because it can be non-nil today.
func (c *Collector) discoverOwnedWith(ctx context.Context, client NodeClient, transportLabel string) error {
	addrs, err := c.ownedAddresses(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	var g errgroup.Group
	g.SetLimit(ownedDiscoveryWorkers)

	for _, addr := range addrs {
		addr := addr
		g.Go(func() error {
			c.walkNodePeers(ctx, client, transportLabel, addr, now)
			return nil
		})
	}
	return g.Wait()
}

// ownedAddresses returns the deduped set of addresses DiscoverOwned
// should walk: every address in Config.SeedNodes, plus the address of
// every node in Storage for which isPoolOwned reports true (queried via
// storage.NodeFilter.Owned, which expresses the identical predicate as
// a SQL/JSONB clause — see its doc comment). Deduping here ensures a
// seed node that is ALSO tagged pool-owned isn't walked twice in the
// same DiscoverOwned pass.
func (c *Collector) ownedAddresses(ctx context.Context) ([]string, error) {
	seen := make(map[string]bool)
	addrs := make([]string, 0, len(c.cfg.SeedNodes))
	for _, addr := range c.cfg.SeedNodes {
		if !seen[addr] {
			seen[addr] = true
			addrs = append(addrs, addr)
		}
	}

	owned := true
	nodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{Owned: &owned})
	if err != nil {
		return nil, fmt.Errorf("collector: list owned nodes: %w", err)
	}
	for _, n := range nodes {
		if !seen[n.Address] {
			seen[n.Address] = true
			addrs = append(addrs, n.Address)
		}
	}

	return addrs, nil
}

// PollConfirmed checks all known confirmed nodes (Node.PublicKey != nil)
// and, for those whose next-poll time is due, calls PollOnce (which
// independently attempts GRPCClient and P2PClient, whichever are
// non-nil). It runs entirely independently of PollUnconfirmed — this is
// the confirmed half of the priority split described in Run's doc
// comment: confirmed nodes must be polled reliably every tick regardless
// of unconfirmed-node population/backlog, since production experience
// showed a single interleaved node list (confirmed and unconfirmed
// nodes sharing one fixed per-tick dial budget) lets unconfirmed
// placeholder nodes — which can outnumber confirmed nodes by orders of
// magnitude — starve confirmed nodes of reliable polling.
func (c *Collector) PollConfirmed(ctx context.Context) error {
	confirmed := true
	return c.poll(ctx, storage.NodeFilter{Confirmed: &confirmed})
}

// PollUnconfirmed checks all known unconfirmed placeholder nodes
// (Node.PublicKey == nil) that have AT LEAST ONE recorded health check
// (i.e. have been probed before, successfully or not) and, for those
// whose next-poll time is due, calls PollOnce, exactly mirroring
// PollConfirmed but over the complementary node subset. It runs
// entirely independently of PollConfirmed — see PollConfirmed's doc
// comment and Run's doc comment for why this split exists and why this
// loop is explicitly lower-priority (it must never block or starve
// PollConfirmed).
//
// The HasHealthChecks: true half of this filter is deliberate and
// distinct from a plain Confirmed: false filter: a node that has NEVER
// had a single health check recorded belongs to PollNeverContacted
// instead (see its doc comment) — without this tightening, such a node
// would be polled by BOTH this loop and PollNeverContacted
// simultaneously, defeating the whole point of prioritizing
// never-contacted nodes ahead of the (typically much larger)
// with-history unconfirmed backlog. See
// TestPollNeverContactedOnlyTouchesNeverContactedNodes and
// TestListNodesHasHealthChecksFilter (internal/storage) for the tests
// proving these two filters are a true partition of the unconfirmed
// population — no double-poll, no gap.
func (c *Collector) PollUnconfirmed(ctx context.Context) error {
	confirmed := false
	hasHistory := true
	return c.poll(ctx, storage.NodeFilter{Confirmed: &confirmed, HasHealthChecks: &hasHistory})
}

// PollNeverContacted checks all known nodes that have NEVER had a single
// health check recorded (zero node_health rows ever — see
// storage.NodeFilter.HasHealthChecks's doc comment for why "zero rows"
// really does mean "never attempted", not just "never succeeded") and,
// for those whose next-poll time is due, calls PollOnce, mirroring
// PollConfirmed/PollUnconfirmed but over this third, disjoint node
// subset.
//
// This is deliberately NOT the same thing as "unconfirmed" (see this
// repo's collector-concurrency-brief): an unconfirmed node can have
// failed-probe history already (the existing PollIntervalLikelyDead
// backoff path handles that, via PollUnconfirmed) — this queue is
// specifically for nodes that have never been dialed at all, typically
// because they were only just discovered and the confirmed/unconfirmed
// poll backlog hasn't reached them yet.
//
// This is the HIGHEST-priority of the three poll loops (see Run's doc
// comment and Collector.NeverContactedTickInterval's shorter default
// tick): a brand-new, zero-history node getting its very first probe
// attempt as soon as possible after discovery is what actually grows
// the confirmed population, per Alex's explicit "walk the network more
// aggressively" request. It runs entirely independently of both
// PollConfirmed and PollUnconfirmed — a large backlog in either of the
// other two must never delay a never-contacted node's first probe.
//
// A node moves OFF this queue and onto PollUnconfirmed (or, if the
// first probe happens to succeed and yield a pubkey, straight to
// PollConfirmed) the instant its first health-check row is written —
// this is a pure query-time distinction (see
// storage.NodeFilter.HasHealthChecks), not a separate stored flag/state
// machine that needs its own bookkeeping, exactly mirroring how the
// confirmed/unconfirmed split itself works.
func (c *Collector) PollNeverContacted(ctx context.Context) error {
	confirmed := false
	hasHistory := false
	return c.poll(ctx, storage.NodeFilter{Confirmed: &confirmed, HasHealthChecks: &hasHistory})
}

// QueueSizes returns the current size of each of the three disjoint poll queues
// PollConfirmed/PollUnconfirmed/PollNeverContacted operate over (see their doc comments), for
// gauge-style observability (see cmd/netmap's Prometheus metrics wiring, which exposes these as
// netmap_<network>_collector_queue_backlog{queue="confirmed"|"unconfirmed"|"never_contacted"}).
// This performs three ListNodes calls, one per filter -- exactly mirroring what
// PollConfirmed/PollUnconfirmed/PollNeverContacted already query, with no new SQL query logic
// of its own. The three results are a true partition of the whole node population: every node
// belongs to exactly one of the three (see PollNeverContacted's doc comment), so
// confirmed+unconfirmed+neverContacted always equals the total node count.
func (c *Collector) QueueSizes(ctx context.Context) (confirmed, unconfirmed, neverContacted int, err error) {
	if c.Storage == nil {
		return 0, 0, 0, errors.New("collector: Storage is not configured")
	}

	isConfirmed := true
	confirmedNodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{Confirmed: &isConfirmed})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("collector: list confirmed nodes: %w", err)
	}

	isUnconfirmed := false
	hasHistory := true
	unconfirmedNodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{Confirmed: &isUnconfirmed, HasHealthChecks: &hasHistory})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("collector: list unconfirmed nodes: %w", err)
	}

	noHistory := false
	neverContactedNodes, err := c.Storage.ListNodes(ctx, storage.NodeFilter{Confirmed: &isUnconfirmed, HasHealthChecks: &noHistory})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("collector: list never-contacted nodes: %w", err)
	}

	return len(confirmedNodes), len(unconfirmedNodes), len(neverContactedNodes), nil
}

// poll checks all nodes matching filter and, for those whose next-poll
// time is due, calls PollOnce. Shared by PollConfirmed, PollUnconfirmed,
// and PollNeverContacted — all three call the same due/setNextPoll/
// PollOnce/jitter logic, just over a filtered node set each; per-node
// poll-interval selection (pollInterval) is completely unaffected by
// this split, since it already branches on n.PublicKey (and, for the
// never-contacted case, on empty history) itself.
//
// The due()/setNextPoll() bookkeeping for every node runs sequentially,
// single-threaded, in this same top-level loop, BEFORE any concurrent
// dialing is dispatched below — so it needs no additional
// synchronization beyond c.mu's existing per-key locking (see due/
// setNextPoll), and each node's next-poll time is still set exactly
// once per pass with no race, regardless of maxPollWorkers. Only the
// actual network dial (PollOnce, the expensive/slow part) runs
// concurrently, bounded to at most maxPollWorkers (250) simultaneous
// in-flight calls via an errgroup.Group with SetLimit — see this
// repo's collector-concurrency-brief for why strictly-sequential
// dialing could not keep up with tens of thousands of tracked nodes.
// dialJitter is applied per-worker, immediately before that worker's
// own dial, rather than once for the whole batch — a single
// batch-wide sleep would serialize away the entire concurrency benefit
// of the worker pool. PollOnce errors are logged, never returned/
// aborted — one node's dial failing must never affect any other node's
// dial in the same pass. g.Wait()'s return value is intentionally
// discarded: every worker func always returns nil (errors are logged
// inline instead), so it can never itself report an error.
func (c *Collector) poll(ctx context.Context, filter storage.NodeFilter) error {
	if c.Storage == nil {
		return errors.New("collector: Storage is not configured")
	}

	nodes, err := c.Storage.ListNodes(ctx, filter)
	if err != nil {
		return fmt.Errorf("collector: list nodes: %w", err)
	}

	now := time.Now()
	var g errgroup.Group
	g.SetLimit(maxPollWorkers)

	for _, n := range nodes {
		if !c.due(n.Address, now) {
			continue
		}
		c.setNextPoll(n.Address, now.Add(c.pollInterval(ctx, n)))

		n := n
		g.Go(func() error {
			if jitter := c.dialJitter(); jitter > 0 {
				time.Sleep(jitter)
			}
			if err := PollOnce(ctx, c.GRPCClient, c.P2PClient, c.Storage, n, c.OnPollResult); err != nil {
				log.Printf("collector: poll %s: %v", n.Address, err)
			}
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

// PollResultFunc is an optional observer of each individual probe attempt PollOnce makes,
// invoked once per configured transport (grpc/p2p) with probeSource and whether that probe
// transport itself succeeded (client.GetInfo returned no error -- i.e. the node responded at
// all over that transport, independent of any subsequent storage.Store recording error). This
// lets a caller (see Collector.OnPollResult, wired from cmd/netmap/main.go's Prometheus metrics)
// count poll outcomes per probe source without this package taking a direct Prometheus
// dependency of its own -- collector stays free of any specific metrics backend.
type PollResultFunc func(probeSource storage.ProbeSource, success bool)

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
//
// onResult is an OPTIONAL trailing PollResultFunc (variadic purely so existing callers, e.g.
// internal/api's admin poll-now/submission-approval call sites, keep compiling unchanged with
// no observer at all): if provided (onResult[0] != nil), it is invoked once per attempted
// transport with that transport's probeSource and success (client.GetInfo err == nil). Only
// the first element is ever consulted; passing more than one is meaningless and never done by
// this package's own call sites.
func PollOnce(ctx context.Context, grpcClient, p2pClient NodeClient, store storage.Store, node storage.Node, onResult ...PollResultFunc) error {
	var observe PollResultFunc
	if len(onResult) > 0 {
		observe = onResult[0]
	}

	var errs []error

	if grpcClient != nil {
		success, err := pollOnceWithSource(ctx, grpcClient, store, node, storage.ProbeSourceGRPC)
		if observe != nil {
			observe(storage.ProbeSourceGRPC, success)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("grpc probe %s: %w", node.Address, err))
		}
	}
	if p2pClient != nil {
		success, err := pollOnceWithSource(ctx, p2pClient, store, node, storage.ProbeSourceP2P)
		if observe != nil {
			observe(storage.ProbeSourceP2P, success)
		}
		if err != nil {
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
//
// The returned bool is whether client.GetInfo itself succeeded (the probe transport got a
// response at all) — this is PollOnce's PollResultFunc "success" signal, deliberately distinct
// from any subsequent RecordHealthCheck/UpsertConfirmedNode storage error (returned separately,
// as error) and from info.Reachable (which, per both grpc_client.go's and p2p_client.go's own
// GetInfo implementations, is always true whenever GetInfo itself returns a nil error — there
// is no real-world case where GetInfo succeeds with Reachable: false today, but this return
// value tracks the GetInfo err itself rather than assuming that equivalence holds forever).
func pollOnceWithSource(ctx context.Context, client NodeClient, store storage.Store, node storage.Node, probeSource storage.ProbeSource) (bool, error) {
	info, err := client.GetInfo(ctx, node.Address)
	if err != nil {
		return false, store.RecordHealthCheck(ctx, storage.HealthCheckInput{
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

	return true, store.RecordHealthCheck(ctx, storage.HealthCheckInput{
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

// collectorLikelyDead mirrors web.go's computeLikelyDead (see
// internal/web/web.go) -- same "3+ probes, zero successes" heuristic --
// duplicated here rather than imported because internal/collector must
// not import internal/web (wrong direction in the package dependency
// graph: web depends on collector's types/behavior, not vice versa).
// Keep this in sync with computeLikelyDead if that heuristic ever
// changes.
func collectorLikelyDead(history []storage.HealthCheck) bool {
	if len(history) < 3 {
		return false
	}
	for _, h := range history {
		if h.Reachable {
			return false
		}
	}
	return true
}

// pollInterval returns the poll cadence for n based on whether it is an
// unconfirmed placeholder node or tagged pool-owned.
//
// Unconfirmed placeholder nodes (n.PublicKey == nil) go through an
// escalating age-based checkpoint schedule BEFORE the likely-dead check
// is ever consulted:
//
//   - age < NewNodeCheckpoint1 (15min since n.FirstSeen): next poll lands
//     at the 15min mark.
//   - age < NewNodeCheckpoint2 (30min): next poll lands at the 30min mark.
//   - age < NewNodeCheckpoint3 (60min): next poll lands at the 60min mark.
//   - age >= NewNodeCheckpoint3: falls through to the existing
//     collectorLikelyDead-gated logic below.
//
// IMPORTANT, DELIBERATE PRECEDENCE RULE: the age-based checkpoint
// schedule is checked FIRST and takes priority over collectorLikelyDead
// for a node's first hour, even if that node already has 3+ failed probes
// in its history. That is, a node younger than NewNodeCheckpoint3 never
// gets PollIntervalLikelyDead, no matter how many times it has failed —
// it stays on the checkpoint schedule until the full hour has elapsed.
// This is intentional (confirmed with stakeholders), not an oversight: a
// brand-new node deserves its full run of close-together checkpoint
// probes before being written off as likely-dead. collectorLikelyDead is
// only ever consulted once age >= NewNodeCheckpoint3, i.e. once all three
// checkpoints have passed while the node is still unconfirmed.
//
// Once past all three checkpoints, an unconfirmed node is checked against
// collectorLikelyDead exactly as before: 3+ consecutive failed probes
// with zero successes backs it off to PollIntervalLikelyDead instead of
// the normal PollIntervalUnconfirmed cadence (see PollIntervalLikelyDead's
// doc comment for the rationale). A node with ZERO history at all (never
// once probed — see PollIntervalNeverContacted's doc comment) gets
// PollIntervalNeverContacted instead — collectorLikelyDead can't
// meaningfully apply to it (it needs 3+ entries), and this is exactly the
// never-contacted case PollNeverContacted's own faster tick cadence
// exists to get ahead of. On a GetNodeHistory error, this falls back to
// PollIntervalUnconfirmed (fail safe: a transient DB error must not
// over-penalize a node's poll cadence) and logs the error.
//
// Confirmed nodes (n.PublicKey != nil) are completely unaffected by any of
// the above: neither the checkpoint schedule nor GetNodeHistory/
// collectorLikelyDead is ever consulted for them — a node graduates out
// of both the instant it confirms, by construction, since that flips it
// onto this else branch on the very next pollInterval call. Their cadence
// is exactly isPoolOwned's PollIntervalPoolOwned / PollIntervalGeneric
// choice, as before.
func (c *Collector) pollInterval(ctx context.Context, n storage.Node) time.Duration {
	if n.PublicKey == nil {
		age := time.Since(n.FirstSeen)
		switch {
		case age < NewNodeCheckpoint1:
			return NewNodeCheckpoint1 - age
		case age < NewNodeCheckpoint2:
			return NewNodeCheckpoint2 - age
		case age < NewNodeCheckpoint3:
			return NewNodeCheckpoint3 - age
		}

		history, err := c.Storage.GetNodeHistory(ctx, n.ID, 3)
		if err != nil {
			log.Printf("collector: get node history for %s: %v", n.Address, err)
			return PollIntervalUnconfirmed
		}
		if len(history) == 0 {
			// Past all three checkpoints (age-based) but still zero
			// health-check rows at all: this is the never-contacted
			// case (see PollIntervalNeverContacted's doc comment) --
			// poll backlog, not node age, is why it hasn't been probed
			// yet. collectorLikelyDead needs 3+ history entries to
			// conclude anything, so it's not even reachable here; skip
			// straight to the dedicated constant instead of falling
			// through to the (numerically identical, for now)
			// PollIntervalUnconfirmed default below.
			return PollIntervalNeverContacted
		}
		if collectorLikelyDead(history) {
			return PollIntervalLikelyDead
		}
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

// isPoolOwned reports whether n should get the fast pool-owned poll/
// discovery cadence. This is true when EITHER of two independent signals
// is present:
//
//   - n.Tags["pool_owned"] == true — an explicit boolean flag some other
//     code path may still set directly.
//   - n.Tags["owner"] is a non-empty string — the real-world tagging path
//     that actually fires today: when an admin approves a submitted node
//     with an OwnerTag (see internal/api/api.go's submission-approval
//     handler), only tags["owner"] is written, never tags["pool_owned"].
//     Per Alex: the mere presence of a non-empty owner tag (i.e. someone
//     registered/claimed this IP) is itself the fast-poll signal, not a
//     separate boolean that path never set. Confirmed live: production
//     has 24 confirmed nodes with tags = {"owner": "Jagtech"} and zero
//     nodes anywhere with pool_owned: true set, meaning every one of
//     those owner-tagged nodes was incorrectly falling through to
//     PollIntervalGeneric's 2-hour cadence before this fix.
//
// An empty-string owner tag does NOT count as "someone registered/
// claimed this IP" and so does not trigger fast cadence.
func isPoolOwned(n storage.Node) bool {
	if v, ok := n.Tags["pool_owned"]; ok {
		if b, ok := v.(bool); ok && b {
			return true
		}
	}
	if v, ok := n.Tags["owner"]; ok {
		if s, ok := v.(string); ok && s != "" {
			return true
		}
	}
	return false
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
// c.mu as nextPoll — both maps belong to the same Collector and none of
// Discover, PollConfirmed, or PollUnconfirmed need to hold the lock for
// long, so a second mutex would add no real benefit.
func (c *Collector) setNextDiscovery(key string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextDiscovery[key] = t
}
