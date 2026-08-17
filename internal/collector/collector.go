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
const PollIntervalGeneric = time.Hour

// PollIntervalPoolOwned is the poll interval for nodes explicitly tagged as pool-owned.
// TODO(netmap): placeholder value — needs confirmation from the pool-ops team on the
// actual desired cadence before this ships.
const PollIntervalPoolOwned = 5 * time.Minute

// defaultTickInterval is how often Run checks which known nodes are due for
// a poll when Collector.TickInterval is unset. It is independent of
// PollIntervalGeneric/PollIntervalPoolOwned, which govern per-node poll
// cadence — this just controls the granularity of that check.
const defaultTickInterval = 30 * time.Second

// NodeInfo is the subset of a Tari base node's health/sync-status info
// needed to record a health check.
type NodeInfo struct {
	Reachable      bool
	Height         *int64
	ChainTipHeight *int64
	Version        *string
	LatencyMS      *int
	RxtHashrate    *float64
	C29Hashrate    *float64
	Sha3xHashrate  *float64
}

// NodeClient is the interface the collector uses to talk to Tari base
// nodes. This models the subset of go-tari-grpc-lib's base-node client
// (ListConnectedPeers/GetPeers plus a health/sync-status call) the
// collector needs.
type NodeClient interface {
	// GetPeers returns the addresses of addr's directly connected peers.
	GetPeers(ctx context.Context, addr string) ([]string, error)

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

func (s *stubClient) GetPeers(ctx context.Context, addr string) ([]string, error) {
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

	// TickInterval governs how often Run checks which known nodes are due
	// for a poll. Defaults to defaultTickInterval if unset. Kept short and
	// independent of the (much longer) per-node poll cadence so tests
	// don't need to wait an hour for anything.
	TickInterval time.Duration

	mu       sync.Mutex
	nextPoll map[string]time.Time // address -> next poll due time
}

// New returns a new Collector for the given config. GRPCClient and
// P2PClient are left nil (network calls are opt-in): set Storage
// (required) and at least one of GRPCClient/P2PClient (recommended, but
// not required — a Collector with neither set just does nothing on
// Discover/Poll rather than panicking) before calling Run, Discover, or
// Poll.
func New(cfg Config) *Collector {
	return &Collector{
		cfg:      cfg,
		nextPoll: make(map[string]time.Time),
	}
}

// Run starts the collector's discovery + poll loop. It respects ctx
// cancellation and returns nil on clean shutdown.
func (c *Collector) Run(ctx context.Context) error {
	tick := c.TickInterval
	if tick <= 0 {
		tick = defaultTickInterval
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	// Run one pass immediately so discovery/polling starts without waiting
	// for the first tick.
	c.runPass(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.runPass(ctx)
		}
	}
}

func (c *Collector) runPass(ctx context.Context) {
	if err := c.Discover(ctx); err != nil {
		log.Printf("collector: discovery pass error: %v", err)
	}
	if err := c.Poll(ctx); err != nil {
		log.Printf("collector: poll pass error: %v", err)
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
// client.GetPeers, deduping visited addresses, and records discovered
// nodes and edges in Storage. transportLabel is used only for log
// messages, to distinguish which transport's walk a given log line came
// from when both are configured.
//
// Discovered nodes/edges are recorded with DiscoverySourceP2P regardless
// of which NodeClient transport (gRPC or P2P-RPC) performed the walk —
// see DiscoverySource's doc comment in models.go: it means "discovered by
// walking the peer graph" broadly, not the P2P-RPC transport specifically.
// This is orthogonal to ProbeSource, which does track which transport a
// given health check came from.
func (c *Collector) discoverWith(ctx context.Context, client NodeClient, transportLabel string) {
	visited := make(map[string]bool)
	queue := append([]string{}, c.cfg.SeedNodes...)

	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]
		if visited[addr] {
			continue
		}
		visited[addr] = true

		fromNode, err := c.Storage.UpsertNode(ctx, storage.NodeInput{
			Address:         addr,
			DiscoverySource: storage.DiscoverySourceP2P,
		})
		if err != nil {
			log.Printf("collector: [%s] upsert node %s: %v", transportLabel, addr, err)
			continue
		}

		peers, err := client.GetPeers(ctx, addr)
		if err != nil {
			log.Printf("collector: [%s] get peers for %s: %v", transportLabel, addr, err)
			continue
		}

		for _, peerAddr := range peers {
			toNode, err := c.Storage.UpsertNode(ctx, storage.NodeInput{
				Address:         peerAddr,
				DiscoverySource: storage.DiscoverySourceP2P,
			})
			if err != nil {
				log.Printf("collector: [%s] upsert node %s: %v", transportLabel, peerAddr, err)
				continue
			}

			if err := c.Storage.UpsertPeerEdge(ctx, fromNode.ID, toNode.ID); err != nil {
				log.Printf("collector: [%s] upsert edge %s -> %s: %v", transportLabel, addr, peerAddr, err)
			}

			if !visited[peerAddr] {
				queue = append(queue, peerAddr)
			}
		}
	}
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
func pollOnceWithSource(ctx context.Context, client NodeClient, store storage.Store, node storage.Node, probeSource storage.ProbeSource) error {
	info, err := client.GetInfo(ctx, node.Address)
	if err != nil {
		return store.RecordHealthCheck(ctx, storage.HealthCheckInput{
			NodeID:      node.ID,
			Reachable:   false,
			ProbeSource: probeSource,
		})
	}

	return store.RecordHealthCheck(ctx, storage.HealthCheckInput{
		NodeID:         node.ID,
		Reachable:      info.Reachable,
		ProbeSource:    probeSource,
		Height:         info.Height,
		ChainTipHeight: info.ChainTipHeight,
		Version:        info.Version,
		LatencyMS:      info.LatencyMS,
		RxtHashrate:    info.RxtHashrate,
		C29Hashrate:    info.C29Hashrate,
		Sha3xHashrate:  info.Sha3xHashrate,
	})
}

// pollInterval returns the poll cadence for n based on whether it is
// tagged pool-owned.
func (c *Collector) pollInterval(n storage.Node) time.Duration {
	if isPoolOwned(n) {
		return PollIntervalPoolOwned
	}
	return PollIntervalGeneric
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
