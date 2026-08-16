// Package collector will poll Tari nodes and discover peers by walking the
// peer graph starting from a set of seed nodes.
//
// TODO(netmap): implement in the collector/storage/API dispatch. This is
// currently a scaffold-only stub with no real gRPC calls; once
// go-tari-grpc-lib is wired in as a dependency, Collector.Run will use it to
// poll nodes and walk the peer graph.
package collector

import (
	"context"
	"time"
)

// PollIntervalGeneric is the minimum interval between polls of a discovered/generic node.
// Enforced for politeness — polling more aggressively risks looking like abuse to the
// wider Tari network.
const PollIntervalGeneric = time.Hour

// PollIntervalPoolOwned is the poll interval for nodes explicitly tagged as pool-owned.
// TODO(netmap): placeholder value — needs confirmation from the pool-ops team on the
// actual desired cadence before this ships.
const PollIntervalPoolOwned = 5 * time.Minute

// Config holds the collector's configuration.
type Config struct {
	// SeedNodes are the addresses of the seed nodes used to bootstrap peer
	// discovery by walking the Tari peer graph.
	SeedNodes []string
}

// Collector polls Tari nodes and discovers peers.
type Collector struct {
	cfg Config
}

// New returns a new Collector for the given config.
func New(cfg Config) *Collector {
	return &Collector{cfg: cfg}
}

// Run starts the collector's polling loop. It respects ctx cancellation and
// returns nil on a clean shutdown.
//
// TODO(netmap): implement in the collector/storage/API dispatch. This will
// use go-tari-grpc-lib to poll nodes once that dependency is wired in.
func (c *Collector) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return nil
	}
}
