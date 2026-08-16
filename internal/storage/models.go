package storage

import (
	"time"

	"github.com/google/uuid"
)

// DiscoverySource identifies how a node entered the topology: discovered by
// walking the peer graph, submitted via the public registry API, or both
// (a node originally seen one way that was later also seen the other way).
type DiscoverySource string

const (
	DiscoverySourceP2P      DiscoverySource = "p2p_discovered"
	DiscoverySourceRegistry DiscoverySource = "registry_submitted"
	DiscoverySourceBoth     DiscoverySource = "both"
)

// Node is a Tari node known to netmap.
type Node struct {
	ID              uuid.UUID       `json:"id"`
	Address         string          `json:"address"`
	DiscoverySource DiscoverySource `json:"discovery_source"`
	Tags            map[string]any  `json:"tags"`
	Label           *string         `json:"label,omitempty"`
	FirstSeen       time.Time       `json:"first_seen"`
	LastSeen        time.Time       `json:"last_seen"`
}

// NodeInput is the input to UpsertNode.
type NodeInput struct {
	Address         string
	DiscoverySource DiscoverySource
	Tags            map[string]any
	Label           *string
}

// NodeFilter filters the results of ListNodes. A zero-value NodeFilter
// applies no filtering.
type NodeFilter struct {
	// DiscoverySource, if non-empty, restricts results to nodes with this
	// exact discovery_source value.
	DiscoverySource DiscoverySource
}

// PeerEdge is a directed edge in the observed peer topology graph.
type PeerEdge struct {
	ID         uuid.UUID `json:"id"`
	FromNodeID uuid.UUID `json:"from_node_id"`
	ToNodeID   uuid.UUID `json:"to_node_id"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
}

// HealthCheck is one recorded health-check result for a node.
type HealthCheck struct {
	ID             uuid.UUID `json:"id"`
	NodeID         uuid.UUID `json:"node_id"`
	Timestamp      time.Time `json:"timestamp"`
	Reachable      bool      `json:"reachable"`
	Height         *int64    `json:"height,omitempty"`
	ChainTipHeight *int64    `json:"chain_tip_height,omitempty"`
	Version        *string   `json:"version,omitempty"`
	LatencyMS      *int      `json:"latency_ms,omitempty"`
	RxtHashrate    *float64  `json:"rxt_hashrate,omitempty"`
	C29Hashrate    *float64  `json:"c29_hashrate,omitempty"`
	Sha3xHashrate  *float64  `json:"sha3x_hashrate,omitempty"`
}

// HealthCheckInput is the input to RecordHealthCheck.
type HealthCheckInput struct {
	NodeID         uuid.UUID
	Reachable      bool
	Height         *int64
	ChainTipHeight *int64
	Version        *string
	LatencyMS      *int
	RxtHashrate    *float64
	C29Hashrate    *float64
	Sha3xHashrate  *float64
}
