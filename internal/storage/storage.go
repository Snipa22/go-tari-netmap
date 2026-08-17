// Package storage provides persistence for topology and node-health data.
// The backend is Postgres, with an optional TimescaleDB hypertable for the
// node_health time-series table (see internal/storage/migrations for why
// that step is best-effort rather than required).
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN is used when NETMAP_DATABASE_URL is unset. It intentionally
// points at a local, non-production instance.
const DefaultDSN = "postgres://localhost:5432/netmap"

// DSNFromEnv returns the configured database DSN from NETMAP_DATABASE_URL,
// falling back to DefaultDSN if unset.
func DSNFromEnv() string {
	if dsn := os.Getenv("NETMAP_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return DefaultDSN
}

// Store is the persistence interface for topology and node-health data.
type Store interface {
	// Ping checks connectivity to the underlying storage backend.
	Ping(ctx context.Context) error

	// Close releases any resources held by the Store.
	Close() error

	// Migrate applies pending schema migrations. It is safe to call
	// repeatedly (idempotent) and safe to call concurrently with normal
	// use of the Store.
	Migrate(ctx context.Context) error

	// UpsertNode inserts a new node or updates an existing one (matched by
	// address). first_seen is set once on insert; last_seen is bumped on
	// every call. If the node already exists with a different
	// discovery_source than the one supplied, the stored source is merged
	// to DiscoverySourceBoth.
	UpsertNode(ctx context.Context, in NodeInput) (Node, error)

	// ListNodes returns nodes matching filter. A zero-value filter returns
	// all nodes.
	ListNodes(ctx context.Context, filter NodeFilter) ([]Node, error)

	// GetNode returns a single node by ID. Returns ErrNotFound if no such
	// node exists.
	GetNode(ctx context.Context, id uuid.UUID) (Node, error)

	// RecordHealthCheck inserts one health-check result row.
	RecordHealthCheck(ctx context.Context, in HealthCheckInput) error

	// GetNodeHistory returns the most recent limit health checks for a
	// node, newest first.
	GetNodeHistory(ctx context.Context, nodeID uuid.UUID, limit int) ([]HealthCheck, error)

	// RecordPeerEdgeObservation records a single directed peer-topology
	// edge observation. Unlike the old UpsertPeerEdge, this is a plain
	// append-only INSERT — no ON CONFLICT, no dedup, no updating of an
	// existing row. Every call creates a new peer_edge_observations row,
	// so repeated discovery-walk observations of the same (from, to) pair
	// accumulate as real history rather than overwriting a single
	// last_seen timestamp.
	RecordPeerEdgeObservation(ctx context.Context, fromNodeID, toNodeID uuid.UUID) error

	// ListTopology returns all nodes and edges for the graph view.
	ListTopology(ctx context.Context) ([]Node, []PeerEdge, error)

	// TopPeeredNodes returns the nodes with the most distinct peers,
	// counting only peer-edge observations at or after since. A node's
	// Degree is the count of distinct other nodes it has an edge to OR
	// from (edges are directed, but "how many connections does this node
	// have" is treated as undirected for this ranking). Results are
	// ordered by Degree descending and capped at limit.
	TopPeeredNodes(ctx context.Context, since time.Time, limit int) ([]NodeDegree, error)
}

// ErrNotFound is returned by GetNode when no node with the given ID exists.
var ErrNotFound = fmt.Errorf("storage: not found")

// pgStore is the Postgres-backed Store implementation.
type pgStore struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by a Postgres connection pool for dsn. It does
// not run migrations; call Migrate explicitly once the Store is
// constructed.
func New(ctx context.Context, dsn string) (Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: create pool: %w", err)
	}

	s := &pgStore{pool: pool}
	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}
	return s, nil
}

// Ping checks connectivity to Postgres.
func (s *pgStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the connection pool.
func (s *pgStore) Close() error {
	s.pool.Close()
	return nil
}

// Migrate applies pending schema migrations.
func (s *pgStore) Migrate(ctx context.Context) error {
	return migrate(ctx, s.pool)
}

func (s *pgStore) UpsertNode(ctx context.Context, in NodeInput) (Node, error) {
	if in.Address == "" {
		return Node{}, fmt.Errorf("storage: address is required")
	}
	if in.DiscoverySource == "" {
		return Node{}, fmt.Errorf("storage: discovery_source is required")
	}

	tags := in.Tags
	if tags == nil {
		tags = map[string]any{}
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return Node{}, fmt.Errorf("storage: marshal tags: %w", err)
	}

	var n Node
	var tagsOut []byte
	err = s.pool.QueryRow(ctx, `
		INSERT INTO nodes (address, discovery_source, tags, label, first_seen, last_seen)
		VALUES ($1, $2, $3, $4, now(), now())
		ON CONFLICT (address) DO UPDATE SET
			discovery_source = CASE
				WHEN nodes.discovery_source = EXCLUDED.discovery_source THEN nodes.discovery_source
				ELSE 'both'
			END,
			tags = nodes.tags || EXCLUDED.tags,
			label = COALESCE(EXCLUDED.label, nodes.label),
			last_seen = now()
		RETURNING id, address, discovery_source, tags, label, first_seen, last_seen
	`, in.Address, string(in.DiscoverySource), tagsJSON, in.Label,
	).Scan(&n.ID, &n.Address, &n.DiscoverySource, &tagsOut, &n.Label, &n.FirstSeen, &n.LastSeen)
	if err != nil {
		return Node{}, fmt.Errorf("storage: upsert node: %w", err)
	}
	if err := json.Unmarshal(tagsOut, &n.Tags); err != nil {
		return Node{}, fmt.Errorf("storage: unmarshal tags: %w", err)
	}
	return n, nil
}

func (s *pgStore) ListNodes(ctx context.Context, filter NodeFilter) ([]Node, error) {
	var rows pgx.Rows
	var err error
	if filter.DiscoverySource != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, address, discovery_source, tags, label, first_seen, last_seen
			FROM nodes
			WHERE discovery_source = $1
			ORDER BY address
		`, string(filter.DiscoverySource))
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, address, discovery_source, tags, label, first_seen, last_seen
			FROM nodes
			ORDER BY address
		`)
	}
	if err != nil {
		return nil, fmt.Errorf("storage: list nodes: %w", err)
	}
	defer rows.Close()

	nodes := []Node{}
	for rows.Next() {
		var n Node
		var tagsOut []byte
		if err := rows.Scan(&n.ID, &n.Address, &n.DiscoverySource, &tagsOut, &n.Label, &n.FirstSeen, &n.LastSeen); err != nil {
			return nil, fmt.Errorf("storage: scan node: %w", err)
		}
		if err := json.Unmarshal(tagsOut, &n.Tags); err != nil {
			return nil, fmt.Errorf("storage: unmarshal tags: %w", err)
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list nodes: %w", err)
	}
	return nodes, nil
}

func (s *pgStore) GetNode(ctx context.Context, id uuid.UUID) (Node, error) {
	var n Node
	var tagsOut []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, address, discovery_source, tags, label, first_seen, last_seen
		FROM nodes
		WHERE id = $1
	`, id).Scan(&n.ID, &n.Address, &n.DiscoverySource, &tagsOut, &n.Label, &n.FirstSeen, &n.LastSeen)
	if err != nil {
		if err == pgx.ErrNoRows {
			return Node{}, ErrNotFound
		}
		return Node{}, fmt.Errorf("storage: get node: %w", err)
	}
	if err := json.Unmarshal(tagsOut, &n.Tags); err != nil {
		return Node{}, fmt.Errorf("storage: unmarshal tags: %w", err)
	}
	return n, nil
}

func (s *pgStore) RecordHealthCheck(ctx context.Context, in HealthCheckInput) error {
	if in.ProbeSource == "" {
		return fmt.Errorf("storage: probe_source is required")
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO node_health (
			node_id, ts, reachable, probe_source, height, chain_tip_height, version,
			latency_ms, rxt_hashrate, c29_hashrate, sha3x_hashrate
		) VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, in.NodeID, in.Reachable, string(in.ProbeSource), in.Height, in.ChainTipHeight, in.Version,
		in.LatencyMS, in.RxtHashrate, in.C29Hashrate, in.Sha3xHashrate,
	)
	if err != nil {
		return fmt.Errorf("storage: record health check: %w", err)
	}
	return nil
}

func (s *pgStore) GetNodeHistory(ctx context.Context, nodeID uuid.UUID, limit int) ([]HealthCheck, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, node_id, ts, reachable, probe_source, height, chain_tip_height, version, latency_ms,
			rxt_hashrate, c29_hashrate, sha3x_hashrate
		FROM node_health
		WHERE node_id = $1
		ORDER BY ts DESC
		LIMIT $2
	`, nodeID, limit)
	if err != nil {
		return nil, fmt.Errorf("storage: get node history: %w", err)
	}
	defer rows.Close()

	checks := []HealthCheck{}
	for rows.Next() {
		var h HealthCheck
		if err := rows.Scan(&h.ID, &h.NodeID, &h.Timestamp, &h.Reachable, &h.ProbeSource, &h.Height, &h.ChainTipHeight,
			&h.Version, &h.LatencyMS, &h.RxtHashrate, &h.C29Hashrate, &h.Sha3xHashrate); err != nil {
			return nil, fmt.Errorf("storage: scan health check: %w", err)
		}
		checks = append(checks, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: get node history: %w", err)
	}
	return checks, nil
}

func (s *pgStore) RecordPeerEdgeObservation(ctx context.Context, fromNodeID, toNodeID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO peer_edge_observations (from_node_id, to_node_id, observed_at)
		VALUES ($1, $2, now())
	`, fromNodeID, toNodeID)
	if err != nil {
		return fmt.Errorf("storage: record peer edge observation: %w", err)
	}
	return nil
}

// ListTopology returns all nodes and edges for the graph view. It queries
// the peer_edges VIEW (a rollup of peer_edge_observations), with no
// time-window filtering: every edge ever observed shows up as "current".
// This is intentional for now — there is no delete/edge-removal detection
// yet, so there is nothing more precise to filter by. Callers wanting a
// time-bounded view of connectivity (e.g. "who's well-connected in the
// last hour") should use TopPeeredNodes instead.
func (s *pgStore) ListTopology(ctx context.Context) ([]Node, []PeerEdge, error) {
	nodes, err := s.ListNodes(ctx, NodeFilter{})
	if err != nil {
		return nil, nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, from_node_id, to_node_id, first_seen, last_seen
		FROM peer_edges
		ORDER BY first_seen
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: list topology edges: %w", err)
	}
	defer rows.Close()

	edges := []PeerEdge{}
	for rows.Next() {
		var e PeerEdge
		if err := rows.Scan(&e.ID, &e.FromNodeID, &e.ToNodeID, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, nil, fmt.Errorf("storage: scan peer edge: %w", err)
		}
		edges = append(edges, e)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("storage: list topology edges: %w", err)
	}
	return nodes, edges, nil
}

// TopPeeredNodes computes, per node, the count of distinct other nodes it
// has an edge to or from (observed at or after since), and returns the
// top `limit` nodes ordered by that count descending. It works by
// unioning (from_node_id, to_node_id) and (to_node_id, from_node_id) pairs
// from peer_edge_observations into a single (node_id, other_node_id)
// relation, then grouping by node_id and counting DISTINCT other_node_id
// — this naturally treats a node's "peer count" as undirected even
// though each individual edge observation is directed. InDegree/OutDegree
// are also computed (distinct predecessors/successors respectively) for
// callers that want the directed breakdown too.
func (s *pgStore) TopPeeredNodes(ctx context.Context, since time.Time, limit int) ([]NodeDegree, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.pool.Query(ctx, `
		WITH undirected AS (
			SELECT from_node_id AS node_id, to_node_id AS other_node_id
			FROM peer_edge_observations
			WHERE observed_at >= $1
			UNION ALL
			SELECT to_node_id AS node_id, from_node_id AS other_node_id
			FROM peer_edge_observations
			WHERE observed_at >= $1
		),
		degrees AS (
			SELECT node_id, count(DISTINCT other_node_id) AS degree
			FROM undirected
			GROUP BY node_id
		),
		out_degrees AS (
			SELECT from_node_id AS node_id, count(DISTINCT to_node_id) AS out_degree
			FROM peer_edge_observations
			WHERE observed_at >= $1
			GROUP BY from_node_id
		),
		in_degrees AS (
			SELECT to_node_id AS node_id, count(DISTINCT from_node_id) AS in_degree
			FROM peer_edge_observations
			WHERE observed_at >= $1
			GROUP BY to_node_id
		)
		SELECT n.id, n.address, d.degree,
			COALESCE(ind.in_degree, 0), COALESCE(outd.out_degree, 0)
		FROM degrees d
		JOIN nodes n ON n.id = d.node_id
		LEFT JOIN in_degrees ind ON ind.node_id = d.node_id
		LEFT JOIN out_degrees outd ON outd.node_id = d.node_id
		ORDER BY d.degree DESC
		LIMIT $2
	`, since, limit)
	if err != nil {
		return nil, fmt.Errorf("storage: top peered nodes: %w", err)
	}
	defer rows.Close()

	out := []NodeDegree{}
	for rows.Next() {
		var nd NodeDegree
		if err := rows.Scan(&nd.NodeID, &nd.Address, &nd.Degree, &nd.InDegree, &nd.OutDegree); err != nil {
			return nil, fmt.Errorf("storage: scan node degree: %w", err)
		}
		out = append(out, nd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: top peered nodes: %w", err)
	}
	return out, nil
}
