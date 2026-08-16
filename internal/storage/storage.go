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

	// UpsertPeerEdge records a directed peer-topology edge, deduplicated by
	// (from, to) pair. last_seen is bumped on every call.
	UpsertPeerEdge(ctx context.Context, fromNodeID, toNodeID uuid.UUID) error

	// ListTopology returns all nodes and edges for the graph view.
	ListTopology(ctx context.Context) ([]Node, []PeerEdge, error)
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
	_, err := s.pool.Exec(ctx, `
		INSERT INTO node_health (
			node_id, ts, reachable, height, chain_tip_height, version, latency_ms,
			rxt_hashrate, c29_hashrate, sha3x_hashrate
		) VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9)
	`, in.NodeID, in.Reachable, in.Height, in.ChainTipHeight, in.Version, in.LatencyMS,
		in.RxtHashrate, in.C29Hashrate, in.Sha3xHashrate,
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
		SELECT id, node_id, ts, reachable, height, chain_tip_height, version, latency_ms,
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
		if err := rows.Scan(&h.ID, &h.NodeID, &h.Timestamp, &h.Reachable, &h.Height, &h.ChainTipHeight,
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

func (s *pgStore) UpsertPeerEdge(ctx context.Context, fromNodeID, toNodeID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO peer_edges (from_node_id, to_node_id, first_seen, last_seen)
		VALUES ($1, $2, now(), now())
		ON CONFLICT (from_node_id, to_node_id) DO UPDATE SET last_seen = now()
	`, fromNodeID, toNodeID)
	if err != nil {
		return fmt.Errorf("storage: upsert peer edge: %w", err)
	}
	return nil
}

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
