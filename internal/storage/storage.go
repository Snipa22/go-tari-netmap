// Package storage provides persistence for topology and node-health data.
// The backend is Postgres, with an optional TimescaleDB hypertable for the
// node_health time-series table (see internal/storage/migrations for why
// that step is best-effort rather than required).
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	// UpsertDiscoveredNode records a node discovered by address alone (no
	// confirmed pubkey yet — e.g. a peer-walk GetPeers hop, or a registry
	// submission that hasn't been probed yet). If this address has never
	// been seen before, it creates a placeholder row (public_key NULL).
	// If it has, it bumps last_seen (and merges discovery_source into
	// DiscoverySourceBoth if it differs from the stored value, mirroring
	// the old UpsertNode's merge semantics) on the existing row for that
	// address — whether that row is still a placeholder or has since
	// been confirmed with a real pubkey. Either way, a node_addresses row
	// for address is also upserted (inserted, or its last_seen bumped).
	// tags are merged into any existing tags (like the old UpsertNode);
	// label, if non-nil, overwrites the stored label.
	UpsertDiscoveredNode(ctx context.Context, address string, discoverySource DiscoverySource, tags map[string]any, label *string) (Node, error)

	// UpsertConfirmedNode records a node we've successfully, directly
	// probed and confirmed the real pubkey of, reached at address. See
	// this method's doc comment on pgStore for the full case breakdown
	// (new node, known-pubkey/new-address, placeholder promotion, and the
	// placeholder-merge-into-already-confirmed-node case) — the short
	// version is: the same real node can be discovered at multiple
	// addresses and via multiple placeholder rows over time, and this
	// method is responsible for converging those onto a single node row
	// once its pubkey is confirmed, without losing any of the
	// health-check/peer-edge history already recorded against the
	// placeholder id(s) being retired.
	UpsertConfirmedNode(ctx context.Context, address string, publicKey []byte, discoverySource DiscoverySource) (Node, error)

	// ListNodeAddresses returns every address a node has ever been seen
	// at, oldest first.
	ListNodeAddresses(ctx context.Context, nodeID uuid.UUID) ([]NodeAddress, error)

	// ListNodeAddressesForNodes is the batch form of ListNodeAddresses:
	// it returns every address ever seen for each of nodeIDs in a single
	// query, avoiding an N+1 round trip per node. The returned map always
	// has one entry per input nodeID, even if that node has zero known
	// addresses (an empty, non-nil slice, never a missing key). An empty
	// nodeIDs returns an empty map without erroring.
	ListNodeAddressesForNodes(ctx context.Context, nodeIDs []uuid.UUID) (map[uuid.UUID][]NodeAddress, error)

	// ListNodes returns nodes matching filter. A zero-value filter returns
	// every node, unpaginated — this is the behavior internal callers
	// (e.g. the collector's Poll/Discover loops) depend on and must keep
	// getting forever. filter.Limit/filter.Offset, when set (Limit > 0
	// and/or Offset > 0), apply real SQL LIMIT/OFFSET pagination on top
	// of any DiscoverySource/ReachableSince filtering; this is strictly
	// opt-in — a zero Limit never truncates the result set.
	// filter.ReachableSince, when non-nil, further restricts results to
	// nodes with at least one reachable=true node_health row at or
	// after that time (see NodeFilter's doc comment) — also strictly
	// opt-in, a nil ReachableSince never filters anything out.
	ListNodes(ctx context.Context, filter NodeFilter) ([]Node, error)

	// CountNodes returns the total number of nodes matching filter's
	// DiscoverySource (if set), ignoring filter.Limit/filter.Offset — it
	// always reports the full matching population, for pagination
	// metadata (e.g. "page N of M" / has-more-pages checks) alongside a
	// paginated ListNodes call using the same filter.
	CountNodes(ctx context.Context, filter NodeFilter) (int, error)

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

	// ListTopology returns nodes and edges for the graph view, with no
	// time-window filtering (every edge ever observed is treated as
	// "current" — see the pgStore implementation's doc comment for why).
	// A zero-value TopologyFilter (MaxNodes == 0) returns every node and
	// edge, unbounded. When filter.MaxNodes > 0, the result is capped to
	// the top MaxNodes nodes by total peer-degree (see TopologyFilter's
	// doc comment), and edges are filtered to only those between two
	// nodes both present in that capped set — the returned graph is
	// always self-consistent (no edge ever dangles to a node not also
	// returned).
	ListTopology(ctx context.Context, filter TopologyFilter) ([]Node, []PeerEdge, error)

	// TopPeeredNodes returns the nodes with the most distinct peers,
	// counting only peer-edge observations at or after since. A node's
	// Degree is the count of distinct other nodes it has an edge to OR
	// from (edges are directed, but "how many connections does this node
	// have" is treated as undirected for this ranking). Results are
	// ordered by Degree descending and capped at limit.
	TopPeeredNodes(ctx context.Context, since time.Time, limit int) ([]NodeDegree, error)

	// ListNodeEdges returns every peer_edges row (the peer_edge_observations
	// rollup view) where nodeID is either the from or to side, newest
	// first by last_seen, capped at limit (0 or negative defaults to 50).
	ListNodeEdges(ctx context.Context, nodeID uuid.UUID, limit int) ([]PeerEdge, error)

	// NetworkHeight returns the most common height value across the most
	// recent health check per node (mode of the latest-per-node heights),
	// and the number of nodes that value was derived from. Returns
	// (nil, 0, nil) if no health checks with a non-nil height exist yet.
	NetworkHeight(ctx context.Context) (*int64, int, error)

	// ListSeedCandidates returns nodes that are BOTH opted-in
	// (discovery_source IN ('registry_submitted', 'both')) AND have at
	// least one reachable=true node_health row at or after
	// (now() - since). This is the monero.fail-style "suggested seed
	// node" gate: opt-in alone is not enough, the node must also be
	// currently/recently healthy. A single SQL query (no N+1 loop)
	// joins nodes, a recency-filtered EXISTS against node_health, and
	// node_addresses. Nodes with a NULL public_key are excluded (a
	// peer_seeds line requires a real pubkey; a placeholder node
	// discovered by address alone can't produce one). Returns a
	// non-nil, empty slice (never nil) when nothing qualifies.
	ListSeedCandidates(ctx context.Context, since time.Duration) ([]SeedCandidate, error)

	// IsAddressPubliclyOptedIn reports whether address already belongs
	// to a node whose discovery_source is registry_submitted or both —
	// i.e. an address whose owner has already opted in to public
	// listing. This is the check used both when a new submission is
	// created (to block duplicate opt-in submissions) and again at
	// approval time (in case the address became opted-in in the
	// meantime, between submission and review).
	IsAddressPubliclyOptedIn(ctx context.Context, address string) (bool, error)

	// CreatePendingSubmission records a new public node-submission for
	// review. If a PENDING submission for this exact address already
	// exists, its label/owner_tag are updated in place and its
	// submitted_at is bumped, rather than creating a second row (see
	// this method's implementation for why).
	CreatePendingSubmission(ctx context.Context, address string, label, ownerTag *string) (PendingSubmission, error)

	// ListPendingSubmissions returns submissions with the given exact
	// status. An empty status defaults to "pending" (the common case:
	// the review queue).
	ListPendingSubmissions(ctx context.Context, status string) ([]PendingSubmission, error)

	// GetPendingSubmission returns a single submission by ID. Returns
	// ErrNotFound if no such submission exists.
	GetPendingSubmission(ctx context.Context, id uuid.UUID) (PendingSubmission, error)

	// ApprovePendingSubmission marks submission id as approved, setting
	// reviewed_at and promoted_node_id. Returns an error if the
	// submission is not currently "pending" (a decision, once made,
	// can't be re-made).
	ApprovePendingSubmission(ctx context.Context, id uuid.UUID, promotedNodeID uuid.UUID) error

	// RejectPendingSubmission marks submission id as rejected, setting
	// reviewed_at and rejection_reason (may be nil). Returns an error if
	// the submission is not currently "pending".
	RejectPendingSubmission(ctx context.Context, id uuid.UUID, reason *string) error

	// RecordSubmissionProbeResult records the outcome of a best-effort
	// connectivity probe run against a still-pending submission at
	// creation time. It intentionally only ever touches
	// pending_submissions — never node_health or nodes — since the
	// submission isn't approved.
	RecordSubmissionProbeResult(ctx context.Context, id uuid.UUID, reachable bool) error

	// CountPendingSubmissions returns the number of submissions currently
	// in the 'pending' state, used to enforce a hard cap on unreviewed
	// queue size (abuse-mitigation: prevents unbounded growth from spam).
	CountPendingSubmissions(ctx context.Context) (int, error)
}

// DefaultSeedHealthWindow is the default "currently healthy" recency
// window ListSeedCandidates (and the API endpoints built on it, see
// internal/api's handleListSeedCandidates/handleConfigPeerSeeds) use to
// decide whether an opted-in node counts as a suggested seed node. This
// project's poll cadence for a regular node is PollIntervalGeneric = 2h
// (see internal/collector/collector.go) — a node that's simply due for
// its next scheduled poll, not actually unreachable, could otherwise go
// up to ~2h without a fresh node_health row. 3h gives one full poll
// cycle of slack (survives a single missed/delayed poll) while staying
// tight enough that a node that's actually been down for anywhere close
// to a day is correctly excluded: a real "reachable right now" signal,
// not stale data.
const DefaultSeedHealthWindow = 3 * time.Hour

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

// nodeColumns is the column list, in order, matching scanNode's Scan calls.
const nodeColumns = "id, address, public_key, discovery_source, tags, label, first_seen, last_seen"

// rowQuerier is the subset of *pgxpool.Pool's and pgx.Tx's method sets that
// getNodeByID needs, letting it run against either a plain pool query or a
// query scoped inside an in-flight transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// scanNode scans one nodeColumns-shaped row into a Node.
func scanNode(row pgx.Row) (Node, error) {
	var n Node
	var tagsOut []byte
	if err := row.Scan(&n.ID, &n.Address, &n.PublicKey, &n.DiscoverySource, &tagsOut, &n.Label, &n.FirstSeen, &n.LastSeen); err != nil {
		return Node{}, err
	}
	if err := json.Unmarshal(tagsOut, &n.Tags); err != nil {
		return Node{}, fmt.Errorf("storage: unmarshal tags: %w", err)
	}
	return n, nil
}

// getNodeByID fetches a single node by id via q (a pool or an in-flight
// transaction), returning ErrNotFound if no such node exists.
func getNodeByID(ctx context.Context, q rowQuerier, id uuid.UUID) (Node, error) {
	row := q.QueryRow(ctx, "SELECT "+nodeColumns+" FROM nodes WHERE id = $1", id)
	n, err := scanNode(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Node{}, ErrNotFound
		}
		return Node{}, fmt.Errorf("storage: get node: %w", err)
	}
	return n, nil
}

// ensureNodeAddress upserts a node_addresses row for (nodeID, address),
// bumping last_seen if the row already exists.
func ensureNodeAddress(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, address string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO node_addresses (node_id, address, first_seen, last_seen)
		VALUES ($1, $2, now(), now())
		ON CONFLICT (node_id, address) DO UPDATE SET last_seen = now()
	`, nodeID, address)
	if err != nil {
		return fmt.Errorf("storage: upsert node_addresses: %w", err)
	}
	return nil
}

// bumpNodeSeen bumps a node's last_seen and merges discoverySource into
// its stored discovery_source (to DiscoverySourceBoth if they differ),
// mirroring the old UpsertNode's merge semantics.
func bumpNodeSeen(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, discoverySource DiscoverySource) error {
	_, err := tx.Exec(ctx, `
		UPDATE nodes SET
			discovery_source = CASE WHEN discovery_source = $1 THEN discovery_source ELSE 'both' END,
			last_seen = now()
		WHERE id = $2
	`, string(discoverySource), nodeID)
	if err != nil {
		return fmt.Errorf("storage: bump node last_seen: %w", err)
	}
	return nil
}

// insertConfirmedNode inserts a brand-new node row with a confirmed
// public_key set from the start, plus its node_addresses row, and returns
// the new node's id. Used by UpsertConfirmedNode's case (a) (brand new
// pubkey + brand new address) and its mirror image under the
// address-claimed-by-a-different-node inconsistency handling.
func insertConfirmedNode(ctx context.Context, tx pgx.Tx, address string, publicKey []byte, discoverySource DiscoverySource) (uuid.UUID, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `
		INSERT INTO nodes (address, public_key, discovery_source, tags, label, first_seen, last_seen)
		VALUES ($1, $2, $3, '{}'::jsonb, NULL, now(), now())
		RETURNING id
	`, address, publicKey, string(discoverySource)).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("storage: insert confirmed node: %w", err)
	}
	if err := ensureNodeAddress(ctx, tx, id, address); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// mergeNodeInto retires placeholderID into confirmedID: every
// node_addresses/peer_edge_observations/node_health row referencing
// placeholderID is repointed at confirmedID (deleting any node_addresses
// row that would otherwise collide with confirmedID's own
// UNIQUE(node_id, address) constraint), placeholderID's nodes row is
// deleted, and confirmedID's last_seen is bumped. Called from within an
// existing transaction (tx) — the caller is responsible for
// Commit/Rollback.
func mergeNodeInto(ctx context.Context, tx pgx.Tx, placeholderID, confirmedID uuid.UUID) error {
	// Drop any of the placeholder's node_addresses rows that would
	// collide with an address confirmedID already has, before
	// repointing the rest — UPDATE ... SET node_id = confirmedID would
	// otherwise violate UNIQUE(node_id, address) for those rows.
	if _, err := tx.Exec(ctx, `
		DELETE FROM node_addresses na
		WHERE na.node_id = $1
		  AND EXISTS (SELECT 1 FROM node_addresses na2 WHERE na2.node_id = $2 AND na2.address = na.address)
	`, placeholderID, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: dedupe node_addresses: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE node_addresses SET node_id = $2 WHERE node_id = $1`, placeholderID, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: repoint node_addresses: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE peer_edge_observations SET from_node_id = $2 WHERE from_node_id = $1`, placeholderID, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: repoint peer_edge_observations.from_node_id: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE peer_edge_observations SET to_node_id = $2 WHERE to_node_id = $1`, placeholderID, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: repoint peer_edge_observations.to_node_id: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE node_health SET node_id = $2 WHERE node_id = $1`, placeholderID, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: repoint node_health: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM nodes WHERE id = $1`, placeholderID); err != nil {
		return fmt.Errorf("storage: merge: delete placeholder node: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE nodes SET last_seen = now() WHERE id = $1`, confirmedID); err != nil {
		return fmt.Errorf("storage: merge: bump confirmed last_seen: %w", err)
	}
	return nil
}

func (s *pgStore) UpsertDiscoveredNode(ctx context.Context, address string, discoverySource DiscoverySource, tags map[string]any, label *string) (Node, error) {
	if address == "" {
		return Node{}, fmt.Errorf("storage: address is required")
	}
	if discoverySource == "" {
		return Node{}, fmt.Errorf("storage: discovery_source is required")
	}
	if tags == nil {
		tags = map[string]any{}
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return Node{}, fmt.Errorf("storage: marshal tags: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Node{}, fmt.Errorf("storage: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var nodeID uuid.UUID
	found := true
	if err := tx.QueryRow(ctx, `SELECT node_id FROM node_addresses WHERE address = $1`, address).Scan(&nodeID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return Node{}, fmt.Errorf("storage: lookup node by address: %w", err)
		}
		// Fall back to nodes.address for addresses that predate
		// node_addresses being consistently maintained (e.g. rows only
		// touched by 0006_pubkey_identity.sql's one-time backfill).
		if err := tx.QueryRow(ctx, `SELECT id FROM nodes WHERE address = $1 LIMIT 1`, address).Scan(&nodeID); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return Node{}, fmt.Errorf("storage: lookup node by address: %w", err)
			}
			found = false
		}
	}

	if !found {
		if err := tx.QueryRow(ctx, `
			INSERT INTO nodes (address, discovery_source, tags, label, first_seen, last_seen)
			VALUES ($1, $2, $3, $4, now(), now())
			RETURNING id
		`, address, string(discoverySource), tagsJSON, label).Scan(&nodeID); err != nil {
			return Node{}, fmt.Errorf("storage: insert discovered node: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE nodes SET
				discovery_source = CASE WHEN discovery_source = $1 THEN discovery_source ELSE 'both' END,
				tags = tags || $2,
				label = COALESCE($3, label),
				last_seen = now()
			WHERE id = $4
		`, string(discoverySource), tagsJSON, label, nodeID); err != nil {
			return Node{}, fmt.Errorf("storage: update discovered node: %w", err)
		}
	}

	if err := ensureNodeAddress(ctx, tx, nodeID, address); err != nil {
		return Node{}, err
	}

	n, err := getNodeByID(ctx, tx, nodeID)
	if err != nil {
		return Node{}, fmt.Errorf("storage: get upserted node: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Node{}, fmt.Errorf("storage: commit: %w", err)
	}
	return n, nil
}

// UpsertConfirmedNode records a node we've successfully, directly probed
// and confirmed the real pubkey of, reached at address. Four cases,
// determined by whether a row already exists with public_key = publicKey
// ("foundByPubkey") and/or whether address is already associated with some
// node ("foundByAddr", via node_addresses or, as a fallback, nodes.address
// directly):
//
//   - (a) !foundByPubkey && !foundByAddr: brand new pubkey and brand new
//     address — create a new confirmed node row plus its node_addresses
//     row.
//   - (b) foundByPubkey && (!foundByAddr || same node both ways): known
//     pubkey; address is either new to it or already tied to it — ensure
//     the node_addresses row exists and bump last_seen. The node's id
//     never changes.
//   - (c) !foundByPubkey && foundByAddr, and address's node is a
//     placeholder (public_key IS NULL): promote that row in place —
//     UPDATE nodes SET public_key = ... WHERE id = placeholder.id — so
//     the placeholder's id (and all history recorded against it) is
//     preserved.
//   - (d) foundByPubkey && foundByAddr && the two resolve to different
//     node ids, and address's node is a placeholder: MERGE. The
//     placeholder is retired into the already-confirmed node (see
//     mergeNodeInto) — its node_addresses/peer_edge_observations/
//     node_health rows are repointed at the confirmed node's id and the
//     placeholder's nodes row is deleted. The surviving id is the
//     already-confirmed node's id.
//
// Two further, non-nominal branches handle a real-network inconsistency
// (the same address reported by two different already-confirmed pubkeys —
// e.g. two nodes behind the same NAT/relay at different times) without
// corrupting either existing node: the new pubkey/address pairing is
// attached to (or creates) the node for publicKey, and the other node is
// left untouched. See the case analysis inline below for exactly which
// branch that falls into.
//
// The entire operation runs inside a single transaction so a failure
// partway through the merge case cannot leave a half-repointed state.
func (s *pgStore) UpsertConfirmedNode(ctx context.Context, address string, publicKey []byte, discoverySource DiscoverySource) (Node, error) {
	if address == "" {
		return Node{}, fmt.Errorf("storage: address is required")
	}
	if len(publicKey) == 0 {
		return Node{}, fmt.Errorf("storage: public_key is required")
	}
	if discoverySource == "" {
		return Node{}, fmt.Errorf("storage: discovery_source is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Node{}, fmt.Errorf("storage: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var byPubkeyID uuid.UUID
	foundByPubkey := true
	if err := tx.QueryRow(ctx, `SELECT id FROM nodes WHERE public_key = $1`, publicKey).Scan(&byPubkeyID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return Node{}, fmt.Errorf("storage: lookup node by pubkey: %w", err)
		}
		foundByPubkey = false
	}

	var addrNodeID uuid.UUID
	var addrPubKey []byte
	foundByAddr := true
	if err := tx.QueryRow(ctx, `
		SELECT n.id, n.public_key FROM node_addresses na
		JOIN nodes n ON n.id = na.node_id
		WHERE na.address = $1
		LIMIT 1
	`, address).Scan(&addrNodeID, &addrPubKey); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return Node{}, fmt.Errorf("storage: lookup node by address: %w", err)
		}
		// Fall back to nodes.address, same reasoning as
		// UpsertDiscoveredNode's fallback.
		if err := tx.QueryRow(ctx, `SELECT id, public_key FROM nodes WHERE address = $1 LIMIT 1`, address).Scan(&addrNodeID, &addrPubKey); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return Node{}, fmt.Errorf("storage: lookup node by address: %w", err)
			}
			foundByAddr = false
		}
	}

	var nodeID uuid.UUID

	switch {
	case foundByPubkey && (!foundByAddr || addrNodeID == byPubkeyID):
		// (b): known pubkey; address is new to it, or already tied to
		// it. Node id never changes.
		nodeID = byPubkeyID
		if err := ensureNodeAddress(ctx, tx, nodeID, address); err != nil {
			return Node{}, err
		}
		if err := bumpNodeSeen(ctx, tx, nodeID, discoverySource); err != nil {
			return Node{}, err
		}

	case foundByPubkey && foundByAddr:
		// addrNodeID != byPubkeyID here (the same-id case is handled
		// above).
		if addrPubKey == nil {
			// (d): address's placeholder must be merged into the
			// already-confirmed node.
			if err := mergeNodeInto(ctx, tx, addrNodeID, byPubkeyID); err != nil {
				return Node{}, err
			}
			nodeID = byPubkeyID
		} else {
			// addrPubKey is necessarily != publicKey: the partial
			// unique index on nodes.public_key guarantees no two rows
			// share a non-null pubkey, and we already know
			// addrNodeID != byPubkeyID. This means address is claimed
			// by two different already-confirmed nodes — a real-network
			// inconsistency (e.g. a shared NAT/relay/onion address seen
			// from two distinct nodes at different times), not a bug in
			// this code. Attach it to the publicKey node instead of
			// touching the other, already-confirmed node.
			log.Printf("storage: address %s already belongs to a different confirmed node (pubkey %x); attaching it to pubkey %x instead", address, addrPubKey, publicKey)
			nodeID = byPubkeyID
			if err := ensureNodeAddress(ctx, tx, nodeID, address); err != nil {
				return Node{}, err
			}
			if err := bumpNodeSeen(ctx, tx, nodeID, discoverySource); err != nil {
				return Node{}, err
			}
		}

	case foundByAddr:
		// !foundByPubkey here.
		if addrPubKey == nil {
			// (c): promote the placeholder in place, preserving its id
			// (and thus all history already recorded against it).
			if _, err := tx.Exec(ctx, `
				UPDATE nodes SET
					public_key = $1,
					discovery_source = CASE WHEN discovery_source = $2 THEN discovery_source ELSE 'both' END,
					last_seen = now()
				WHERE id = $3
			`, publicKey, string(discoverySource), addrNodeID); err != nil {
				return Node{}, fmt.Errorf("storage: promote placeholder node: %w", err)
			}
			nodeID = addrNodeID
			if err := ensureNodeAddress(ctx, tx, nodeID, address); err != nil {
				return Node{}, err
			}
		} else {
			// addrPubKey is necessarily != publicKey (no row has
			// publicKey, per !foundByPubkey). address already belongs to
			// a different already-confirmed node — the same
			// inconsistency as above, mirrored: since no node yet exists
			// for publicKey, create a brand new one rather than
			// corrupting the existing node at this address.
			log.Printf("storage: address %s already belongs to a different confirmed node (pubkey %x); creating a separate node for pubkey %x", address, addrPubKey, publicKey)
			newID, err := insertConfirmedNode(ctx, tx, address, publicKey, discoverySource)
			if err != nil {
				return Node{}, err
			}
			nodeID = newID
		}

	default:
		// (a): !foundByPubkey && !foundByAddr — brand new pubkey and
		// brand new address.
		newID, err := insertConfirmedNode(ctx, tx, address, publicKey, discoverySource)
		if err != nil {
			return Node{}, err
		}
		nodeID = newID
	}

	n, err := getNodeByID(ctx, tx, nodeID)
	if err != nil {
		return Node{}, fmt.Errorf("storage: get upserted node: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Node{}, fmt.Errorf("storage: commit: %w", err)
	}
	return n, nil
}

func (s *pgStore) ListNodeAddresses(ctx context.Context, nodeID uuid.UUID) ([]NodeAddress, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, node_id, address, first_seen, last_seen
		FROM node_addresses
		WHERE node_id = $1
		ORDER BY first_seen
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("storage: list node addresses: %w", err)
	}
	defer rows.Close()

	addrs := []NodeAddress{}
	for rows.Next() {
		var a NodeAddress
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Address, &a.FirstSeen, &a.LastSeen); err != nil {
			return nil, fmt.Errorf("storage: scan node address: %w", err)
		}
		addrs = append(addrs, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list node addresses: %w", err)
	}
	return addrs, nil
}

// ListNodeAddressesForNodes is the batch form of ListNodeAddresses. It
// initializes an empty-slice map entry for every input nodeID up front
// (so a node with zero addresses still gets an empty-slice entry, never a
// missing key), then fills in rows from a single
// `WHERE node_id = ANY($1)` query. An empty nodeIDs returns an empty map
// without querying at all.
func (s *pgStore) ListNodeAddressesForNodes(ctx context.Context, nodeIDs []uuid.UUID) (map[uuid.UUID][]NodeAddress, error) {
	out := make(map[uuid.UUID][]NodeAddress, len(nodeIDs))
	for _, id := range nodeIDs {
		out[id] = []NodeAddress{}
	}
	if len(nodeIDs) == 0 {
		return out, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, node_id, address, first_seen, last_seen
		FROM node_addresses
		WHERE node_id = ANY($1)
		ORDER BY node_id, first_seen
	`, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("storage: list node addresses for nodes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var a NodeAddress
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Address, &a.FirstSeen, &a.LastSeen); err != nil {
			return nil, fmt.Errorf("storage: scan node address: %w", err)
		}
		out[a.NodeID] = append(out[a.NodeID], a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list node addresses for nodes: %w", err)
	}
	return out, nil
}

func (s *pgStore) ListNodes(ctx context.Context, filter NodeFilter) ([]Node, error) {
	query := "SELECT " + nodeColumns + " FROM nodes"
	args := []any{}

	if filter.DiscoverySource != "" {
		args = append(args, string(filter.DiscoverySource))
		query += fmt.Sprintf(" WHERE discovery_source = $%d", len(args))
	}

	if filter.ReachableSince != nil {
		args = append(args, *filter.ReachableSince)
		clause := fmt.Sprintf("EXISTS (SELECT 1 FROM node_health nh WHERE nh.node_id = nodes.id AND nh.reachable AND nh.ts >= $%d)", len(args))
		if filter.DiscoverySource != "" {
			query += " AND " + clause
		} else {
			query += " WHERE " + clause
		}
	}

	query += " ORDER BY address"

	// Limit/Offset are strictly opt-in (see NodeFilter's doc comment): a
	// zero Limit never truncates the result set, since internal callers
	// (e.g. the collector's Poll/Discover loops) rely on a zero-value
	// filter returning every node, unpaginated, forever.
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if filter.Offset > 0 {
		args = append(args, filter.Offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("storage: list nodes: %w", err)
	}
	defer rows.Close()

	nodes := []Node{}
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: scan node: %w", err)
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list nodes: %w", err)
	}
	return nodes, nil
}

// CountNodes returns the total number of nodes matching filter's
// DiscoverySource (if set), ignoring filter.Limit/filter.Offset entirely
// — it always reports the full matching population.
func (s *pgStore) CountNodes(ctx context.Context, filter NodeFilter) (int, error) {
	query := "SELECT count(*) FROM nodes"
	args := []any{}
	if filter.DiscoverySource != "" {
		args = append(args, string(filter.DiscoverySource))
		query += " WHERE discovery_source = $1"
	}

	var count int
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("storage: count nodes: %w", err)
	}
	return count, nil
}

func (s *pgStore) GetNode(ctx context.Context, id uuid.UUID) (Node, error) {
	return getNodeByID(ctx, s.pool, id)
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

// scanPeerEdgeRows scans a *peer_edges-shaped* rows result into a
// []PeerEdge. Shared by ListTopology's capped and uncapped paths.
func scanPeerEdgeRows(rows pgx.Rows) ([]PeerEdge, error) {
	edges := []PeerEdge{}
	for rows.Next() {
		var e PeerEdge
		if err := rows.Scan(&e.ID, &e.FromNodeID, &e.ToNodeID, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, fmt.Errorf("storage: scan peer edge: %w", err)
		}
		edges = append(edges, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list topology edges: %w", err)
	}
	return edges, nil
}

// topNodeIDsByDegree ranks every node by total peer-degree — an
// undirected distinct-peer count across ALL peer_edge_observations, with
// no time-window filtering (adapted from TopPeeredNodes's degree CTE,
// minus its `since` filter, since ListTopology's contract is explicitly
// "no time filtering") — and returns the top maxNodes node IDs.
//
// Degree-0 nodes (no edges at all) are included, ordered after every
// nonzero-degree node, so a mostly-empty network still fills out
// maxNodes with something to show rather than an oddly small graph; ties
// (including the "all degree 0" case) are broken by address for
// determinism.
func topNodeIDsByDegree(ctx context.Context, pool *pgxpool.Pool, maxNodes int) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx, `
		WITH undirected AS (
			SELECT from_node_id AS node_id, to_node_id AS other_node_id
			FROM peer_edge_observations
			UNION ALL
			SELECT to_node_id AS node_id, from_node_id AS other_node_id
			FROM peer_edge_observations
		),
		degrees AS (
			SELECT node_id, count(DISTINCT other_node_id) AS degree
			FROM undirected
			GROUP BY node_id
		)
		SELECT n.id
		FROM nodes n
		LEFT JOIN degrees d ON d.node_id = n.id
		ORDER BY COALESCE(d.degree, 0) DESC, n.address
		LIMIT $1
	`, maxNodes)
	if err != nil {
		return nil, fmt.Errorf("storage: rank nodes by degree: %w", err)
	}
	defer rows.Close()

	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("storage: scan ranked node id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: rank nodes by degree: %w", err)
	}
	return ids, nil
}

// ListTopology returns nodes and edges for the graph view. It queries the
// peer_edges VIEW (a rollup of peer_edge_observations), with no
// time-window filtering: every edge ever observed shows up as "current".
// This is intentional for now — there is no delete/edge-removal detection
// yet, so there is nothing more precise to filter by. Callers wanting a
// time-bounded view of connectivity (e.g. "who's well-connected in the
// last hour") should use TopPeeredNodes instead.
//
// When filter.MaxNodes == 0, behavior is byte-for-byte identical to the
// original, uncapped ListTopology: every node and every edge. When
// filter.MaxNodes > 0, the result is capped: only the top MaxNodes nodes
// by total peer-degree (see topNodeIDsByDegree) are returned, and edges
// are restricted to those where BOTH endpoints are in that capped set —
// so the returned graph never has an edge dangling to a node that isn't
// also present in the returned node list.
//
// filter.MaxEdges, if > 0, additionally caps the number of edges
// returned within that same MaxNodes > 0 branch (independent of
// MaxNodes — it can be set alone or combined with it). It has no effect
// when filter.MaxNodes <= 0 (the uncapped branch always returns every
// edge, matching MaxNodes's own all-or-nothing semantics there).
func (s *pgStore) ListTopology(ctx context.Context, filter TopologyFilter) ([]Node, []PeerEdge, error) {
	if filter.MaxNodes <= 0 {
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

		edges, err := scanPeerEdgeRows(rows)
		if err != nil {
			return nil, nil, err
		}
		return nodes, edges, nil
	}

	nodeIDs, err := topNodeIDsByDegree(ctx, s.pool, filter.MaxNodes)
	if err != nil {
		return nil, nil, err
	}
	if len(nodeIDs) == 0 {
		return []Node{}, []PeerEdge{}, nil
	}

	nodeRows, err := s.pool.Query(ctx, `
		SELECT `+nodeColumns+`
		FROM nodes
		WHERE id = ANY($1)
		ORDER BY address
	`, nodeIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: list capped topology nodes: %w", err)
	}
	defer nodeRows.Close()

	nodes := []Node{}
	for nodeRows.Next() {
		n, err := scanNode(nodeRows)
		if err != nil {
			return nil, nil, fmt.Errorf("storage: scan node: %w", err)
		}
		nodes = append(nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("storage: list capped topology nodes: %w", err)
	}

	// filter.MaxEdges, when > 0, adds a LIMIT here. ORDER BY first_seen
	// (already in place above for determinism) means a MaxEdges-truncated
	// result is a non-exhaustive sample of the real edges between the
	// capped node set, not necessarily "the N most significant edges" by
	// any topological measure — this is an intentional, accepted
	// data-volume/response-size tradeoff (not a correctness bug),
	// matching the same tradeoff already accepted for MaxNodes above.
	edgeQuery := `
		SELECT id, from_node_id, to_node_id, first_seen, last_seen
		FROM peer_edges
		WHERE from_node_id = ANY($1) AND to_node_id = ANY($1)
		ORDER BY first_seen
	`
	args := []any{nodeIDs}
	if filter.MaxEdges > 0 {
		edgeQuery += `LIMIT $2`
		args = append(args, filter.MaxEdges)
	}
	edgeRows, err := s.pool.Query(ctx, edgeQuery, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: list capped topology edges: %w", err)
	}
	defer edgeRows.Close()

	edges, err := scanPeerEdgeRows(edgeRows)
	if err != nil {
		return nil, nil, err
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
//
// OnionPeerCount/ClearnetPeerCount further break the undirected peer set
// down by what's known about each peer's OWN addresses (via
// node_addresses, not the ranked node's addresses): OnionPeerCount is how
// many distinct peers have at least one node_addresses row that looks
// like a `.onion` address, and ClearnetPeerCount is how many distinct
// peers have at least one node_addresses row that parses as an IPv4 or
// IPv6 address. A peer with both a known onion address and a known
// clearnet address counts in BOTH numbers — which of a peer's addresses
// was actually used for a given edge observation isn't tracked, only
// that the two nodes are known peers, so peers are classified by their
// overall known capabilities, mirroring
// PublicNode.HasOnion/HasIPv4/HasIPv6 in internal/api/privacy.go (that
// classification is intentionally re-implemented here in SQL rather than
// imported, since internal/storage must not depend on internal/api).
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
		),
		onion_peer_counts AS (
			SELECT u.node_id, count(DISTINCT u.other_node_id) AS onion_peer_count
			FROM undirected u
			WHERE EXISTS (
				SELECT 1 FROM node_addresses na
				WHERE na.node_id = u.other_node_id
				  AND na.address ILIKE '%.onion%'
			)
			GROUP BY u.node_id
		),
		clearnet_peer_counts AS (
			SELECT u.node_id, count(DISTINCT u.other_node_id) AS clearnet_peer_count
			FROM undirected u
			WHERE EXISTS (
				SELECT 1 FROM node_addresses na
				WHERE na.node_id = u.other_node_id
				  AND na.address NOT ILIKE '%.onion%'
				  AND (
					-- host:port with an IPv4 dotted-quad host.
					na.address ~ '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]+$'
					-- [host]:port with a bracketed IPv6 host (the
					-- textual form net.JoinHostPort/net.SplitHostPort
					-- use, matching classifyAddress's Go-side handling).
					OR na.address ~ '^\[[0-9a-fA-F:]+\]:[0-9]+$'
				  )
			)
			GROUP BY u.node_id
		)
		SELECT n.id, n.address, d.degree,
			COALESCE(ind.in_degree, 0), COALESCE(outd.out_degree, 0),
			COALESCE(opc.onion_peer_count, 0), COALESCE(cpc.clearnet_peer_count, 0)
		FROM degrees d
		JOIN nodes n ON n.id = d.node_id
		LEFT JOIN in_degrees ind ON ind.node_id = d.node_id
		LEFT JOIN out_degrees outd ON outd.node_id = d.node_id
		LEFT JOIN onion_peer_counts opc ON opc.node_id = d.node_id
		LEFT JOIN clearnet_peer_counts cpc ON cpc.node_id = d.node_id
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
		if err := rows.Scan(
			&nd.NodeID, &nd.Address, &nd.Degree, &nd.InDegree, &nd.OutDegree,
			&nd.OnionPeerCount, &nd.ClearnetPeerCount,
		); err != nil {
			return nil, fmt.Errorf("storage: scan node degree: %w", err)
		}
		out = append(out, nd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: top peered nodes: %w", err)
	}
	return out, nil
}

// ListNodeEdges returns every peer_edges row (the peer_edge_observations
// rollup view described in ListTopology's doc comment) where nodeID is
// either the from or to side, newest first by last_seen, capped at limit
// (0 or negative defaults to 50).
func (s *pgStore) ListNodeEdges(ctx context.Context, nodeID uuid.UUID, limit int) ([]PeerEdge, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, from_node_id, to_node_id, first_seen, last_seen
		FROM peer_edges
		WHERE from_node_id = $1 OR to_node_id = $1
		ORDER BY last_seen DESC
		LIMIT $2
	`, nodeID, limit)
	if err != nil {
		return nil, fmt.Errorf("storage: list node edges: %w", err)
	}
	defer rows.Close()

	edges := []PeerEdge{}
	for rows.Next() {
		var e PeerEdge
		if err := rows.Scan(&e.ID, &e.FromNodeID, &e.ToNodeID, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, fmt.Errorf("storage: scan node edge: %w", err)
		}
		edges = append(edges, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list node edges: %w", err)
	}
	return edges, nil
}

// NetworkHeight returns the most common height value across the most
// recent health check per node (mode of the latest-per-node heights), and
// the number of nodes that value was derived from. It first picks each
// node's latest node_health row via DISTINCT ON (node_id) ... ORDER BY
// node_id, ts DESC, filters out rows with a NULL height, then groups the
// remaining heights and returns the one with the highest count. Returns
// (nil, 0, nil) if no health checks with a non-nil height exist yet.
func (s *pgStore) NetworkHeight(ctx context.Context) (*int64, int, error) {
	var height int64
	var count int
	err := s.pool.QueryRow(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (node_id) node_id, height
			FROM node_health
			ORDER BY node_id, ts DESC
		)
		SELECT height, count(*) AS node_count
		FROM latest
		WHERE height IS NOT NULL
		GROUP BY height
		ORDER BY node_count DESC
		LIMIT 1
	`).Scan(&height, &count)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("storage: network height: %w", err)
	}
	return &height, count, nil
}

// ListSeedCandidates returns nodes that are BOTH opted-in
// (discovery_source IN ('registry_submitted', 'both')) AND have at least
// one reachable=true node_health row at or after (now() - since). This
// is a single query, not N+1: it filters nodes by discovery_source and
// a non-null public_key, gates on a recency-filtered EXISTS against
// node_health (passing a computed time.Time cutoff, the same
// `time.Now().Add(-since)` convention TopPeeredNodes already uses for
// its own `since` parameter, rather than a raw interval), INNER JOINs
// node_addresses (a confirmed node — public_key IS NOT NULL — always
// has at least one node_addresses row, since every path that sets
// public_key also calls ensureNodeAddress), and aggregates every known
// address per node with array_agg + GROUP BY, ordered by node id for
// stable output. Nodes with a NULL public_key are excluded entirely: a
// peer_seeds line requires a real pubkey, and a placeholder node
// discovered by address alone can't produce one.
func (s *pgStore) ListSeedCandidates(ctx context.Context, since time.Duration) ([]SeedCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT n.id, n.public_key, n.label, n.tags,
			array_agg(na.address ORDER BY na.first_seen) AS addresses
		FROM nodes n
		JOIN node_addresses na ON na.node_id = n.id
		WHERE n.discovery_source IN ('registry_submitted', 'both')
		  AND n.public_key IS NOT NULL
		  AND EXISTS (
		      SELECT 1 FROM node_health h
		      WHERE h.node_id = n.id AND h.reachable AND h.ts >= $1
		  )
		GROUP BY n.id, n.public_key, n.label, n.tags
		ORDER BY n.id
	`, time.Now().Add(-since))
	if err != nil {
		return nil, fmt.Errorf("storage: list seed candidates: %w", err)
	}
	defer rows.Close()

	out := []SeedCandidate{}
	for rows.Next() {
		var c SeedCandidate
		var tagsOut []byte
		if err := rows.Scan(&c.NodeID, &c.PublicKey, &c.Label, &tagsOut, &c.Addresses); err != nil {
			return nil, fmt.Errorf("storage: scan seed candidate: %w", err)
		}
		if len(tagsOut) > 0 {
			if err := json.Unmarshal(tagsOut, &c.Tags); err != nil {
				return nil, fmt.Errorf("storage: unmarshal seed candidate tags: %w", err)
			}
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list seed candidates: %w", err)
	}
	return out, nil
}

// pendingSubmissionColumns is the column list, in order, matching
// scanPendingSubmission's Scan calls.
const pendingSubmissionColumns = "id, address, label, owner_tag, status, submitted_at, reviewed_at, rejection_reason, promoted_node_id, probe_attempted_at, probe_reachable"

// scanPendingSubmission scans one pendingSubmissionColumns-shaped row into
// a PendingSubmission.
func scanPendingSubmission(row pgx.Row) (PendingSubmission, error) {
	var ps PendingSubmission
	if err := row.Scan(
		&ps.ID, &ps.Address, &ps.Label, &ps.OwnerTag, &ps.Status,
		&ps.SubmittedAt, &ps.ReviewedAt, &ps.RejectionReason, &ps.PromotedNodeID,
		&ps.ProbeAttemptedAt, &ps.ProbeReachable,
	); err != nil {
		return PendingSubmission{}, err
	}
	return ps, nil
}

// IsAddressPubliclyOptedIn reports whether address already belongs to a
// node whose discovery_source is registry_submitted or both. It joins
// node_addresses to nodes rather than checking nodes.address directly, so
// it also catches addresses recorded as a secondary node_addresses row
// (see UpsertConfirmedNode's case (b): a node can have multiple
// simultaneously-valid addresses).
func (s *pgStore) IsAddressPubliclyOptedIn(ctx context.Context, address string) (bool, error) {
	var optedIn bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM node_addresses na
			JOIN nodes n ON n.id = na.node_id
			WHERE na.address = $1
			  AND n.discovery_source IN ('registry_submitted', 'both')
		)
	`, address).Scan(&optedIn)
	if err != nil {
		return false, fmt.Errorf("storage: is address publicly opted in: %w", err)
	}
	return optedIn, nil
}

// CreatePendingSubmission records a new public node-submission for
// review. If a PENDING submission for this exact address already exists,
// this UPDATEs that row's label/owner_tag and bumps its submitted_at
// instead of inserting a duplicate. This "update in place" behavior
// (rather than rejecting the second submission as a duplicate) is
// deliberate: it keeps the API idempotent-ish for a legitimate
// re-submitter who's still waiting on review (e.g. they made a typo in
// the label and just want to fix it), and the partial unique index on
// (address) WHERE status = 'pending' already guarantees no two PENDING
// rows can exist for the same address at the database level — so the
// application-level path has to handle the "already pending" case
// cleanly one way or another, rather than surfacing a raw constraint
// violation to the caller.
func (s *pgStore) CreatePendingSubmission(ctx context.Context, address string, label, ownerTag *string) (PendingSubmission, error) {
	if address == "" {
		return PendingSubmission{}, fmt.Errorf("storage: address is required")
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO pending_submissions (address, label, owner_tag, status, submitted_at)
		VALUES ($1, $2, $3, 'pending', now())
		ON CONFLICT (address) WHERE status = 'pending'
		DO UPDATE SET label = $2, owner_tag = $3, submitted_at = now()
		RETURNING `+pendingSubmissionColumns, address, label, ownerTag)
	ps, err := scanPendingSubmission(row)
	if err != nil {
		return PendingSubmission{}, fmt.Errorf("storage: create pending submission: %w", err)
	}
	return ps, nil
}

// ListPendingSubmissions returns submissions with the given exact status,
// newest-submitted first. An empty status defaults to "pending".
func (s *pgStore) ListPendingSubmissions(ctx context.Context, status string) ([]PendingSubmission, error) {
	if status == "" {
		status = SubmissionStatusPending
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+pendingSubmissionColumns+`
		FROM pending_submissions
		WHERE status = $1
		ORDER BY submitted_at DESC
	`, status)
	if err != nil {
		return nil, fmt.Errorf("storage: list pending submissions: %w", err)
	}
	defer rows.Close()

	submissions := []PendingSubmission{}
	for rows.Next() {
		ps, err := scanPendingSubmission(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: scan pending submission: %w", err)
		}
		submissions = append(submissions, ps)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list pending submissions: %w", err)
	}
	return submissions, nil
}

// GetPendingSubmission returns a single submission by ID, or ErrNotFound
// if no such submission exists.
func (s *pgStore) GetPendingSubmission(ctx context.Context, id uuid.UUID) (PendingSubmission, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+pendingSubmissionColumns+` FROM pending_submissions WHERE id = $1`, id)
	ps, err := scanPendingSubmission(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PendingSubmission{}, ErrNotFound
		}
		return PendingSubmission{}, fmt.Errorf("storage: get pending submission: %w", err)
	}
	return ps, nil
}

// ApprovePendingSubmission marks submission id as approved, setting
// reviewed_at and promoted_node_id. It errors if the submission is not
// currently "pending" — the WHERE status = 'pending' clause means the
// UPDATE affects zero rows in that case, which this method detects via
// RowsAffected and reports as an error rather than silently succeeding.
func (s *pgStore) ApprovePendingSubmission(ctx context.Context, id uuid.UUID, promotedNodeID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE pending_submissions SET
			status = 'approved',
			reviewed_at = now(),
			promoted_node_id = $2
		WHERE id = $1 AND status = 'pending'
	`, id, promotedNodeID)
	if err != nil {
		return fmt.Errorf("storage: approve pending submission: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.GetPendingSubmission(ctx, id); err != nil {
			return err
		}
		return fmt.Errorf("storage: submission %s is not pending", id)
	}
	return nil
}

// RecordSubmissionProbeResult records the outcome of a best-effort
// connectivity probe run against a still-pending submission at creation
// time. Unlike Approve/RejectPendingSubmission, a plain `WHERE id = $1`
// update with no status guard and no error on zero rows affected is
// deliberate here: by the time the async probe finishes, the submission
// may have already been approved or rejected by a human reviewer — that's
// an acceptable, harmless race (the probe result is purely informational
// for the reviewer while the row was still pending), not something the
// caller needs to know about or handle.
func (s *pgStore) RecordSubmissionProbeResult(ctx context.Context, id uuid.UUID, reachable bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE pending_submissions SET
			probe_attempted_at = now(),
			probe_reachable = $2
		WHERE id = $1
	`, id, reachable)
	if err != nil {
		return fmt.Errorf("storage: record submission probe result: %w", err)
	}
	return nil
}

// CountPendingSubmissions returns the number of submissions currently in
// the 'pending' state.
func (s *pgStore) CountPendingSubmissions(ctx context.Context) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pending_submissions WHERE status = 'pending'`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("storage: count pending submissions: %w", err)
	}
	return count, nil
}

// RejectPendingSubmission marks submission id as rejected, setting
// reviewed_at and rejection_reason. Same "must currently be pending"
// guard as ApprovePendingSubmission.
func (s *pgStore) RejectPendingSubmission(ctx context.Context, id uuid.UUID, reason *string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE pending_submissions SET
			status = 'rejected',
			reviewed_at = now(),
			rejection_reason = $2
		WHERE id = $1 AND status = 'pending'
	`, id, reason)
	if err != nil {
		return fmt.Errorf("storage: reject pending submission: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.GetPendingSubmission(ctx, id); err != nil {
			return err
		}
		return fmt.Errorf("storage: submission %s is not pending", id)
	}
	return nil
}
