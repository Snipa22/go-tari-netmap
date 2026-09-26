package collector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// PruneOldPeerEdgeObservations deletes peer_edge_observations rows older than
// Config.PeerEdgeObservationRetention (defaulting to defaultPeerEdgeObservationRetention, 30
// days, when unset/<= 0), returning the number of rows deleted.
//
// peer_edge_observations is deliberately a PLAIN (non-hypertable) table, unlike node_health
// (see 0002_timescale_hypertable_optional.sql/0016_node_health_retention_optional.sql).
// Converting it to a TimescaleDB hypertable was attempted live and reverted: TimescaleDB
// requires every UNIQUE index on a hypertable to include the partitioning column, but
// idx_peer_edge_observations_report_batch_id -- the partial UNIQUE index on (from_node_id,
// to_node_id, report_batch_id) WHERE report_batch_id IS NOT NULL that
// internal/api/collector_report.go's applyCollectorReport relies on for atomic
// ON CONFLICT ... DO NOTHING retry-idempotency (see 0012_report_batch_idempotency.sql) -- does
// not, and cannot, include observed_at without breaking that dedup (a retried batch's rows get
// a fresh observed_at on every retry, so a partitioning-column-inclusive unique index would
// never actually catch a duplicate). So this table keeps its retention bounded at the
// application level instead, via this batched DELETE, called on a timer by
// runPeerEdgeRetentionLoop.
//
// Deletion is done in bounded batches of peerEdgeRetentionBatchSize rows per DELETE statement
// (see storage.Store.PruneOldPeerEdgeObservations), looping until a batch removes fewer than
// that many rows, rather than a single unbounded DELETE -- this table is currently on the order
// of millions of rows and gigabytes in size, and one giant DELETE would hold a long-lived lock/
// transaction against it. This mirrors this codebase's established preference for bounded/
// batched operations over large tables elsewhere (e.g. remotestore's Config.FlushBatchSize).
func (c *Collector) PruneOldPeerEdgeObservations(ctx context.Context) (int64, error) {
	if !c.PeerEdgeRetentionEnabled {
		return 0, nil
	}
	if c.Storage == nil {
		return 0, errors.New("collector: Storage is not configured")
	}

	retention := c.PeerEdgeObservationRetention
	if retention <= 0 {
		retention = defaultPeerEdgeObservationRetention
	}

	cutoff := time.Now().Add(-retention)
	deleted, err := c.Storage.PruneOldPeerEdgeObservations(ctx, cutoff, peerEdgeRetentionBatchSize)
	if err != nil {
		return deleted, fmt.Errorf("collector: prune old peer edge observations: %w", err)
	}
	return deleted, nil
}

// runPeerEdgeRetentionLoop runs PruneOldPeerEdgeObservations once immediately, then on every
// tick, until ctx is cancelled. It runs entirely independently of every other loop in Run (see
// Run's peerEdgeRetentionTick), on its own ticker -- a slow/backlogged prune pass never delays,
// and can never be delayed by, any poll/discovery/geoip-refresh loop.
func (c *Collector) runPeerEdgeRetentionLoop(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	runOnce := func() {
		deleted, err := c.PruneOldPeerEdgeObservations(ctx)
		if err != nil {
			log.Printf("collector: peer edge retention pass error: %v", err)
			return
		}
		log.Printf("collector: peer edge retention: pruned %d peer_edge_observations row(s)", deleted)
	}

	runOnce()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
