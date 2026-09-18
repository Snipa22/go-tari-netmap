package remotestore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// The wire types below mirror internal/api/collector_report.go's collectorReportRequest/
// collectorReportResponse field-for-field (by JSON tag, not by importing that package --
// internal/api is a much larger dependency than this store needs, and keeping the wire
// contract as plain, independently-defined JSON tags on both ends is standard practice for an
// HTTP API boundary; internal/remotestore's own test suite exercises this contract end-to-end
// against a real internal/api-backed httptest.Server to catch drift between the two).

type reportWireRequest struct {
	SelfIdentity    []string                   `json:"self_identity"`
	ConfirmedNodes  []reportWireConfirmedNode  `json:"confirmed_nodes"`
	DiscoveredNodes []reportWireDiscoveredNode `json:"discovered_nodes"`
	HealthChecks    []reportWireHealthCheck    `json:"health_checks"`
	PeerEdges       []reportWirePeerEdge       `json:"peer_edges"`
}

type reportWireConfirmedNode struct {
	Address         string `json:"address"`
	PublicKey       string `json:"public_key"`
	DiscoverySource string `json:"discovery_source"`
}

type reportWireDiscoveredNode struct {
	Address string `json:"address"`
}

type reportWireHealthCheck struct {
	Address               string     `json:"address"`
	Reachable             bool       `json:"reachable"`
	ProbeSource           string     `json:"probe_source"`
	Height                *int64     `json:"height,omitempty"`
	ChainTipHeight        *int64     `json:"chain_tip_height,omitempty"`
	Version               *string    `json:"version,omitempty"`
	LatencyMS             *int       `json:"latency_ms,omitempty"`
	RxtHashrate           *float64   `json:"rxt_hashrate,omitempty"`
	C29Hashrate           *float64   `json:"c29_hashrate,omitempty"`
	Sha3xHashrate         *float64   `json:"sha3x_hashrate,omitempty"`
	PeerIdentityUpdatedAt *time.Time `json:"peer_identity_updated_at,omitempty"`
}

type reportWirePeerEdge struct {
	FromAddress string `json:"from_address"`
	ToAddress   string `json:"to_address"`
}

type reportWireResponse struct {
	ConfirmedNodesApplied  int `json:"confirmed_nodes_applied"`
	DiscoveredNodesApplied int `json:"discovered_nodes_applied"`
	HealthChecksApplied    int `json:"health_checks_applied"`
	PeerEdgesApplied       int `json:"peer_edges_applied"`
	SelfIdentityTagged     int `json:"self_identity_tagged"`
}

// flushOnce snapshots and clears every pending buffer, then sends it to the central API as one
// or more bounded POST /internal/collectors/report requests, each containing AT MOST
// Config.FlushBatchSize records combined across all four buffers (see splitBatch) -- see this
// repo's readiness-review follow-up, Fix 3, for why: sending the entire backlog in one
// unbounded request (the previous behavior) meant that after any central outage, the very
// next flush could be enormous, risking a client-side timeout mid-request against a server
// applying items one-or-more-SQL-round-trips-each; Config.SelfAddresses is sent as
// self_identity on EVERY sub-batch request (even one with nothing else to report), regardless
// of whether any buffer had anything in it -- self-identity tagging must not depend on this
// collector having something else to report, and re-tagging on every request is a harmless,
// idempotent no-op server-side.
//
// On failure, the CURRENTLY-remaining data (the sub-batch that just failed, PLUS every
// not-yet-attempted item still queued behind it -- flushOnce advances past a sub-batch only
// after it succeeds) is restored -- PREPENDED to whatever has accumulated in the buffers in
// the meantime, so it's retried, in order, on the next flush trigger -- rather than being
// dropped. This is the "a flush failure must never drop buffered data" guarantee this
// package's doc comment promises. A batch that DOES fail partway through a multi-sub-batch
// flush leaves every already-successfully-sent sub-batch applied centrally and only the
// remainder queued for retry -- internal/api/collector_report.go's applyCollectorReport makes
// that retry idempotent (Fix 3's other half): a byte-identical resend of the same sub-batch
// (guaranteed here, since json.Marshal of a map-free struct is deterministic and the retried
// slice's content is unchanged) is recognized and not re-applied a second time.
//
// A flush with nothing to report at all (every buffer empty AND no configured
// SelfAddresses) is skipped entirely without any HTTP call.
func (s *Store) flushOnce(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	confirmed := s.pendingConfirmed
	discovered := s.pendingDiscovered
	health := s.pendingHealth
	edges := s.pendingEdges
	s.pendingConfirmed = nil
	s.pendingDiscovered = nil
	s.pendingHealth = nil
	s.pendingEdges = nil
	s.mu.Unlock()

	if len(confirmed) == 0 && len(discovered) == 0 && len(health) == 0 && len(edges) == 0 && len(s.cfg.SelfAddresses) == 0 {
		return nil
	}

	batchSize := s.flushBatchSize()
	// first ensures at least one request is sent even when every buffer is already empty
	// but Config.SelfAddresses is non-empty (a self-identity-only flush) -- every subsequent
	// iteration's condition (any buffer still non-empty) is what actually drives a
	// multi-sub-batch flush.
	first := true
	for first || len(confirmed) > 0 || len(discovered) > 0 || len(health) > 0 || len(edges) > 0 {
		first = false

		cN, dN, hN, eN := splitBatch(len(confirmed), len(discovered), len(health), len(edges), batchSize)
		batchConfirmed := confirmed[:cN]
		batchDiscovered := discovered[:dN]
		batchHealth := health[:hN]
		batchEdges := edges[:eN]

		req := reportWireRequest{
			SelfIdentity:    s.cfg.SelfAddresses,
			ConfirmedNodes:  make([]reportWireConfirmedNode, len(batchConfirmed)),
			DiscoveredNodes: make([]reportWireDiscoveredNode, len(batchDiscovered)),
			HealthChecks:    make([]reportWireHealthCheck, len(batchHealth)),
			PeerEdges:       make([]reportWirePeerEdge, len(batchEdges)),
		}
		for i, c := range batchConfirmed {
			req.ConfirmedNodes[i] = reportWireConfirmedNode{
				Address:         c.Address,
				PublicKey:       hex.EncodeToString(c.PublicKey),
				DiscoverySource: string(c.DiscoverySource),
			}
		}
		for i, d := range batchDiscovered {
			req.DiscoveredNodes[i] = reportWireDiscoveredNode{Address: d.Address}
		}
		for i, h := range batchHealth {
			req.HealthChecks[i] = reportWireHealthCheck{
				Address:               h.Address,
				Reachable:             h.Input.Reachable,
				ProbeSource:           string(h.Input.ProbeSource),
				Height:                h.Input.Height,
				ChainTipHeight:        h.Input.ChainTipHeight,
				Version:               h.Input.Version,
				LatencyMS:             h.Input.LatencyMS,
				RxtHashrate:           h.Input.RxtHashrate,
				C29Hashrate:           h.Input.C29Hashrate,
				Sha3xHashrate:         h.Input.Sha3xHashrate,
				PeerIdentityUpdatedAt: h.Input.PeerIdentityUpdatedAt,
			}
		}
		for i, e := range batchEdges {
			req.PeerEdges[i] = reportWirePeerEdge{FromAddress: e.FromAddress, ToAddress: e.ToAddress}
		}

		resp, err := s.postReport(ctx, req)

		// Report-channel health/metrics bookkeeping (Fix 1/Fix 2) -- recorded for every
		// actual HTTP attempt, success or failure, one update per sub-batch.
		s.mu.Lock()
		s.recordFlushResultLocked(err)
		s.mu.Unlock()
		if s.cfg.OnFlushResult != nil {
			s.cfg.OnFlushResult(err == nil)
		}

		if err != nil {
			// Restore everything not yet successfully sent: confirmed/discovered/health/
			// edges at this point still hold the just-failed sub-batch PLUS every
			// not-yet-attempted item behind it (we only reslice past a sub-batch after it
			// succeeds, below) -- so restoring exactly these four slices, prepended to
			// whatever has accumulated in the meantime, is correct without any further
			// bookkeeping.
			s.mu.Lock()
			s.pendingConfirmed = append(append([]pendingConfirmedNode{}, confirmed...), s.pendingConfirmed...)
			s.pendingDiscovered = append(append([]pendingDiscoveredNode{}, discovered...), s.pendingDiscovered...)
			s.pendingHealth = append(append([]pendingHealthCheck{}, health...), s.pendingHealth...)
			s.pendingEdges = append(append([]pendingPeerEdge{}, edges...), s.pendingEdges...)
			s.mu.Unlock()
			return err
		}

		s.logf("remotestore: flushed report batch (collector=%s): confirmed_nodes=%d discovered_nodes=%d health_checks=%d peer_edges=%d self_identity_tagged=%d",
			s.cfg.CollectorName, resp.ConfirmedNodesApplied, resp.DiscoveredNodesApplied, resp.HealthChecksApplied, resp.PeerEdgesApplied, resp.SelfIdentityTagged)

		confirmed = confirmed[cN:]
		discovered = discovered[dN:]
		health = health[hN:]
		edges = edges[eN:]
	}

	return nil
}

// splitBatch computes how many items of each pending buffer to include in the next bounded
// sub-batch request, capped at maxTotal combined across all four -- matching
// maybeSignalFlushLocked's own "total = sum of all four buffers" accounting (writes.go). The
// order (confirmed, then discovered, then health, then edges) is arbitrary but stable; each
// returned count is always <= its corresponding input length, and the four returned counts
// never sum to more than maxTotal (they may sum to less, if the combined backlog is smaller
// than maxTotal -- the common case).
func splitBatch(confirmedLen, discoveredLen, healthLen, edgesLen, maxTotal int) (c, d, h, e int) {
	remaining := maxTotal
	take := func(n int) int {
		if n > remaining {
			n = remaining
		}
		if n < 0 {
			n = 0
		}
		remaining -= n
		return n
	}
	c = take(confirmedLen)
	d = take(discoveredLen)
	h = take(healthLen)
	e = take(edgesLen)
	return
}

// postReport issues the actual POST /internal/collectors/report HTTP call.
func (s *Store) postReport(ctx context.Context, req reportWireRequest) (reportWireResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return reportWireResponse{}, errRemoteStore("marshal report request: %w", err)
	}

	url := strings.TrimRight(s.cfg.BaseURL, "/") + "/internal/collectors/report"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return reportWireResponse{}, errRemoteStore("build report request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Collector-Key", s.cfg.APIKey)

	resp, err := s.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return reportWireResponse{}, errRemoteStore("post report: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return reportWireResponse{}, errRemoteStore("post report: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	var out reportWireResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return reportWireResponse{}, errRemoteStore("decode report response: %w", err)
	}
	return out, nil
}
