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

// flushOnce snapshots and clears every pending buffer, builds a single report request
// (including Config.SelfAddresses as self_identity on every call, regardless of whether any
// buffer had anything in it -- self-identity tagging must not depend on this collector having
// something else to report), and POSTs it to the central API.
//
// On failure, the snapshotted data is restored -- PREPENDED to whatever has accumulated in
// the buffers in the meantime, so it's retried, in order, on the next flush trigger -- rather
// than being dropped. This is the "a flush failure must never drop buffered data" guarantee
// this package's doc comment promises.
//
// A flush with nothing to report at all (every buffer empty AND no configured
// SelfAddresses) is skipped entirely without an HTTP call.
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

	req := reportWireRequest{
		SelfIdentity:    s.cfg.SelfAddresses,
		ConfirmedNodes:  make([]reportWireConfirmedNode, len(confirmed)),
		DiscoveredNodes: make([]reportWireDiscoveredNode, len(discovered)),
		HealthChecks:    make([]reportWireHealthCheck, len(health)),
		PeerEdges:       make([]reportWirePeerEdge, len(edges)),
	}
	for i, c := range confirmed {
		req.ConfirmedNodes[i] = reportWireConfirmedNode{
			Address:         c.Address,
			PublicKey:       hex.EncodeToString(c.PublicKey),
			DiscoverySource: string(c.DiscoverySource),
		}
	}
	for i, d := range discovered {
		req.DiscoveredNodes[i] = reportWireDiscoveredNode{Address: d.Address}
	}
	for i, h := range health {
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
	for i, e := range edges {
		req.PeerEdges[i] = reportWirePeerEdge{FromAddress: e.FromAddress, ToAddress: e.ToAddress}
	}

	resp, err := s.postReport(ctx, req)
	if err != nil {
		s.mu.Lock()
		s.pendingConfirmed = append(append([]pendingConfirmedNode{}, confirmed...), s.pendingConfirmed...)
		s.pendingDiscovered = append(append([]pendingDiscoveredNode{}, discovered...), s.pendingDiscovered...)
		s.pendingHealth = append(append([]pendingHealthCheck{}, health...), s.pendingHealth...)
		s.pendingEdges = append(append([]pendingPeerEdge{}, edges...), s.pendingEdges...)
		s.mu.Unlock()
		return err
	}

	s.logf("remotestore: flushed report (collector=%s): confirmed_nodes=%d discovered_nodes=%d health_checks=%d peer_edges=%d self_identity_tagged=%d",
		s.cfg.CollectorName, resp.ConfirmedNodesApplied, resp.DiscoveredNodesApplied, resp.HealthChecksApplied, resp.PeerEdgesApplied, resp.SelfIdentityTagged)
	return nil
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
