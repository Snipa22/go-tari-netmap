// Package api exposes an HTTP API for topology/health data: node registry
// submission and listing, per-node health history, and the topology graph.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/collector"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

// asyncCheckTimeout bounds the best-effort health check kicked off when a
// node is submitted via POST /nodes.
const asyncCheckTimeout = 30 * time.Second

// MaxPendingSubmissions caps the number of unreviewed ('pending')
// submissions the review queue will hold at once — abuse mitigation
// against unbounded queue growth from spam, distinct from the per-IP rate
// limit (this caps total outstanding queue size regardless of how many
// distinct source IPs contribute to it). 100 is a generous cap for a
// human-reviewed queue (in normal operation it should rarely get anywhere
// near this) while still bounding worst-case storage/listing cost. It's
// an exported var, not a const, specifically so tests can temporarily
// lower it (rather than actually creating 100 real rows to exercise the
// cap) — see internal/api/api_test.go's queue-cap test.
var MaxPendingSubmissions = 100

// NewRouter returns the HTTP handler for the API. store persists topology
// and health data; grpcClient and p2pClient are used to kick off an async,
// non-blocking health check via each configured transport when a new node
// is registered via POST /nodes. Either may be nil — collector.PollOnce
// skips a nil client's probe entirely rather than erroring.
func NewRouter(store storage.Store, grpcClient, p2pClient collector.NodeClient) http.Handler {
	mux := http.NewServeMux()

	// Created once and shared across every POST /nodes call (NewRouter
	// itself is only called once, at process startup) — see
	// internal/api/ratelimit.go for both types' reasoning.
	limiter := newIPRateLimiter()
	lockouts := newInvalidSubmissionTracker()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /nodes", handleListNodes(store))
	mux.HandleFunc("POST /nodes", handleCreateNode(store, grpcClient, p2pClient, limiter, lockouts))
	mux.HandleFunc("POST /nodes/poll-now", handlePollNow(store, grpcClient, p2pClient))
	mux.HandleFunc("GET /nodes/{id}", handleGetNode(store))
	mux.HandleFunc("GET /nodes/{id}/history", handleGetNodeHistory(store))
	mux.HandleFunc("GET /topology", handleTopology(store))
	mux.HandleFunc("GET /topology/top-peered", handleTopPeeredNodes(store))

	mux.HandleFunc("GET /submissions", handleListSubmissions(store))
	mux.HandleFunc("POST /submissions/{id}/approve", handleApproveSubmission(store, grpcClient, p2pClient))
	mux.HandleFunc("POST /submissions/{id}/reject", handleRejectSubmission(store))

	return mux
}

func handleListNodes(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := storage.NodeFilter{
			DiscoverySource: storage.DiscoverySource(r.URL.Query().Get("discovery_source")),
		}

		nodes, err := store.ListNodes(r.Context(), filter)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		public := make([]PublicNode, len(nodes))
		for i, n := range nodes {
			addrs, err := store.ListNodeAddresses(r.Context(), n.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			public[i] = ScrubNode(n, addrs)
		}
		writeJSON(w, http.StatusOK, public)
	}
}

// createNodeRequest is the POST /nodes request body: a public GRPC node
// registration submission.
type createNodeRequest struct {
	Host     string   `json:"host"`
	Port     flexPort `json:"port"`
	Label    string   `json:"label"`
	OwnerTag string   `json:"owner_tag"`
}

// flexPort unmarshals a JSON number or a numeric JSON string into an int.
// The API contract is a JSON number, but the dashboard's htmx form submits
// via the json-enc extension, which encodes all form fields (including
// <input type="number">) as JSON strings — so both are accepted.
type flexPort int

func (p *flexPort) UnmarshalJSON(data []byte) error {
	var n int
	if err := json.Unmarshal(data, &n); err == nil {
		*p = flexPort(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return errors.New("port must be a number")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return errors.New("port must be a number")
	}
	*p = flexPort(n)
	return nil
}

func handleCreateNode(store storage.Store, grpcClient, p2pClient collector.NodeClient, limiter *ipRateLimiter, lockouts *invalidSubmissionTracker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}

		ip := clientIP(r)

		// Escalating lockout for repeatedly-invalid submitters is
		// checked before anything else, including the flat rate
		// limit — a locked-out source shouldn't even burn one of its
		// rate-limit tokens re-attempting.
		if locked, remaining := lockouts.IsLockedOut(ip); locked {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(remaining.Seconds())))
			writeError(w, http.StatusTooManyRequests, errors.New("this source has been temporarily blocked after repeated invalid submissions"))
			return
		}

		if !limiter.Allow(ip) {
			w.Header().Set("Retry-After", "3600")
			writeError(w, http.StatusTooManyRequests, errors.New("too many submissions from this source, try again later"))
			return
		}

		if req.Host == "" {
			lockouts.RecordInvalidStrike(ip)
			writeError(w, http.StatusBadRequest, errors.New("host is required"))
			return
		}
		if req.Port < 1 || req.Port > 65535 {
			lockouts.RecordInvalidStrike(ip)
			writeError(w, http.StatusBadRequest, errors.New("port must be between 1 and 65535"))
			return
		}

		// Basic SSRF hardening: reject private/reserved IPs (this
		// service's own async health-check/probe machinery would
		// otherwise dial whatever's submitted here) and anything that
		// isn't a syntactically plausible public IP or .onion address.
		// See validateSubmittedHost's doc comment for the exact rules.
		if err := validateSubmittedHost(req.Host); err != nil {
			lockouts.RecordInvalidStrike(ip)
			writeError(w, http.StatusBadRequest, err)
			return
		}

		address := fmt.Sprintf("%s:%d", req.Host, req.Port)

		// A public submission of an address that's already publicly
		// opted-in (registry_submitted or both) must NOT be allowed to
		// silently merge into the existing node — that would let anyone
		// re-submit someone else's already-registered address and bump
		// its last_seen/merge tags/overwrite its label with zero
		// ownership check. Reject outright rather than queuing it for
		// review: there's nothing to review, the address already has an
		// owner of record. This is a well-formed, merely-conflicting
		// submission (not bad-faith malformed input), so no invalid
		// strike is recorded for it.
		optedIn, err := store.IsAddressPubliclyOptedIn(r.Context(), address)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if optedIn {
			writeError(w, http.StatusConflict, errors.New("this address is already publicly registered — contact the operator to update it"))
			return
		}

		// Hard cap on unreviewed queue size — a capacity problem, not a
		// fault of this particular submission, so no invalid strike
		// either.
		pendingCount, err := store.CountPendingSubmissions(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if pendingCount >= MaxPendingSubmissions {
			writeError(w, http.StatusServiceUnavailable, errors.New("submission queue is full, try again later"))
			return
		}

		var label *string
		if req.Label != "" {
			label = &req.Label
		}
		var ownerTag *string
		if req.OwnerTag != "" {
			ownerTag = &req.OwnerTag
		}

		// Submissions no longer auto-publish: they queue for human
		// review (see storage.PendingSubmission and
		// handleApproveSubmission/handleRejectSubmission below).
		// UpsertDiscoveredNode and the async health-check kickoff are
		// now deferred to approval time.
		submission, err := store.CreatePendingSubmission(r.Context(), address, label, ownerTag)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// Best-effort, non-blocking connectivity probe for the human
		// reviewer's benefit — launched via `go` and never awaited, so
		// it cannot delay this response. Uses its own
		// context.Background()-derived timeout (see probeSubmission's
		// doc comment) since r.Context() is canceled once this handler
		// returns.
		go probeSubmission(store, grpcClient, p2pClient, submission.ID, address)

		writeJSON(w, http.StatusAccepted, submission)
	}
}

// pollNowRequest is the POST /nodes/poll-now request body: same host/port
// shape as createNodeRequest, minus the registry-submission-only fields
// (Label, OwnerTag) that don't apply to a forced admin probe.
type pollNowRequest struct {
	Host string   `json:"host"`
	Port flexPort `json:"port"`
}

// pollNowResponse is the POST /nodes/poll-now response body. Unlike
// every other node-returning endpoint in this file, it returns the
// FULL, unscrubbed storage.Node (real address, real pubkey if
// confirmed) rather than going through ScrubNode/PublicNode — this
// endpoint is explicitly an internal admin/debugging tool operated
// directly by Alex on the private VLAN, not part of the public
// read API's privacy contract, and the whole point of this
// endpoint is giving him the real, current, post-probe node state
// to confirm things like a dual-stack merge actually happened.
type pollNowResponse struct {
	Node  storage.Node `json:"node"`
	Error string       `json:"error,omitempty"`
}

// handlePollNow forces a synchronous health-check probe of an
// address, bypassing the discovery/registry pipeline's usual async
// kickoff and review queue entirely. Deliberately does NOT call
// validateSubmittedHost — this is an internal admin-only endpoint
// (private VLAN, same trust model as the rest of the internal API),
// and an admin must be able to force-poll local/private-network
// addresses for testing. Contrast with handleCreateNode's SSRF check
// above: that check exists because POST /nodes is reachable by
// untrusted public submitters, which is not the threat model here.
func handlePollNow(store storage.Store, grpcClient, p2pClient collector.NodeClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req pollNowRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}

		if req.Host == "" {
			writeError(w, http.StatusBadRequest, errors.New("host is required"))
			return
		}
		if req.Port < 1 || req.Port > 65535 {
			writeError(w, http.StatusBadRequest, errors.New("port must be between 1 and 65535"))
			return
		}

		address := fmt.Sprintf("%s:%d", req.Host, req.Port)

		node, err := store.UpsertDiscoveredNode(r.Context(), address, storage.DiscoverySourceP2P, nil, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// Bounded probe timeout, same convention (and constant) as
		// the async health-check kickoff elsewhere in this file —
		// r.Context() alone has no deadline of its own here, and we
		// want this synchronous call to give up eventually rather
		// than hang on an unreachable node.
		ctx, cancel := context.WithTimeout(r.Context(), asyncCheckTimeout)
		defer cancel()

		// Synchronous, not launched via `go`: the whole point of this
		// endpoint is to force a probe and report back its result
		// immediately. A probe failure (unreachable node) is an
		// expected, useful result to report, not a fatal handler
		// error — PollOnce already records reachability failures as
		// rows, not via this returned error (see its doc comment), so
		// we don't abort on it; we still want to show the resulting
		// node state either way.
		pollErr := collector.PollOnce(ctx, grpcClient, p2pClient, store, node)

		updated, err := store.GetNode(ctx, node.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		var errStr string
		if pollErr != nil {
			errStr = pollErr.Error()
		}
		writeJSON(w, http.StatusOK, pollNowResponse{Node: updated, Error: errStr})
	}
}

func handleListSubmissions(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")

		submissions, err := store.ListPendingSubmissions(r.Context(), status)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, submissions)
	}
}

// approveSubmissionResponse is the POST /submissions/{id}/approve response
// body: the resulting promoted node alongside the now-approved
// submission.
type approveSubmissionResponse struct {
	Node       storage.Node              `json:"node"`
	Submission storage.PendingSubmission `json:"submission"`
}

func handleApproveSubmission(store storage.Store, grpcClient, p2pClient collector.NodeClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid submission id"))
			return
		}

		submission, err := store.GetPendingSubmission(r.Context(), id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				writeError(w, http.StatusNotFound, errors.New("submission not found"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if submission.Status != storage.SubmissionStatusPending {
			writeError(w, http.StatusConflict, fmt.Errorf("submission already reviewed (status=%s)", submission.Status))
			return
		}

		// Re-check opt-in status now, in case it changed since the
		// submission was originally queued (e.g. another submission for
		// the same address was approved in the meantime). If the
		// address has since become publicly opted-in, this approval is
		// rejected without mutating pending_submissions — it's left
		// pending so a human makes an explicit decision (reject it with
		// an accurate reason, since auto-rejecting on the reviewer's
		// behalf would hide why) rather than this silently flipping to
		// rejected on its own.
		optedIn, err := store.IsAddressPubliclyOptedIn(r.Context(), submission.Address)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if optedIn {
			writeError(w, http.StatusConflict, errors.New("this address has since become publicly registered — reject this submission instead of approving it"))
			return
		}

		tags := map[string]any{}
		if submission.OwnerTag != nil && *submission.OwnerTag != "" {
			tags["owner"] = *submission.OwnerTag
		}

		node, err := store.UpsertDiscoveredNode(r.Context(), submission.Address, storage.DiscoverySourceRegistry, tags, submission.Label)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if err := store.ApprovePendingSubmission(r.Context(), id, node.ID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		submission.Status = storage.SubmissionStatusApproved
		submission.PromotedNodeID = &node.ID

		// Kick off an async health check, same pattern (and reasoning)
		// as the old handleCreateNode used to, just deferred to
		// approval time now.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), asyncCheckTimeout)
			defer cancel()
			_ = collector.PollOnce(ctx, grpcClient, p2pClient, store, node)
		}()

		writeJSON(w, http.StatusOK, approveSubmissionResponse{Node: node, Submission: submission})
	}
}

// rejectSubmissionRequest is the optional POST /submissions/{id}/reject
// request body.
type rejectSubmissionRequest struct {
	Reason string `json:"reason"`
}

func handleRejectSubmission(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid submission id"))
			return
		}

		var req rejectSubmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}

		submission, err := store.GetPendingSubmission(r.Context(), id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				writeError(w, http.StatusNotFound, errors.New("submission not found"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if submission.Status != storage.SubmissionStatusPending {
			writeError(w, http.StatusConflict, fmt.Errorf("submission already reviewed (status=%s)", submission.Status))
			return
		}

		var reason *string
		if req.Reason != "" {
			reason = &req.Reason
		}
		if err := store.RejectPendingSubmission(r.Context(), id, reason); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		submission.Status = storage.SubmissionStatusRejected
		submission.RejectionReason = reason

		writeJSON(w, http.StatusOK, submission)
	}
}

func handleGetNode(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid node id"))
			return
		}

		node, err := store.GetNode(r.Context(), id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				writeError(w, http.StatusNotFound, errors.New("node not found"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		addrs, err := store.ListNodeAddresses(r.Context(), node.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, ScrubNode(node, addrs))
	}
}

func handleGetNodeHistory(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid node id"))
			return
		}

		limit := 20
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				writeError(w, http.StatusBadRequest, errors.New("invalid limit"))
				return
			}
			limit = n
		}

		history, err := store.GetNodeHistory(r.Context(), id, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, history)
	}
}

// topologyResponse is the GET /topology response body.
type topologyResponse struct {
	Nodes []PublicNode       `json:"nodes"`
	Edges []storage.PeerEdge `json:"edges"`
}

func handleTopology(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodes, edges, err := store.ListTopology(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		public := make([]PublicNode, len(nodes))
		for i, n := range nodes {
			addrs, err := store.ListNodeAddresses(r.Context(), n.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			public[i] = ScrubNode(n, addrs)
		}
		// edges only carry from_node_id/to_node_id UUIDs (see
		// storage.PeerEdge) — no address data, so no scrubbing needed.
		writeJSON(w, http.StatusOK, topologyResponse{Nodes: public, Edges: edges})
	}
}

func handleTopPeeredNodes(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		since := time.Hour
		if v := r.URL.Query().Get("since"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, errors.New("invalid since"))
				return
			}
			since = d
		}

		limit := 20
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				writeError(w, http.StatusBadRequest, errors.New("invalid limit"))
				return
			}
			limit = n
		}

		degrees, err := store.TopPeeredNodes(r.Context(), time.Now().Add(-since), limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		public := make([]PublicNodeDegree, len(degrees))
		for i, d := range degrees {
			pd, err := ScrubNodeDegree(r.Context(), store, d)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			public[i] = pd
		}
		writeJSON(w, http.StatusOK, public)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
