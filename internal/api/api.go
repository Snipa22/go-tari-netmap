// Package api exposes an HTTP API for topology/health data: node registry
// submission and listing, per-node health history, and the topology graph.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// NewRouter returns the HTTP handler for the API. store persists topology
// and health data; client is used to kick off an async, non-blocking health
// check when a new node is registered via POST /nodes.
func NewRouter(store storage.Store, client collector.NodeClient) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /nodes", handleListNodes(store))
	mux.HandleFunc("POST /nodes", handleCreateNode(store, client))
	mux.HandleFunc("GET /nodes/{id}", handleGetNode(store))
	mux.HandleFunc("GET /nodes/{id}/history", handleGetNodeHistory(store))
	mux.HandleFunc("GET /topology", handleTopology(store))

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
		writeJSON(w, http.StatusOK, nodes)
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

func handleCreateNode(store storage.Store, client collector.NodeClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createNodeRequest
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

		tags := map[string]any{}
		if req.OwnerTag != "" {
			tags["owner"] = req.OwnerTag
		}

		var label *string
		if req.Label != "" {
			label = &req.Label
		}

		address := fmt.Sprintf("%s:%d", req.Host, req.Port)
		node, err := store.UpsertNode(r.Context(), storage.NodeInput{
			Address:         address,
			DiscoverySource: storage.DiscoverySourceRegistry,
			Tags:            tags,
			Label:           label,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// Kick off an async health check so newly submitted nodes get
		// checked promptly rather than waiting for the collector's next
		// full poll cycle. This is intentionally non-blocking: the POST
		// response below does not wait on the network call, and there is
		// no request left to report the outcome to, so errors are
		// swallowed after logging is left to collector.PollOnce's own
		// RecordHealthCheck call (an unreachable result is still
		// recorded).
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), asyncCheckTimeout)
			defer cancel()
			_ = collector.PollOnce(ctx, client, store, node)
		}()

		writeJSON(w, http.StatusCreated, node)
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
		writeJSON(w, http.StatusOK, node)
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
	Nodes []storage.Node     `json:"nodes"`
	Edges []storage.PeerEdge `json:"edges"`
}

func handleTopology(store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodes, edges, err := store.ListTopology(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, topologyResponse{Nodes: nodes, Edges: edges})
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
