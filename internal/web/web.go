// Package web serves the go-tari-netmap dashboard: a server-rendered htmx +
// Go html/template UI. No separate JS framework, no frontend build step.
// Public, no auth gate by design.
package web

import (
	"embed"
	"errors"
	"html/template"
	"net/http"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

//go:embed templates/*.tmpl
var templatesFS embed.FS

// dashboardCounts summarizes the node population by discovery source for
// the dashboard header.
type dashboardCounts struct {
	Total    int
	P2P      int
	Registry int
	Both     int
}

// dashboardNodeRow is one row of the dashboard's node table, combining node
// data with its most recent health-check result (if any).
type dashboardNodeRow struct {
	storage.Node
	LatestHealth *storage.HealthCheck
}

// dashboardData is the template data for index.html.tmpl.
type dashboardData struct {
	Counts dashboardCounts
	Nodes  []dashboardNodeRow
}

// nodeDetailData is the template data for node_detail.html.tmpl.
type nodeDetailData struct {
	Node    storage.Node
	History []storage.HealthCheck
}

// NewHandler returns an http.Handler serving the dashboard: a homepage
// summary + node table (with a registry-submission form posted via htmx
// directly to the /api/nodes JSON API), and a per-node detail/history page.
func NewHandler(store storage.Store) (http.Handler, error) {
	tmpl, err := template.ParseFS(templatesFS, "templates/*.tmpl")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", handleDashboard(tmpl, store))
	mux.HandleFunc("GET /nodes/{id}", handleNodeDetail(tmpl, store))
	return mux, nil
}

func handleDashboard(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodes, err := store.ListNodes(r.Context(), storage.NodeFilter{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := dashboardData{}
		for _, n := range nodes {
			data.Counts.Total++
			switch n.DiscoverySource {
			case storage.DiscoverySourceP2P:
				data.Counts.P2P++
			case storage.DiscoverySourceRegistry:
				data.Counts.Registry++
			case storage.DiscoverySourceBoth:
				data.Counts.Both++
			}

			row := dashboardNodeRow{Node: n}
			history, err := store.GetNodeHistory(r.Context(), n.ID, 1)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(history) > 0 {
				row.LatestHealth = &history[0]
			}
			data.Nodes = append(data.Nodes, row)
		}

		if err := tmpl.ExecuteTemplate(w, "index.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func handleNodeDetail(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid node id", http.StatusBadRequest)
			return
		}

		node, err := store.GetNode(r.Context(), id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		history, err := store.GetNodeHistory(r.Context(), id, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := nodeDetailData{Node: node, History: history}
		if err := tmpl.ExecuteTemplate(w, "node_detail.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
