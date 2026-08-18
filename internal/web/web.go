// Package web serves the go-tari-netmap dashboard: a server-rendered htmx +
// Go html/template UI. No separate JS framework, no frontend build step.
// Public, no auth gate by design.
package web

import (
	"embed"
	"encoding/hex"
	"errors"
	"html/template"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/api"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

//go:embed templates/*.tmpl
var templatesFS embed.FS

//go:embed static/*.css
var staticFS embed.FS

// shortHexLen is the number of hex characters of a confirmed node's
// PublicKey shown inline in tables/headers before the "…" ellipsis. The
// full hex is still available via the identity's FullHex field for a
// title="" tooltip.
const shortHexLen = 12

// identity is the privacy-aware, pubkey-first display identity of a node:
// never the raw address. A node with no confirmed PublicKey yet
// (Confirmed == false) has empty ShortHex/FullHex and is rendered as an
// "unconfirmed" badge by the templates instead.
type identity struct {
	Confirmed bool
	ShortHex  string
	FullHex   string
}

// buildIdentity derives an identity from a node's raw PublicKey bytes.
func buildIdentity(publicKey []byte) identity {
	if len(publicKey) == 0 {
		return identity{}
	}
	full := hex.EncodeToString(publicKey)
	short := full
	if len(short) > shortHexLen {
		short = short[:shortHexLen] + "…"
	}
	return identity{Confirmed: true, ShortHex: short, FullHex: full}
}

// capabilities mirrors api.PublicNode's HasIPv4/HasIPv6/HasOnion booleans:
// what transports a node is known to have, without revealing any real
// address string unless the node has opted in (see PublicAddress on the
// row/detail types below).
type capabilities struct {
	HasIPv4  bool
	HasIPv6  bool
	HasOnion bool
}

// scrubbedView bundles the three privacy-scrubbed fields every node
// display type in this package needs, derived from api.ScrubNode — the
// same single source of truth the JSON API uses to decide whether a real
// address string is safe to show (see internal/api/privacy.go's doc
// comment on PublicNode). Reusing ScrubNode here, rather than
// reimplementing the classification rule, is what guarantees the web
// layer can never accidentally show a p2p_discovered node's raw address.
type scrubbedView struct {
	Identity      identity
	Capabilities  capabilities
	PublicAddress *string
}

// scrubForDisplay builds a scrubbedView for n given its known addresses
// addrs (see api.ScrubNode's doc comment for addrs' semantics — nil/empty
// is fine and falls back to n.Address).
func scrubForDisplay(n storage.Node, addrs []storage.NodeAddress) scrubbedView {
	pn := api.ScrubNode(n, addrs)
	return scrubbedView{
		Identity:      buildIdentity(pn.PublicKey),
		Capabilities:  capabilities{HasIPv4: pn.HasIPv4, HasIPv6: pn.HasIPv6, HasOnion: pn.HasOnion},
		PublicAddress: pn.Address,
	}
}

// dashboardCounts summarizes the node population by discovery source and
// by privacy-relevant capability for the dashboard header.
type dashboardCounts struct {
	Total    int
	P2P      int
	Registry int
	Both     int

	Confirmed    int
	Unconfirmed  int
	OnionCapable int
	ClearnetOnly int
}

// dashboardNodeRow is one row of the dashboard's node table, combining node
// data with its most recent health-check result (if any) and its
// privacy-scrubbed display fields.
type dashboardNodeRow struct {
	storage.Node
	LatestHealth *storage.HealthCheck

	Identity      identity
	Capabilities  capabilities
	PublicAddress *string
}

// topPeeredRow is one row of the dashboard's "top peered" panel: a node's
// privacy-scrubbed identity plus its connectivity stats from
// storage.TopPeeredNodes.
type topPeeredRow struct {
	ID            uuid.UUID
	Identity      identity
	Capabilities  capabilities
	PublicAddress *string
	Degree        int
	InDegree      int
	OutDegree     int
}

// dashboardData is the template data for index.html.tmpl.
type dashboardData struct {
	Counts                 dashboardCounts
	Nodes                  []dashboardNodeRow
	NetworkHeight          *int64
	NetworkHeightNodeCount int
	TopPeered              []topPeeredRow
}

// peerEdgeRow is one row of a node detail page's "peer connections" table:
// the OTHER node's privacy-scrubbed identity (never an address), the
// direction of the observed edge relative to the page's node, and when it
// was first/last observed.
type peerEdgeRow struct {
	OtherNodeID   uuid.UUID
	OtherIdentity identity
	Direction     string
	FirstSeen     time.Time
	LastSeen      time.Time
}

// nodeDetailData is the template data for node_detail.html.tmpl.
type nodeDetailData struct {
	Node    storage.Node
	History []storage.HealthCheck

	Identity      identity
	Capabilities  capabilities
	PublicAddress *string
	PeerEdges     []peerEdgeRow
}

// topPeeredWindow is the lookback window used to compute the dashboard's
// "top peered" panel: nodes with the most distinct peers observed within
// the last hour.
const topPeeredWindow = 1 * time.Hour

// topPeeredLimit caps the number of rows shown in the dashboard's "top
// peered" panel.
const topPeeredLimit = 10

// nodeEdgesLimit caps the number of rows shown in a node detail page's
// "peer connections" table.
const nodeEdgesLimit = 50

// NewHandler returns an http.Handler serving the dashboard: a homepage
// summary + node table (with a registry-submission form posted via htmx
// directly to the /api/nodes JSON API), a per-node detail/history page,
// and the static CSS stylesheet backing both.
func NewHandler(store storage.Store) (http.Handler, error) {
	// derefBool is registered as a template func because Go's
	// text/template `{{if}}` truth test on a pointer only checks
	// non-nil-ness, not the pointed-to value — a *bool pointing at
	// false would otherwise render as "true" in
	// submissions.html.tmpl's probe-result cell. Used to distinguish
	// nil (not yet probed) from a genuine false (unreachable).
	tmpl, err := template.New("web").Funcs(template.FuncMap{
		"derefBool": func(b *bool) bool { return b != nil && *b },
	}).ParseFS(templatesFS, "templates/*.tmpl")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", handleDashboard(tmpl, store))
	mux.HandleFunc("GET /nodes/{id}", handleNodeDetail(tmpl, store))
	mux.HandleFunc("GET /topology", handleTopologyGraph(tmpl))
	mux.HandleFunc("GET /submissions", handleSubmissions(tmpl, store))
	mux.HandleFunc("GET /static/style.css", handleStaticCSS)
	return mux, nil
}

func handleStaticCSS(w http.ResponseWriter, r *http.Request) {
	data, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Write(data)
}

func handleDashboard(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		nodes, err := store.ListNodes(ctx, storage.NodeFilter{})
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

			view := scrubForDisplay(n, nil)
			if view.Identity.Confirmed {
				data.Counts.Confirmed++
			} else {
				data.Counts.Unconfirmed++
			}
			if view.Capabilities.HasOnion {
				data.Counts.OnionCapable++
			}
			if (view.Capabilities.HasIPv4 || view.Capabilities.HasIPv6) && !view.Capabilities.HasOnion {
				data.Counts.ClearnetOnly++
			}

			row := dashboardNodeRow{
				Node:          n,
				Identity:      view.Identity,
				Capabilities:  view.Capabilities,
				PublicAddress: view.PublicAddress,
			}
			history, err := store.GetNodeHistory(ctx, n.ID, 1)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(history) > 0 {
				row.LatestHealth = &history[0]
			}
			data.Nodes = append(data.Nodes, row)
		}

		height, heightNodeCount, err := store.NetworkHeight(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data.NetworkHeight = height
		data.NetworkHeightNodeCount = heightNodeCount

		degrees, err := store.TopPeeredNodes(ctx, time.Now().Add(-topPeeredWindow), topPeeredLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, nd := range degrees {
			n, err := store.GetNode(ctx, nd.NodeID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			addrs, err := store.ListNodeAddresses(ctx, nd.NodeID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			view := scrubForDisplay(n, addrs)
			data.TopPeered = append(data.TopPeered, topPeeredRow{
				ID:            nd.NodeID,
				Identity:      view.Identity,
				Capabilities:  view.Capabilities,
				PublicAddress: view.PublicAddress,
				Degree:        nd.Degree,
				InDegree:      nd.InDegree,
				OutDegree:     nd.OutDegree,
			})
		}

		if err := tmpl.ExecuteTemplate(w, "index.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// handleTopologyGraph serves the visual topology graph page. It is a
// near-static handler: it renders the page shell only, with no node/edge
// data passed in as Go template values. All graph data is fetched
// client-side from the already-scrubbed GET /api/topology JSON endpoint
// (see topology.html.tmpl's inline <script>) — this guarantees the graph
// can never bypass the one place the privacy contract (no raw addresses
// for p2p_discovered nodes) is enforced, api.ScrubNode.
func handleTopologyGraph(tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := tmpl.ExecuteTemplate(w, "topology.html.tmpl", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// submissionsData is the template data for submissions.html.tmpl.
type submissionsData struct {
	Submissions []storage.PendingSubmission
}

// handleSubmissions serves the human-facing submission review page: a
// server-rendered (Go html/template, not client-side JS) table of pending
// submissions, with Approve/Reject buttons that post directly to the JSON
// /api/submissions/{id}/approve and /reject endpoints via htmx. This is a
// distinct route from the JSON GET /api/submissions endpoint registered
// in internal/api — same path, different mux, following this package's
// established web-vs-JSON-API split (see NewHandler's doc comment).
func handleSubmissions(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		submissions, err := store.ListPendingSubmissions(r.Context(), "pending")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := submissionsData{Submissions: submissions}
		if err := tmpl.ExecuteTemplate(w, "submissions.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func handleNodeDetail(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid node id", http.StatusBadRequest)
			return
		}

		node, err := store.GetNode(ctx, id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		addrs, err := store.ListNodeAddresses(ctx, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view := scrubForDisplay(node, addrs)

		history, err := store.GetNodeHistory(ctx, id, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		edges, err := store.ListNodeEdges(ctx, id, nodeEdgesLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := nodeDetailData{
			Node:          node,
			History:       history,
			Identity:      view.Identity,
			Capabilities:  view.Capabilities,
			PublicAddress: view.PublicAddress,
		}

		for _, e := range edges {
			otherID := e.ToNodeID
			direction := "outbound"
			if e.FromNodeID != id {
				otherID = e.FromNodeID
				direction = "inbound"
			}

			otherNode, err := store.GetNode(ctx, otherID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			data.PeerEdges = append(data.PeerEdges, peerEdgeRow{
				OtherNodeID:   otherID,
				OtherIdentity: buildIdentity(otherNode.PublicKey),
				Direction:     direction,
				FirstSeen:     e.FirstSeen,
				LastSeen:      e.LastSeen,
			})
		}

		if err := tmpl.ExecuteTemplate(w, "node_detail.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
