// Package web serves the go-tari-netmap dashboard: a server-rendered htmx +
// Go html/template UI. No separate JS framework, no frontend build step.
// Public, no auth gate by design — except the /admin/* sub-area (the
// submission review page), which is HTTP Basic Auth-gated via
// internal/adminauth; see NewHandler.
package web

import (
	"context"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/adminauth"
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
// address string unless the node has opted in (see PublicAddresses on the
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
	Identity        identity
	Capabilities    capabilities
	PublicAddresses []string
}

// scrubForDisplay builds a scrubbedView for n given its known addresses
// addrs (see api.ScrubNode's doc comment for addrs' semantics — nil/empty
// is fine and falls back to n.Address).
func scrubForDisplay(n storage.Node, addrs []storage.NodeAddress) scrubbedView {
	pn := api.ScrubNode(n, addrs)
	return scrubbedView{
		Identity:        buildIdentity(pn.PublicKey),
		Capabilities:    capabilities{HasIPv4: pn.HasIPv4, HasIPv6: pn.HasIPv6, HasOnion: pn.HasOnion},
		PublicAddresses: pn.Addresses,
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

// dashboardNodeRow is one row of a node table (the dashboard's or
// /network's), combining node data with its most recent health-check
// result (if any) and its privacy-scrubbed display fields.
type dashboardNodeRow struct {
	storage.Node
	LatestHealth *storage.HealthCheck

	Identity        identity
	Capabilities    capabilities
	PublicAddresses []string

	// LikelyDead reuses handleNodeDetail's heuristic (see
	// computeLikelyDead) against whatever history buildNodeTableData
	// fetched for this row's historyLimit. The main dashboard passes
	// historyLimit == 1, so this is always false there (not enough
	// history to ever satisfy the >= 3 threshold) — it's only
	// meaningful on /network, which passes historyLimit == 3.
	LikelyDead bool
}

// topPeeredRow is one row of the dashboard's "top peered" panel: a node's
// privacy-scrubbed identity plus its connectivity stats from
// storage.TopPeeredNodes.
type topPeeredRow struct {
	ID                uuid.UUID
	Identity          identity
	Capabilities      capabilities
	PublicAddresses   []string
	Degree            int
	InDegree          int
	OutDegree         int
	OnionPeerCount    int
	ClearnetPeerCount int
}

// dashboardData is the template data for index.html.tmpl.
type dashboardData struct {
	Counts                 dashboardCounts
	Nodes                  []dashboardNodeRow
	NetworkHeight          *int64
	NetworkHeightNodeCount int
	TopPeered              []topPeeredRow
	TopPeeredWindowLabel   string

	// Node table pagination metadata (see handleDashboard). NodeTotal is
	// the count of the reachable-within-window filtered set that the
	// table itself is paginating through (see dashboardReachableWindow),
	// NOT Counts.Total — the table only ever shows nodes from that
	// filtered set, so pagination math must be based on its size, not
	// the whole population's. Counts.Total (the "Total nodes" summary
	// card) intentionally keeps reflecting the whole population and can
	// disagree with NodeTotal.
	NodePage       int
	NodeLimit      int
	NodeTotal      int
	NodeRangeStart int
	NodeRangeEnd   int
	HasPrevPage    bool
	HasNextPage    bool
	PrevPage       int
	NextPage       int
}

// networkData is the template data for network.html.tmpl (GET /network —
// see handleFullNetwork). Same whole-population summary counts and node
// table shape as dashboardData, but deliberately has no TopPeered/
// TopPeeredWindowLabel fields — that panel stays exclusively on the main
// dashboard.
type networkData struct {
	Counts                 dashboardCounts
	Nodes                  []dashboardNodeRow
	NetworkHeight          *int64
	NetworkHeightNodeCount int

	// Node table pagination metadata (see handleFullNetwork and
	// buildNodeTableData). Unlike dashboardData's NodeTotal, this IS
	// the same as Counts.Total — /network's table has no
	// ReachableSince filter, so it's paginating through the whole
	// population.
	NodePage       int
	NodeLimit      int
	NodeTotal      int
	NodeRangeStart int
	NodeRangeEnd   int
	HasPrevPage    bool
	HasNextPage    bool
	PrevPage       int
	NextPage       int
}

// nodeTablePagination is the pagination metadata buildNodeTableData
// computes for a paginated node table — the shared shape copied into
// both dashboardData's and networkData's flat Node*/HasPrevPage/etc.
// fields by their respective handlers.
type nodeTablePagination struct {
	Page       int
	Limit      int
	Total      int
	RangeStart int
	RangeEnd   int

	HasPrevPage bool
	HasNextPage bool
	PrevPage    int
	NextPage    int
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

	Identity        identity
	Capabilities    capabilities
	PublicAddresses []string
	PeerEdges       []peerEdgeRow

	// LikelyDead is a simple, deliberately non-aggregate-query heuristic
	// computed from the already-fetched History slice — see
	// computeLikelyDead for exactly how it's derived.
	LikelyDead bool
}

// topPeeredWindow is the lookback window used to compute the dashboard's
// "top peered" panel: nodes with the most distinct peers observed within
// the last hour.
const topPeeredWindow = 1 * time.Hour

// topPeeredLimit caps the number of rows shown in the dashboard's "top
// peered" panel.
const topPeeredLimit = 10

// dashboardNodePageSize is the default page size for the dashboard's node
// table (`?page=` is 1-indexed; offset = (page-1)*limit).
const dashboardNodePageSize = 50

// dashboardNodeMaxPageSize caps a caller-supplied `?limit=` on the
// dashboard's node table.
const dashboardNodeMaxPageSize = 200

// nodeEdgesLimit caps the number of rows shown in a node detail page's
// "peer connections" table.
const nodeEdgesLimit = 50

// dashboardRowHistoryLimit is the number of history rows
// buildNodeTableData fetches per row on the main dashboard — just
// enough for LatestHealth, and deliberately too few (< 3) for
// computeLikelyDead to ever flag a row true there (see
// dashboardNodeRow.LikelyDead's doc comment).
const dashboardRowHistoryLimit = 1

// networkRowHistoryLimit is the number of history rows
// buildNodeTableData fetches per row on /network — enough to also
// compute computeLikelyDead's "3+ probes, zero successes" heuristic per
// row, at the cost of a slightly larger per-row GetNodeHistory call
// (still one query per visible row, same as the dashboard's, just with
// a bigger LIMIT).
const networkRowHistoryLimit = 3

// dashboardReachableWindow is the display-layer cutoff for "reachable
// recently enough to show in the main Nodes table" on the dashboard. It
// only affects what's rendered on this one page — nodes outside this
// window stay in the database and keep being probed by the collector
// exactly as before; they just aren't shown in the main table until
// they're reachable again. The whole-population summary cards
// (dashboardCounts) are deliberately unaffected by this window (see
// handleDashboard).
const dashboardReachableWindow = 24 * time.Hour

// windowLabel formats a lookback duration like topPeeredWindow into a
// short human string (e.g. "(last 1h)") for display next to a panel
// header, so the label stays in sync with the constant instead of being
// hardcoded separately. Unlike time.Duration.String() (which would
// render 1*time.Hour as "1h0m0s"), this drops zero-valued trailing
// units.
func windowLabel(d time.Duration) string {
	return "(last " + formatDuration(d) + ")"
}

// formatDuration renders d as a compact "1h", "30m", "1h30m"-style
// string, omitting any zero-valued hours/minutes/seconds components.
func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	out := ""
	if h > 0 {
		out += fmt.Sprintf("%dh", h)
	}
	if m > 0 {
		out += fmt.Sprintf("%dm", m)
	}
	if s > 0 {
		out += fmt.Sprintf("%ds", s)
	}
	return out
}

// computeLikelyDead applies the "3+ probes, zero successes" heuristic
// shared by handleNodeDetail's nodeDetailData.LikelyDead and
// buildNodeTableData's per-row dashboardNodeRow.LikelyDead: a node with
// at least 3 recorded health checks in history and not a single
// Reachable == true among them is overwhelmingly likely to be
// permanently gone rather than just having a bad day — in production,
// 83% of nodes probed 3+ times never once succeed, so this is a
// deliberately simple, well-grounded proxy for "persistently dead", not
// a guess. Fewer than 3 history entries is never enough to conclude
// anything, so it always returns false in that case.
func computeLikelyDead(history []storage.HealthCheck) bool {
	if len(history) < 3 {
		return false
	}
	for _, h := range history {
		if h.Reachable {
			return false
		}
	}
	return true
}

// buildNodeTableData fetches the whole-population dashboardCounts
// (Total/P2P/Registry/Both/Confirmed/Unconfirmed/OnionCapable/
// ClearnetOnly — always unfiltered, regardless of reachableSince) plus a
// paginated page of dashboardNodeRow for a node table. reachableSince
// nil means no filter (the full node population, for /network);
// non-nil applies that cutoff (the main dashboard's
// dashboardReachableWindow liveness view). page/limit/offset drive the
// SQL-level LIMIT/OFFSET of the paginated page; historyLimit controls
// how many HealthCheck rows are fetched per row (see
// dashboardRowHistoryLimit/networkRowHistoryLimit) — LatestHealth only
// ever needs the most recent one, but computeLikelyDead needs at least
// 3 to ever return true.
//
// This is shared, extract-don't-change logic behind both
// handleDashboard and handleFullNetwork: same whole-population counts
// query, same per-row scrub+history+pagination-math shape, differing
// only in the ReachableSince filter and historyLimit each passes in.
func buildNodeTableData(ctx context.Context, store storage.Store, reachableSince *time.Time, page, limit, offset, historyLimit int) (dashboardCounts, []dashboardNodeRow, nodeTablePagination, error) {
	var counts dashboardCounts
	var pagination nodeTablePagination

	// Unpaginated: dashboardCounts must reflect the WHOLE population,
	// never just the current page or any ReachableSince filter. This is
	// a separate query from the paginated one used for the rows below,
	// deliberately NOT reusing its LIMIT/OFFSET/ReachableSince.
	allNodes, err := store.ListNodes(ctx, storage.NodeFilter{})
	if err != nil {
		return counts, nil, pagination, err
	}

	allNodeIDs := make([]uuid.UUID, len(allNodes))
	for i, n := range allNodes {
		allNodeIDs[i] = n.ID
	}
	// ONE batch call for the whole node set — covers both this counts
	// pass and the paginated table rows below (its keys are a superset
	// of the paginated page's node IDs), so neither reintroduces the
	// old per-node ListNodeAddresses N+1.
	addrsByNode, err := store.ListNodeAddressesForNodes(ctx, allNodeIDs)
	if err != nil {
		return counts, nil, pagination, err
	}

	for _, n := range allNodes {
		counts.Total++
		switch n.DiscoverySource {
		case storage.DiscoverySourceP2P:
			counts.P2P++
		case storage.DiscoverySourceRegistry:
			counts.Registry++
		case storage.DiscoverySourceBoth:
			counts.Both++
		}

		view := scrubForDisplay(n, addrsByNode[n.ID])
		if view.Identity.Confirmed {
			counts.Confirmed++
		} else {
			counts.Unconfirmed++
		}
		if view.Capabilities.HasOnion {
			counts.OnionCapable++
		}
		if (view.Capabilities.HasIPv4 || view.Capabilities.HasIPv6) && !view.Capabilities.HasOnion {
			counts.ClearnetOnly++
		}
	}

	// Paginated: the actual SQL-level LIMIT/OFFSET query backing the
	// node table rows shown on this page, restricted to reachableSince
	// if non-nil. This is a display-only cut when non-nil — the
	// whole-population counts above are deliberately built from the
	// unfiltered allNodes query and never use this filter.
	pageNodes, err := store.ListNodes(ctx, storage.NodeFilter{ReachableSince: reachableSince, Limit: limit, Offset: offset})
	if err != nil {
		return counts, nil, pagination, err
	}

	var rows []dashboardNodeRow
	for _, n := range pageNodes {
		view := scrubForDisplay(n, addrsByNode[n.ID])
		row := dashboardNodeRow{
			Node:            n,
			Identity:        view.Identity,
			Capabilities:    view.Capabilities,
			PublicAddresses: view.PublicAddresses,
		}
		history, err := store.GetNodeHistory(ctx, n.ID, historyLimit)
		if err != nil {
			return counts, nil, pagination, err
		}
		if len(history) > 0 {
			row.LatestHealth = &history[0]
		}
		row.LikelyDead = computeLikelyDead(history)
		rows = append(rows, row)
	}

	// filteredTotal is the size of the SAME reachableSince-filtered set
	// pageNodes is a page of, used only for its len() as pagination
	// metadata below. When reachableSince is nil, that set IS the whole
	// population, so counts.Total (already computed above) can be
	// reused directly instead of firing a second identical query.
	filteredTotal := counts.Total
	if reachableSince != nil {
		filteredNodes, err := store.ListNodes(ctx, storage.NodeFilter{ReachableSince: reachableSince})
		if err != nil {
			return counts, nil, pagination, err
		}
		filteredTotal = len(filteredNodes)
	}

	pagination.Page = page
	pagination.Limit = limit
	pagination.Total = filteredTotal
	if len(pageNodes) > 0 {
		pagination.RangeStart = offset + 1
		pagination.RangeEnd = offset + len(pageNodes)
	}
	pagination.HasPrevPage = page > 1
	pagination.PrevPage = page - 1
	pagination.HasNextPage = offset+len(pageNodes) < filteredTotal
	pagination.NextPage = page + 1

	return counts, rows, pagination, nil
}

// parseNodeTablePageParams parses the `?page=`/`?limit=` query params
// shared by both the dashboard's and /network's node tables, applying
// the same 1-indexed-page/default-page-size/max-page-size conventions
// (dashboardNodePageSize/dashboardNodeMaxPageSize) to both. Returns the
// resolved page, limit, and the SQL OFFSET derived from them.
func parseNodeTablePageParams(r *http.Request) (page, limit, offset int) {
	page = 1
	if v := r.URL.Query().Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	limit = dashboardNodePageSize
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > dashboardNodeMaxPageSize {
		limit = dashboardNodeMaxPageSize
	}
	offset = (page - 1) * limit
	return page, limit, offset
}

// NewHandler returns an http.Handler serving the dashboard: a homepage
// summary + node table (with a registry-submission form posted via htmx
// directly to the /api/nodes JSON API), a per-node detail/history page,
// the static CSS stylesheet backing both, and the HTTP Basic Auth-gated
// /admin/submissions review page. adminCreds configures that gate — see
// internal/adminauth.Wrap's doc comment for the fail-closed-503 behavior
// when adminCreds isn't fully configured.
func NewHandler(store storage.Store, adminCreds adminauth.Credentials) (http.Handler, error) {
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
	mux.HandleFunc("GET /network", handleFullNetwork(tmpl, store))
	mux.HandleFunc("GET /static/style.css", handleStaticCSS)

	// The submission review page moved under /admin/* (see this
	// package's and internal/api's NewRouter doc comments) — it's the
	// only web-layer admin route today, but registered on its own
	// sub-mux (rather than directly on the top-level mux) so the
	// adminauth.Wrap gate below covers the whole /admin/ prefix
	// uniformly, matching internal/api.NewRouter's structure.
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /admin/submissions", handleSubmissions(tmpl, store))
	mux.Handle("/admin/", adminauth.Wrap(adminCreds, adminMux))

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

		page, limit, offset := parseNodeTablePageParams(r)

		// The main dashboard's Nodes table is restricted to nodes
		// reachable within dashboardReachableWindow (see its doc
		// comment) — a display-only cut, the whole-population counts
		// buildNodeTableData returns are deliberately built from an
		// unfiltered query and don't use this cutoff at all.
		cutoff := time.Now().Add(-dashboardReachableWindow)
		counts, rows, pagination, err := buildNodeTableData(ctx, store, &cutoff, page, limit, offset, dashboardRowHistoryLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := dashboardData{
			TopPeeredWindowLabel: windowLabel(topPeeredWindow),
			Counts:               counts,
			Nodes:                rows,
			NodePage:             pagination.Page,
			NodeLimit:            pagination.Limit,
			NodeTotal:            pagination.Total,
			NodeRangeStart:       pagination.RangeStart,
			NodeRangeEnd:         pagination.RangeEnd,
			HasPrevPage:          pagination.HasPrevPage,
			HasNextPage:          pagination.HasNextPage,
			PrevPage:             pagination.PrevPage,
			NextPage:             pagination.NextPage,
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
				ID:                nd.NodeID,
				Identity:          view.Identity,
				Capabilities:      view.Capabilities,
				PublicAddresses:   view.PublicAddresses,
				Degree:            nd.Degree,
				InDegree:          nd.InDegree,
				OutDegree:         nd.OutDegree,
				OnionPeerCount:    nd.OnionPeerCount,
				ClearnetPeerCount: nd.ClearnetPeerCount,
			})
		}

		if err := tmpl.ExecuteTemplate(w, "index.html.tmpl", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// handleFullNetwork serves GET /network: the complete, unfiltered node
// population (all nodes, paginated — no ReachableSince filter), as a
// companion to handleDashboard's "/" view, which only shows nodes
// reachable within dashboardReachableWindow. This is the "show me
// everything, including likely-dead/stale nodes" view, so its
// per-row history fetch uses networkRowHistoryLimit (3, not 1) to also
// compute each row's LikelyDead badge via computeLikelyDead. It
// deliberately has no "top peered" panel — that stays exclusively on
// the main dashboard (see networkData's doc comment) — but does reuse
// the exact same whole-population summary counts and NetworkHeight
// fetch shown there.
func handleFullNetwork(tmpl *template.Template, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		page, limit, offset := parseNodeTablePageParams(r)

		// reachableSince is nil here — /network's table has no
		// liveness filter at all, unlike handleDashboard's.
		counts, rows, pagination, err := buildNodeTableData(ctx, store, nil, page, limit, offset, networkRowHistoryLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := networkData{
			Counts:         counts,
			Nodes:          rows,
			NodePage:       pagination.Page,
			NodeLimit:      pagination.Limit,
			NodeTotal:      pagination.Total,
			NodeRangeStart: pagination.RangeStart,
			NodeRangeEnd:   pagination.RangeEnd,
			HasPrevPage:    pagination.HasPrevPage,
			HasNextPage:    pagination.HasNextPage,
			PrevPage:       pagination.PrevPage,
			NextPage:       pagination.NextPage,
		}

		height, heightNodeCount, err := store.NetworkHeight(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data.NetworkHeight = height
		data.NetworkHeightNodeCount = heightNodeCount

		if err := tmpl.ExecuteTemplate(w, "network.html.tmpl", data); err != nil {
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
// /api/admin/submissions/{id}/approve and /reject endpoints via htmx.
// This is a distinct route from the JSON GET /api/admin/submissions
// endpoint registered in internal/api — same path (under /admin),
// different mux, following this package's established web-vs-JSON-API
// split (see NewHandler's doc comment). Both this page and the JSON
// endpoints it posts to sit behind the same adminauth.Wrap gate.
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
			Node:            node,
			History:         history,
			Identity:        view.Identity,
			Capabilities:    view.Capabilities,
			PublicAddresses: view.PublicAddresses,
		}

		// LikelyDead against the already-fetched 50-row history above —
		// no new aggregate query needed. See computeLikelyDead's doc
		// comment for the heuristic itself; also reused (with a much
		// shorter per-row history fetch) by buildNodeTableData for
		// /network's rows.
		data.LikelyDead = computeLikelyDead(history)

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
