package api

import (
	"context"
	"crypto/subtle"
	"net/http"
)

// collectorKeyHeader is the request header POST /internal/collectors/report (see
// collector_report.go) authenticates against -- a single, simple, per-collector API key,
// checked against an operator-configured map of collector_name -> CollectorConfig (see
// NewRouter's collectors parameter, sourced from NETMAP_COLLECTOR_KEYS/
// NETMAP_COLLECTOR_SELF_ADDRESSES in cmd/netmap/main.go). This is a deliberately simpler trust
// model than adminauth.Credentials' single username/password pair behind HTTP Basic Auth:
// multiple named collectors, each with its own key, writing directly with no review queue --
// a single-operator (Alex) trusted channel, not a public submission endpoint.
const collectorKeyHeader = "X-Collector-Key"

// CollectorConfig is one entry of NewRouter's collectors parameter: a single trusted remote
// collector satellite's own API key (see collectorKeyHeader above) plus the address(es) it is
// allowed to self-identify as via a report's self_identity field.
//
// SelfAddresses anchors self_identity tagging (tags.role = "collector", see
// collector_report.go's applyCollectorReport) to addresses the OPERATOR has independently
// configured as this specific collector's own -- e.g. the exact same address(es) configured on
// that satellite's own deployment via cmd/netmap-p2p-responder's -public-tcp-addr/-onion3-addr
// flags (see that binary's selfAdvertisedAddresses helper for the matching wire-format
// conversion). Without this, self_identity tagging is otherwise completely unconstrained: any
// key holder could tag ANY address role=collector, permanently suppressing it from every
// recommendation route (GET /topology, GET /topology/top-peered, GET /nodes/seeds, etc.) --
// see this repo's readiness-review follow-up, Fix 6(c). A self_identity address a collector's
// own CollectorConfig.SelfAddresses doesn't list is rejected (see applyCollectorReport), not
// silently ignored -- the whole batch still applies, but that one self_identity tagging
// attempt is dropped and logged as a heads-up.
type CollectorConfig struct {
	APIKey        string
	SelfAddresses []string
}

// authenticateCollector validates r's X-Collector-Key header against every key in collectors,
// returning the matching collector's name and true on success. Every configured key is
// compared via crypto/subtle.ConstantTimeCompare (never plain string ==, mirroring
// adminauth.basicAuthMiddleware's discipline) and, for the same timing-side-channel reason,
// every entry is compared unconditionally rather than short-circuiting on the first match --
// response timing must not leak how close an invalid key came to matching any configured
// collector. An empty header value never matches (a configured key must never itself be the
// empty string in practice, but this guards against that degenerate case regardless).
func authenticateCollector(collectors map[string]CollectorConfig, r *http.Request) (name string, ok bool) {
	provided := r.Header.Get(collectorKeyHeader)
	if provided == "" {
		return "", false
	}

	for candidateName, cfg := range collectors {
		if cfg.APIKey == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(cfg.APIKey)) == 1 {
			name, ok = candidateName, true
		}
	}
	return name, ok
}

// collectorContext carries the authenticated collector's identity (see authenticateCollector)
// from wrapCollectorAuth's middleware down to handleCollectorReport/applyCollectorReport --
// see this repo's readiness-review follow-up, Fix 4 (collector name -> reported_by_collector
// attribution) and Fix 6(c) (SelfAddresses -> self_identity tagging's allowlist). Previously
// (before that follow-up) the authenticated name was resolved and then immediately discarded
// by wrapCollectorAuth, with no way for the handler underneath to ever learn it.
type collectorContext struct {
	Name          string
	SelfAddresses []string
}

type collectorContextKey struct{}

// collectorFromContext extracts the collectorContext wrapCollectorAuth attaches to an
// authenticated request, for handleCollectorReport/applyCollectorReport to consume. The zero
// value (ok == false) should never actually occur on a request that reached
// handleCollectorReport, since wrapCollectorAuth always attaches one before calling next --
// but callers should still treat a missing value as "no collector identity", not panic, in
// case that invariant is ever violated by a future change.
func collectorFromContext(ctx context.Context) (collectorContext, bool) {
	cc, ok := ctx.Value(collectorContextKey{}).(collectorContext)
	return cc, ok
}

// wrapCollectorAuth gates next behind the X-Collector-Key check against collectors (see
// authenticateCollector). If collectors is empty (NETMAP_COLLECTOR_KEYS unset/unconfigured),
// the returned handler ignores next entirely and unconditionally responds 503 Service
// Unavailable to every request -- mirroring adminauth.Wrap's fail-closed behavior when
// adminauth.Credentials isn't fully configured: this trusted-collector ingestion channel must
// never silently accept every request just because the operator forgot to set the env var.
//
// On success, the matched collector's identity (name + configured SelfAddresses) is attached
// to the request's context (see collectorContext/collectorFromContext above) so
// handleCollectorReport/applyCollectorReport downstream can consume it -- see those two
// fixes' rationale on CollectorConfig's/collectorContext's own doc comments.
func wrapCollectorAuth(collectors map[string]CollectorConfig, next http.Handler) http.Handler {
	if len(collectors) == 0 {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "collector ingestion disabled: NETMAP_COLLECTOR_KEYS not configured", http.StatusServiceUnavailable)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, ok := authenticateCollector(collectors, r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		cc := collectorContext{Name: name, SelfAddresses: collectors[name].SelfAddresses}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), collectorContextKey{}, cc)))
	})
}
