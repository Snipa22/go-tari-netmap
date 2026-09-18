package api

import (
	"crypto/subtle"
	"net/http"
)

// collectorKeyHeader is the request header POST /internal/collectors/report (see
// collector_report.go) authenticates against -- a single, simple, per-collector API key,
// checked against an operator-configured map of collector_name -> api_key (see
// NewRouter's collectorKeys parameter, sourced from NETMAP_COLLECTOR_KEYS in
// cmd/netmap/main.go). This is a deliberately simpler trust model than adminauth.Credentials'
// single username/password pair behind HTTP Basic Auth: multiple named collectors, each with
// its own key, writing directly with no review queue -- a single-operator (Alex) trusted
// channel, not a public submission endpoint.
const collectorKeyHeader = "X-Collector-Key"

// authenticateCollector validates r's X-Collector-Key header against every key in keys,
// returning the matching collector's name and true on success. Every configured key is
// compared via crypto/subtle.ConstantTimeCompare (never plain string ==, mirroring
// adminauth.basicAuthMiddleware's discipline) and, for the same timing-side-channel reason,
// every entry is compared unconditionally rather than short-circuiting on the first match --
// response timing must not leak how close an invalid key came to matching any configured
// collector. An empty header value never matches (a configured key must never itself be the
// empty string in practice, but this guards against that degenerate case regardless).
func authenticateCollector(keys map[string]string, r *http.Request) (name string, ok bool) {
	provided := r.Header.Get(collectorKeyHeader)
	if provided == "" {
		return "", false
	}

	for candidateName, candidateKey := range keys {
		if candidateKey == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(candidateKey)) == 1 {
			name, ok = candidateName, true
		}
	}
	return name, ok
}

// wrapCollectorAuth gates next behind the X-Collector-Key check against keys (see
// authenticateCollector). If keys is empty (NETMAP_COLLECTOR_KEYS unset/unconfigured), the
// returned handler ignores next entirely and unconditionally responds 503 Service Unavailable
// to every request -- mirroring adminauth.Wrap's fail-closed behavior when
// adminauth.Credentials isn't fully configured: this trusted-collector ingestion channel must
// never silently accept every request just because the operator forgot to set the env var.
func wrapCollectorAuth(keys map[string]string, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "collector ingestion disabled: NETMAP_COLLECTOR_KEYS not configured", http.StatusServiceUnavailable)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authenticateCollector(keys, r); !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
