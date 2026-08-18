// Package adminauth provides the single, shared HTTP Basic Auth gate used
// by both the web (internal/web) and JSON API (internal/api) layers to
// protect their respective /admin/* areas. Factoring this into one place
// avoids duplicating the constant-time credential comparison (or the
// fail-closed-when-unconfigured behavior) in two packages with
// potentially diverging logic.
package adminauth

import (
	"crypto/subtle"
	"net/http"
)

// Credentials holds the admin username/password expected on every
// request to a /admin/* route, sourced from NETMAP_ADMIN_USER /
// NETMAP_ADMIN_PASSWORD (see cmd/netmap/main.go). A zero-value
// Credentials (either field empty) is treated as "not configured" by
// Wrap below — see Configured's doc comment.
type Credentials struct {
	Username string
	Password string
}

// Configured reports whether both Username and Password are non-empty.
// Both must be set for the admin area to be reachable at all; a
// half-configured pair (e.g. only NETMAP_ADMIN_USER set) is treated the
// same as fully unconfigured — fail closed, never fall back to a
// default/blank credential for the missing half.
func (c Credentials) Configured() bool {
	return c.Username != "" && c.Password != ""
}

// Wrap returns next protected by HTTP Basic Auth against creds.
//
// If creds isn't fully Configured, the returned handler ignores next
// entirely and unconditionally responds 503 Service Unavailable to
// every request — regardless of whether Basic Auth headers are present
// or what they contain. This is the fail-closed behavior required when
// NETMAP_ADMIN_USER/NETMAP_ADMIN_PASSWORD aren't both set: the admin
// area must be disabled outright, not silently left open or 404ing.
//
// If creds is Configured, the returned handler requires a valid
// Authorization: Basic header matching creds exactly (both username and
// password compared in constant time via crypto/subtle, never with
// plain string ==) before calling next; otherwise it responds 401 with
// a WWW-Authenticate header so browsers show a real login prompt.
func Wrap(creds Credentials, next http.Handler) http.Handler {
	if !creds.Configured() {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "admin area disabled: NETMAP_ADMIN_USER/NETMAP_ADMIN_PASSWORD not configured", http.StatusServiceUnavailable)
		})
	}
	return basicAuthMiddleware(creds.Username, creds.Password, next)
}

// basicAuthMiddleware requires HTTP Basic Auth credentials matching
// username/password exactly before calling next. Both the supplied
// username and password are compared against the expected values with
// subtle.ConstantTimeCompare (not == string comparison) to avoid a
// timing side channel; the comparison is only treated as a match when
// r.BasicAuth's ok is true AND both ConstantTimeCompare calls return 1.
// ConstantTimeCompare returns 0 (not a panic) for differing-length
// inputs, so no separate length check is needed for safety — but note
// that alone is deliberately not treated as a "safe short-circuit": both
// comparisons still run unconditionally below, rather than short-
// circuiting on the first mismatch, so response timing doesn't leak
// which of username/password was wrong.
func basicAuthMiddleware(username, password string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()

		validUser := subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1
		validPass := subtle.ConstantTimeCompare([]byte(pass), []byte(password)) == 1

		if !ok || !validUser || !validPass {
			w.Header().Set("WWW-Authenticate", `Basic realm="go-tari-netmap admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
