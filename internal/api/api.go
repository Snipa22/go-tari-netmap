// Package api will expose an HTTP API for topology/health data.
//
// TODO(netmap): implement in the collector/storage/API dispatch. Real
// topology/health endpoints land in the follow-up implementation pass.
package api

import "net/http"

// NewRouter returns the HTTP handler for the API. Only a health check is
// wired up so far.
func NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
