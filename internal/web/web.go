// Package web serves the go-tari-netmap dashboard: a server-rendered htmx +
// Go html/template UI. This is currently a minimal placeholder — real
// dashboard views land in the follow-up implementation pass.
package web

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/*.tmpl
var templatesFS embed.FS

// NewHandler returns an http.Handler serving the dashboard placeholder at
// GET /.
func NewHandler() (http.Handler, error) {
	tmpl, err := template.ParseFS(templatesFS, "templates/*.tmpl")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := tmpl.ExecuteTemplate(w, "index.html.tmpl", nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return mux, nil
}
