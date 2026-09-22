// Package docs embeds this repo's checked-in OpenAPI 3.0 spec (openapi.yaml,
// alongside this file) so internal/api can serve it live (GET /api/spec, after
// cmd/netmap/main.go's /api StripPrefix mount) without reading it off disk at
// runtime — same //go:embed convention internal/web/web.go already uses for its
// own templates/*.tmpl and static/*.css.
package docs

import _ "embed"

// OpenAPISpecYAML is the raw contents of openapi.yaml, embedded at build time. Kept
// as a package-level []byte (rather than a string) since callers (internal/api's
// GET /spec handler) write it directly to an http.ResponseWriter.
//
//go:embed openapi.yaml
var OpenAPISpecYAML []byte
