// Command netmap is the entrypoint for the go-tari-netmap HTTP server.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/Snipa22/go-tari-netmap/internal/api"
	"github.com/Snipa22/go-tari-netmap/internal/web"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	webHandler, err := web.NewHandler()
	if err != nil {
		log.Fatalf("failed to build web handler: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", webHandler)
	mux.Handle("/api/", http.StripPrefix("/api", api.NewRouter()))

	log.Printf("go-tari-netmap listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
