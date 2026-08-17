// Command netmap is the entrypoint for the go-tari-netmap HTTP server.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Snipa22/go-tari-netmap/internal/api"
	"github.com/Snipa22/go-tari-netmap/internal/collector"
	"github.com/Snipa22/go-tari-netmap/internal/storage"
	"github.com/Snipa22/go-tari-netmap/internal/web"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := storage.DSNFromEnv()
	store, err := storage.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to storage: %v", err)
	}
	defer store.Close()

	// The base schema migration must succeed for the binary to start; the
	// optional TimescaleDB hypertable step is allowed to fail (logged, not
	// fatal) — see internal/storage/migrations for why.
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}

	// Real go-tari-grpc-lib-backed client: talks to Tari base nodes over
	// gRPC. Dials fresh per-call (see grpcNodeClient's doc comment in
	// internal/collector/grpc_client.go for why that's fine here).
	client := collector.NewGRPCClient()

	c := collector.New(collector.Config{SeedNodes: parseSeedNodes(os.Getenv("NETMAP_SEED_NODES"))})
	c.Storage = store
	c.Client = client

	go func() {
		if err := c.Run(ctx); err != nil {
			log.Printf("collector: run error: %v", err)
		}
	}()

	webHandler, err := web.NewHandler(store)
	if err != nil {
		log.Fatalf("failed to build web handler: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", webHandler)
	mux.Handle("/api/", http.StripPrefix("/api", api.NewRouter(store, client)))

	srv := &http.Server{Addr: *addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
	}()

	log.Printf("go-tari-netmap listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

// parseSeedNodes parses a comma-separated list of seed node addresses,
// trimming whitespace and dropping empty entries. An empty/unset raw value
// returns a nil (empty) seed list.
func parseSeedNodes(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seeds := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			seeds = append(seeds, p)
		}
	}
	return seeds
}
