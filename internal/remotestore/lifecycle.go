package remotestore

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// Run starts this Store's background cache-refresh and flush loops, blocking until ctx is
// canceled, then returning nil. Mirrors collector.Collector.Run's convention -- callers run
// it via `go store.Run(ctx)`.
//
// The cache is refreshed once immediately (best-effort, logged not fatal on failure) before
// entering the ticker loop, so a freshly started satellite's passive-responder role has a
// populated peer list to serve as soon as possible rather than waiting a full
// CacheRefreshInterval for its first refresh.
//
// Run itself does NOT attempt a final flush when ctx is canceled -- that is Close's
// responsibility (see its doc comment), so that a caller's normal `defer store.Close()`
// shutdown sequence has exactly one place responsible for that best-effort final attempt,
// regardless of exactly when/whether Run's own goroutine has already exited.
func (s *Store) Run(ctx context.Context) error {
	if err := s.refreshCache(ctx); err != nil {
		s.logf("remotestore: initial seed_list refresh failed (will retry on next tick): %v", err)
	}

	cacheTicker := time.NewTicker(s.cacheRefreshInterval())
	defer cacheTicker.Stop()
	flushTicker := time.NewTicker(s.flushInterval())
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-cacheTicker.C:
			if err := s.refreshCache(ctx); err != nil {
				s.logf("remotestore: seed_list refresh failed: %v", err)
			}
		case <-flushTicker.C:
			if err := s.flushOnce(ctx); err != nil {
				s.logf("remotestore: flush failed (buffered data retained for retry): %v", err)
			}
		case <-s.flushTrigger:
			if err := s.flushOnce(ctx); err != nil {
				s.logf("remotestore: size-triggered flush failed (buffered data retained for retry): %v", err)
			}
		}
	}
}

// Ping implements storage.Store by checking connectivity to the central API's GET /healthz
// route -- this is what backs cmd/netmap-p2p-responder's own /healthz check (see that
// binary's metrics.go: it calls store.Ping to decide its own liveness), so a satellite
// correctly reports itself unhealthy when it can no longer reach the central system, not just
// when its own process has crashed.
func (s *Store) Ping(ctx context.Context) error {
	url := strings.TrimRight(s.cfg.BaseURL, "/") + "/healthz"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errRemoteStore("build healthz request: %w", err)
	}

	resp, err := s.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return errRemoteStore("ping central API: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return errRemoteStore("ping central API: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// Close marks this Store closed (every subsequent write method returns errClosed) and makes
// one best-effort attempt to flush any still-buffered data before returning -- see flushOnce's
// doc comment for why a flush failure here still doesn't lose that data (it's simply left in
// the buffer, which is then discarded along with the rest of this Store's in-memory state once
// the process exits; there is no durable local queue). A flush failure here is logged, not
// returned, since Close's own contract (mirroring storage.Store.Close's error return) is about
// resource cleanup, not about the flush's outcome.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.flushOnce(ctx); err != nil {
		s.logf("remotestore: final flush on Close failed, buffered data is lost: %v", err)
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// Migrate implements storage.Store as a no-op: schema migrations are the central system's
// responsibility alone (it is the only process that ever holds a direct Postgres connection --
// see this repo's governing brief), never a remote collector satellite's.
func (s *Store) Migrate(ctx context.Context) error {
	return nil
}
