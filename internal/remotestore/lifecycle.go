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

// Ping implements storage.Store by checking BOTH bare central-API reachability (GET
// /healthz) AND, more importantly, the report channel's own actual health (see this repo's
// readiness-review follow-up, Fix 1) -- this is what backs cmd/netmap-p2p-responder's own
// /healthz check (see that binary's metrics.go: it calls store.Ping to decide its own
// liveness), so a satellite correctly reports itself unhealthy when it can no longer
// actually report data, not just when the central system's bare GET /healthz (an
// unconditional 200 with no backend check at all, see internal/api/api.go's own doc comment
// on that route) happens to respond, and not just when its own process has crashed.
//
// Before Fix 1, this method ONLY proxied the central GET /healthz call -- so a satellite with
// a wrong/rotated collector API key, a central Postgres outage, or NETMAP_COLLECTOR_KEYS
// unset centrally reported fully healthy here while every single
// POST /internal/collectors/report was actually failing (GET /healthz doesn't touch auth or
// storage at all). The report-channel check below closes that gap: if the most recent flush
// attempt failed AND it's been longer than Config.ReportChannelUnhealthyThreshold since the
// last SUCCESSFUL flush, Ping fails on that basis alone, without even needing to make the
// GET /healthz call.
//
// A satellite that has never yet attempted a flush (lastFlushAttempt is the zero time.Time --
// e.g. immediately after startup, before the first FlushInterval tick) is NOT considered
// unhealthy on this basis: there's nothing to judge yet, so this check is skipped entirely and
// Ping falls through to the bare central-API reachability check below, exactly as before Fix
// 1. Likewise, a satellite whose most recent flush attempt succeeded skips this check (nothing
// to report), and one whose most recent attempt failed but is still within the configured
// grace threshold ALSO falls through to the bare-reachability check below, rather than failing
// immediately on a single blip.
func (s *Store) Ping(ctx context.Context) error {
	s.mu.Lock()
	lastAttempt := s.lastFlushAttempt
	lastSuccess := s.lastFlushSuccess
	lastErr := s.lastFlushErr
	s.mu.Unlock()

	if lastErr != nil && !lastAttempt.IsZero() {
		staleSince := lastSuccess
		neverSucceeded := staleSince.IsZero()
		if neverSucceeded {
			// Never once succeeded -- judge staleness from the first-ever attempt instead,
			// so a satellite that has been failing to report since the moment it started
			// doesn't get an indefinite pass just because lastFlushSuccess is still the
			// zero value.
			staleSince = lastAttempt
		}
		threshold := s.reportChannelUnhealthyThreshold()
		if time.Since(staleSince) > threshold {
			if neverSucceeded {
				return errRemoteStore("report channel unhealthy: last flush attempt failed (%v); no flush has ever succeeded (threshold %s)", lastErr, threshold)
			}
			return errRemoteStore("report channel unhealthy: last flush attempt failed (%v); last successful flush was %s ago (threshold %s)", lastErr, time.Since(lastSuccess).Round(time.Second), threshold)
		}
	}

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
