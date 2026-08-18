package api

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// clientIP extracts the source IP to key rate-limiting/lockout state by.
// r.RemoteAddr is directly trustworthy here — this service has no reverse
// proxy in front of it, so honoring X-Forwarded-For would let any
// submitter trivially spoof their rate-limit/lockout identity; we
// deliberately do not read that header anywhere in this package.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// Not host:port shaped (e.g. a test using a bare IP with no
		// port) — fall back to the raw value rather than hard-failing
		// the request over this.
		return r.RemoteAddr
	}
	return host
}

// ipRateLimiter enforces a per-source-IP submission rate limit on
// POST /nodes. See clientIP's doc comment for why r.RemoteAddr (and not
// X-Forwarded-For) is the identity used here.
type ipRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rateLimiterEntry

	rate  rate.Limit
	burst int
}

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimiterStaleAfter and rateLimiterCleanupInterval bound the memory
// used by ipRateLimiter.limiters: entries not seen in over an hour are
// pruned every 10 minutes by a background goroutine, so a flood of
// one-off source IPs can't grow this map unboundedly forever.
const (
	rateLimiterStaleAfter      = time.Hour
	rateLimiterCleanupInterval = 10 * time.Minute
)

// newIPRateLimiter returns a limiter allowing 5 submissions per source IP
// per hour, burst 5: permissive enough for a legitimate operator
// submitting a few nodes in one sitting, strict enough to meaningfully
// slow down automated spam. It starts its own background cleanup
// goroutine immediately; there is no explicit shutdown since one limiter
// lives for the lifetime of the process (matching NewRouter being called
// once at startup).
func newIPRateLimiter() *ipRateLimiter {
	l := &ipRateLimiter{
		limiters: make(map[string]*rateLimiterEntry),
		rate:     rate.Every(time.Hour / 5),
		burst:    5,
	}
	go l.cleanupLoop()
	return l
}

// Allow reports whether a new submission from ip is currently within the
// rate limit, creating that IP's limiter entry on first use.
func (l *ipRateLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.limiters[ip]
	if !ok {
		entry = &rateLimiterEntry{limiter: rate.NewLimiter(l.rate, l.burst)}
		l.limiters[ip] = entry
	}
	entry.lastSeen = time.Now()
	return entry.limiter.Allow()
}

func (l *ipRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rateLimiterCleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		l.prune()
	}
}

func (l *ipRateLimiter) prune() {
	cutoff := time.Now().Add(-rateLimiterStaleAfter)
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, entry := range l.limiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.limiters, ip)
		}
	}
}

// invalidSubmissionTracker escalates the consequence for a source IP
// that repeatedly submits genuinely invalid (malformed/SSRF-blocked)
// node addresses — a stronger, harsher signal than the flat rate limit
// above, specifically for demonstrated bad-faith/repeated invalid
// behavior rather than mere volume.
type invalidSubmissionTracker struct {
	mu    sync.Mutex
	state map[string]*invalidSubmissionState
}

type invalidSubmissionState struct {
	strikes     int
	windowStart time.Time
	lockedUntil time.Time
}

// strikeWindow/strikeThreshold/lockoutDuration: 4 genuinely invalid
// submissions from one source IP inside an hour is a strong bad-faith
// signal (a legitimate operator fixing typos won't hit this — the SSRF
// and shape checks it's tied to are unambiguous, not "this one probably
// won't connect"), earning a 6-hour lockout on further submissions from
// that IP.
const (
	strikeWindow    = time.Hour
	strikeThreshold = 4
	lockoutDuration = 6 * time.Hour
)

const (
	invalidSubmissionStaleAfter      = strikeWindow + lockoutDuration
	invalidSubmissionCleanupInterval = 10 * time.Minute
)

// newInvalidSubmissionTracker returns a tracker with its own background
// cleanup goroutine, started immediately (see ipRateLimiter's constructor
// doc comment for why there's no explicit shutdown).
func newInvalidSubmissionTracker() *invalidSubmissionTracker {
	t := &invalidSubmissionTracker{state: make(map[string]*invalidSubmissionState)}
	go t.cleanupLoop()
	return t
}

// RecordInvalidStrike records one genuinely-invalid submission from ip,
// escalating to a lockout once strikeThreshold is reached within
// strikeWindow.
func (t *invalidSubmissionTracker) RecordInvalidStrike(ip string) {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok := t.state[ip]
	if !ok {
		s = &invalidSubmissionState{windowStart: now}
		t.state[ip] = s
	}
	if now.Sub(s.windowStart) > strikeWindow {
		s.strikes = 0
		s.windowStart = now
	}
	s.strikes++
	if s.strikes >= strikeThreshold {
		s.lockedUntil = now.Add(lockoutDuration)
	}
}

// IsLockedOut reports whether ip is currently locked out, and if so, how
// much longer the lockout has to run.
func (t *invalidSubmissionTracker) IsLockedOut(ip string) (bool, time.Duration) {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok := t.state[ip]
	if !ok {
		return false, 0
	}
	if now.Before(s.lockedUntil) {
		return true, s.lockedUntil.Sub(now)
	}
	return false, 0
}

func (t *invalidSubmissionTracker) cleanupLoop() {
	ticker := time.NewTicker(invalidSubmissionCleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		t.prune()
	}
}

func (t *invalidSubmissionTracker) prune() {
	cutoff := time.Now().Add(-invalidSubmissionStaleAfter)
	t.mu.Lock()
	defer t.mu.Unlock()
	for ip, s := range t.state {
		lastActivity := s.windowStart
		if s.lockedUntil.After(lastActivity) {
			lastActivity = s.lockedUntil
		}
		if lastActivity.Before(cutoff) {
			delete(t.state, ip)
		}
	}
}
