package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/config"
)

// pairingAttemptTracker tracks failed pairing attempts per source IP and per
// challenge. After maxAttempts failures, the tracker enters a lockout state
// for lockoutWindow duration, during which further attempts are rejected.
type pairingAttemptTracker struct {
	mu                sync.Mutex
	ipAttempts        map[string]attemptState
	challengeAttempts map[string]attemptState
	maxAttempts       int
	lockoutWindow     time.Duration
}

// attemptState holds the failure count and lockout deadline for a single key.
type attemptState struct {
	failures    int
	lockedUntil time.Time
}

// newPairingAttemptTracker creates an attempt tracker with configuration-aware
// defaults. Zero or negative config values fall back to compiled-in constants.
func newPairingAttemptTracker(cfg config.Config) *pairingAttemptTracker {
	maxAttempts := cfg.PairingMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = pairingMaxAttempts
	}
	lockoutWindow := cfg.PairingLockoutWindow
	if lockoutWindow <= 0 {
		lockoutWindow = pairingLockoutWindow
	}
	return &pairingAttemptTracker{
		ipAttempts:        make(map[string]attemptState),
		challengeAttempts: make(map[string]attemptState),
		maxAttempts:       maxAttempts,
		lockoutWindow:     lockoutWindow,
	}
}

// recordFailure increments the failure count for both the source IP and the
// challenge key. If failures reach maxAttempts, the lockout deadline is set.
func (t *pairingAttemptTracker) recordFailure(ip, challengeKey string, now time.Time) {
	if ip == "" {
		ip = "unknown"
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ipAttempts[ip] = nextAttemptState(t.ipAttempts[ip], now, t.maxAttempts, t.lockoutWindow)
	if challengeKey != "" {
		t.challengeAttempts[challengeKey] = nextAttemptState(t.challengeAttempts[challengeKey], now, t.maxAttempts, t.lockoutWindow)
	}
}

// isLocked returns true if either the source IP or the challenge key is
// currently in a lockout period. Expired lockouts are automatically cleaned up.
func (t *pairingAttemptTracker) isLocked(ip, challengeKey string, now time.Time) bool {
	if ip == "" {
		ip = "unknown"
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if attemptLocked(t.ipAttempts, ip, now) {
		return true
	}
	if challengeKey != "" && attemptLocked(t.challengeAttempts, challengeKey, now) {
		return true
	}
	return false
}

// reset clears the failure state for a source IP and challenge key after
// successful pairing.
func (t *pairingAttemptTracker) reset(ip, challengeKey string) {
	if ip == "" {
		ip = "unknown"
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.ipAttempts, ip)
	if challengeKey != "" {
		delete(t.challengeAttempts, challengeKey)
	}
}

// nextAttemptState advances the failure counter and sets a lockout deadline
// if the max attempts threshold is reached.
func nextAttemptState(state attemptState, now time.Time, maxAttempts int, lockoutWindow time.Duration) attemptState {
	state.failures++
	if state.failures >= maxAttempts {
		state.lockedUntil = now.UTC().Add(lockoutWindow)
		state.failures = 0 // reset counter so next batch starts fresh after lockout expires
	}
	return state
}

// attemptLocked checks whether a specific key is currently locked. It also
// performs lazy cleanup of expired lockouts to prevent unbounded map growth.
func attemptLocked(attempts map[string]attemptState, key string, now time.Time) bool {
	state, ok := attempts[key]
	if !ok {
		return false
	}
	if state.lockedUntil.IsZero() {
		return false
	}
	if now.UTC().After(state.lockedUntil) {
		delete(attempts, key)
		return false
	}
	return true
}

// pairingAttemptKey normalizes a challenge ID into a tracking key. Empty
// challenge IDs are treated as "active" to track attempts against the current
// challenge collectively.
func pairingAttemptKey(challengeID string) string {
	challengeID = strings.TrimSpace(challengeID)
	if challengeID == "" {
		return "active"
	}
	return challengeID
}

// requestSourceIP extracts the source IP address from an HTTP request for
// rate limiting and attempt tracking purposes.
func requestSourceIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}
