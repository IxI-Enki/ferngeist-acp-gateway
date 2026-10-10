package session

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func newTestTokenServiceAt(now time.Time) (*TokenService, *time.Time) {
	svc := NewTokenService(slog.New(slog.NewTextHandler(io.Discard, nil)))
	current := now
	svc.now = func() time.Time { return current }
	return svc, &current
}

// TestMintReturnsValidatableToken verifies that Mint produces a 64-char hex token
// that can be validated to recover session and device IDs.
func TestMintReturnsValidatableToken(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token, err := svc.Mint("session-1", "device-1", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	if token == "" {
		t.Fatal("Mint() returned empty token")
	}
	if len(token) != 64 {
		t.Fatalf("token length = %d, want 64", len(token))
	}

	sessionID, deviceID, err := svc.Validate(token)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if sessionID != "session-1" {
		t.Fatalf("sessionID = %q, want %q", sessionID, "session-1")
	}
	if deviceID != "device-1" {
		t.Fatalf("deviceID = %q, want %q", deviceID, "device-1")
	}
}

// TestValidateConsumesToken verifies that an attach token can only be validated once;
// subsequent attempts are rejected.
func TestValidateConsumesToken(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token, err := svc.Mint("session-1", "device-1", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}

	if _, _, err := svc.Validate(token); err != nil {
		t.Fatalf("first Validate() error = %v", err)
	}
	if _, _, err := svc.Validate(token); err != ErrAttachTokenInvalid {
		t.Fatalf("second Validate() error = %v, want %v", err, ErrAttachTokenInvalid)
	}
}

// TestValidateRejectsExpiredToken verifies that an attach token minted with a
// negative TTL is rejected as expired.
func TestValidateRejectsExpiredToken(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token, err := svc.Mint("session-1", "device-1", -time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}

	if _, _, err := svc.Validate(token); err != ErrAttachTokenExpired {
		t.Fatalf("Validate() error = %v, want %v", err, ErrAttachTokenExpired)
	}
}

// TestValidateRejectsInvalidToken verifies that a garbage token string is rejected.
func TestValidateRejectsInvalidToken(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	if _, _, err := svc.Validate("not-a-real-token"); err != ErrAttachTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrAttachTokenInvalid)
	}
}

// TestValidateRejectsEmptyToken verifies that an empty token string is rejected.
func TestValidateRejectsEmptyToken(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	if _, _, err := svc.Validate(""); err != ErrAttachTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrAttachTokenInvalid)
	}
}

// TestClearAllInvalidatesTokens verifies that ClearAll invalidates all minted tokens.
func TestClearAllInvalidatesTokens(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token, err := svc.Mint("session-1", "device-1", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	svc.ClearAll()

	if _, _, err := svc.Validate(token); err != ErrAttachTokenInvalid {
		t.Fatalf("Validate() after ClearAll error = %v, want %v", err, ErrAttachTokenInvalid)
	}
}

// TestMintProducesUniqueTokens verifies that Mint produces distinct tokens for
// different sessions.
func TestMintProducesUniqueTokens(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token1, err := svc.Mint("session-1", "device-1", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	token2, err := svc.Mint("session-2", "device-2", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}

	if token1 == "" || token2 == "" {
		t.Fatal("Mint() returned empty token")
	}
	if token1 == token2 {
		t.Fatal("Mint() returned identical tokens")
	}
}

// TestMintContainsCorrectClaims verifies that each attach token encodes the
// correct session and device IDs.
func TestMintContainsCorrectClaims(t *testing.T) {
	svc, _ := newTestTokenServiceAt(time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC))

	token1, err := svc.Mint("session-1", "device-1", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	token2, err := svc.Mint("session-2", "device-2", time.Minute)
	if err != nil {
		t.Fatalf("Mint() error = %v", err)
	}

	sessionID, deviceID, err := svc.Validate(token1)
	if err != nil {
		t.Fatalf("token1 Validate() error = %v", err)
	}
	if sessionID != "session-1" || deviceID != "device-1" {
		t.Fatalf("token1 claims = (%q, %q), want (session-1, device-1)", sessionID, deviceID)
	}

	sessionID, deviceID, err = svc.Validate(token2)
	if err != nil {
		t.Fatalf("token2 Validate() error = %v", err)
	}
	if sessionID != "session-2" || deviceID != "device-2" {
		t.Fatalf("token2 claims = (%q, %q), want (session-2, device-2)", sessionID, deviceID)
	}
}

// TestExpiredTokensSwept fails while expired-but-never-presented tokens linger:
// minting after the TTL + sweep interval must not grow the map.
func TestExpiredTokensSwept(t *testing.T) {
	t0 := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	svc, current := newTestTokenServiceAt(t0)

	if _, err := svc.Mint("s1", "d1", time.Minute); err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	*current = t0.Add(2 * time.Minute) // past TTL (1m) and sweep interval
	if _, err := svc.Mint("s2", "d1", time.Minute); err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	svc.mu.Lock()
	n := len(svc.attachTokens)
	svc.mu.Unlock()
	if n != 1 {
		t.Fatalf("len(attachTokens) = %d, want 1 (expired entry swept)", n)
	}
}

// TestValidateSweepsExpiredTokens covers the Validate-triggered sweep path.
func TestValidateSweepsExpiredTokens(t *testing.T) {
	t0 := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	svc, current := newTestTokenServiceAt(t0)

	if _, err := svc.Mint("s1", "d1", time.Minute); err != nil {
		t.Fatalf("Mint() error = %v", err)
	}
	*current = t0.Add(2 * time.Minute)
	if _, _, err := svc.Validate("never-minted"); err != ErrAttachTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrAttachTokenInvalid)
	}
	svc.mu.Lock()
	n := len(svc.attachTokens)
	svc.mu.Unlock()
	if n != 0 {
		t.Fatalf("len(attachTokens) = %d, want 0 (expired entry swept)", n)
	}
}

// TestAttachTokenCountIsCapped bounds memory under a mint flood inside one
// sweep window (all entries fresh, so sweeping alone cannot help).
func TestAttachTokenCountIsCapped(t *testing.T) {
	t0 := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	svc, _ := newTestTokenServiceAt(t0)

	for i := range maxAttachTokens + 500 {
		if _, err := svc.Mint(fmt.Sprintf("s%d", i), "d1", time.Minute); err != nil {
			t.Fatalf("Mint() error = %v", err)
		}
	}
	svc.mu.Lock()
	n := len(svc.attachTokens)
	svc.mu.Unlock()
	if n > maxAttachTokens {
		t.Fatalf("len(attachTokens) = %d, want <= %d", n, maxAttachTokens)
	}
}
