// Package gateway tests cover runtime token lifecycle (register, validate, revoke, expiry),
// attach token minting/validation/single-use semantics, in-memory fallback when the store
// is unavailable, store error logging, and concurrent access safety.
package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/runtime"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/storage"
)

// ---------------------------------------------------------------------------
// Runtime token validation — memory-only path
// ---------------------------------------------------------------------------

// TestValidateReturnsNilForRegisteredRuntimeToken verifies that a registered runtime token passes validation with the correct token.
func TestValidateReturnsNilForRegisteredRuntimeToken(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// TestValidateReturnsNilAfterRegister verifies that a registered runtime token is validated successfully when backed by persistent storage.
func TestValidateReturnsNilAfterRegister(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// TestValidateReturnsErrRuntimeTokenInvalidAfterRevoke verifies that revoking a runtime token causes subsequent validation to fail.
func TestValidateReturnsErrRuntimeTokenInvalidAfterRevoke(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	service.Revoke("runtime-1")

	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after revoke error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// ---------------------------------------------------------------------------
// ClearAll
// ---------------------------------------------------------------------------

// TestClearAllInvalidatesRuntimeTokens verifies that ClearAll invalidates all stored runtime tokens.
func TestClearAllInvalidatesRuntimeTokens(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	service.ClearAll()

	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after ClearAll error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// ---------------------------------------------------------------------------
// Validation — edge cases (memory-only)
// ---------------------------------------------------------------------------

// TestValidateExpiredToken verifies that an expired runtime token returns ErrRuntimeTokenExpired.
func TestValidateExpiredToken(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(-time.Minute),
	})

	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenExpired {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenExpired)
	}
}

// TestValidateWrongToken verifies that a token mismatching the registered one is rejected.
func TestValidateWrongToken(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "valid-token",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := service.Validate("runtime-1", "wrong-token"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestValidateEmptyToken verifies that an empty token string is rejected with ErrRuntimeTokenMissing.
func TestValidateEmptyToken(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	if err := service.Validate("runtime-1", ""); err != ErrRuntimeTokenMissing {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenMissing)
	}
}

// ---------------------------------------------------------------------------
// Validation — store-only path (no prior Register call)
// ---------------------------------------------------------------------------

// TestValidateReturnsNilForTokenStoredDirectlyInDB verifies that a runtime token persisted directly in the store is found by Validate.
func TestValidateReturnsNilForTokenStoredDirectlyInDB(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	if err := store.SaveRuntimeToken(context.Background(), storage.RuntimeTokenRecord{
		RuntimeID: "runtime-1",
		Token:     "token-1",
		ExpiresAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("SaveRuntimeToken() error = %v", err)
	}

	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// TestValidateExpiredTokenFromStore verifies that an expired runtime token loaded from persistent storage is rejected.
func TestValidateExpiredTokenFromStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(-time.Minute),
	})

	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenExpired {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenExpired)
	}
}

// TestValidateWrongTokenFromStore verifies that a wrong token loaded from persistent storage is rejected.
func TestValidateWrongTokenFromStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "valid-token",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := service.Validate("runtime-1", "wrong-token"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// ---------------------------------------------------------------------------
// RevokeIfMatches — happy paths
// ---------------------------------------------------------------------------

// TestRevokeIfMatchesReturnsTrueAndInvalidatesTokenOnMatch verifies that RevokeIfMatches revokes a matching token, making it invalid.
func TestRevokeIfMatchesReturnsTrueAndInvalidatesTokenOnMatch(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if revoked := service.RevokeIfMatches("runtime-1", "token-1"); !revoked {
		t.Fatal("RevokeIfMatches() should return true for matching token")
	}
	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after revoke error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestRevokeIfMatchesReturnsFalseForWrongToken verifies that RevokeIfMatches does not revoke a non-matching token.
func TestRevokeIfMatchesReturnsFalseForWrongToken(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if revoked := service.RevokeIfMatches("runtime-1", "wrong-token"); revoked {
		t.Fatal("RevokeIfMatches() should return false for wrong token")
	}
	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() after wrong revoke attempt error = %v", err)
	}
}

// TestRevokeIfMatchesEmptyToken verifies that RevokeIfMatches returns false for an empty token.
func TestRevokeIfMatchesEmptyToken(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if revoked := service.RevokeIfMatches("runtime-1", ""); revoked {
		t.Fatal("RevokeIfMatches() should return false for empty token")
	}
}

// ---------------------------------------------------------------------------
// Memory fallback when store record is deleted
// ---------------------------------------------------------------------------

// TestValidateReturnsNilWhenMemoryFallbackUsed verifies that a registered token is still valid from the in-memory cache after the store record is deleted.
func TestValidateReturnsNilWhenMemoryFallbackUsed(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := store.DeleteRuntimeToken(context.Background(), "runtime-1"); err != nil {
		t.Fatalf("DeleteRuntimeToken() error = %v", err)
	}

	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() from memory fallback error = %v", err)
	}
}

// TestRevokeIfMatchesFallsBackToMemoryWhenStoreClosed verifies that RevokeIfMatches falls back to in-memory state and succeeds when the store record is gone.
func TestRevokeIfMatchesFallsBackToMemoryWhenStoreClosed(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := store.DeleteRuntimeToken(context.Background(), "runtime-1"); err != nil {
		t.Fatalf("DeleteRuntimeToken() error = %v", err)
	}

	if revoked := service.RevokeIfMatches("runtime-1", "token-1"); !revoked {
		t.Fatal("RevokeIfMatches() should return true from memory fallback")
	}
}

// TestRevokeIfMatchesReturnsFalseForWrongTokenFromMemory verifies that RevokeIfMatches correctly rejects a wrong token even in memory fallback mode.
func TestRevokeIfMatchesReturnsFalseForWrongTokenFromMemory(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if err := store.DeleteRuntimeToken(context.Background(), "runtime-1"); err != nil {
		t.Fatalf("DeleteRuntimeToken() error = %v", err)
	}

	if revoked := service.RevokeIfMatches("runtime-1", "wrong-token"); revoked {
		t.Fatal("RevokeIfMatches() should return false for wrong token from memory fallback")
	}
}

// ---------------------------------------------------------------------------
// RevokeIfMatches — newer token protection
// ---------------------------------------------------------------------------

// TestRevokeIfMatchesReturnsFalseForOldTokenWhenNewerExists verifies that an old (replaced) token cannot be revoked when a newer token is registered for the same runtime.
func TestRevokeIfMatchesReturnsFalseForOldTokenWhenNewerExists(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-old",
		TokenExpiresAt: now.Add(time.Minute),
	})
	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-new",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if revoked := service.RevokeIfMatches("runtime-1", "token-old"); revoked {
		t.Fatal("RevokeIfMatches() should not revoke a newer runtime token")
	}
	if err := service.Validate("runtime-1", "token-new"); err != nil {
		t.Fatalf("Validate(new token) error = %v", err)
	}
}

// TestRevokeIfMatchesReturnsTrueForCurrentToken verifies that the current (newest) token can be revoked.
func TestRevokeIfMatchesReturnsTrueForCurrentToken(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-old",
		TokenExpiresAt: now.Add(time.Minute),
	})
	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-new",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if revoked := service.RevokeIfMatches("runtime-1", "token-new"); !revoked {
		t.Fatal("RevokeIfMatches() should revoke the active runtime token")
	}
	if err := service.Validate("runtime-1", "token-new"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after matched revoke error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// ---------------------------------------------------------------------------
// Expired runtime token cleanup
// ---------------------------------------------------------------------------

// TestValidateReturnsInvalidForNonExistentRuntimeAndCleansUp verifies that validating a non-existent runtime returns invalid and triggers cleanup of expired entries.
func TestValidateReturnsInvalidForNonExistentRuntimeAndCleansUp(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})
	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "expired-runtime",
		BearerToken:    "expired-token",
		TokenExpiresAt: now.Add(-time.Minute),
	})

	if err := service.Validate("non-existent", "token"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}

	if err := service.Validate("expired-runtime", "expired-token"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after cleanup error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestValidateReturnsInvalidForExpiredRuntimeTokenAfterCleanup verifies that an expired runtime token returns the expired error.
func TestValidateReturnsInvalidForExpiredRuntimeTokenAfterCleanup(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "expired-runtime",
		BearerToken:    "expired-token",
		TokenExpiresAt: now.Add(-time.Minute),
	})

	if err := service.Validate("expired-runtime", "expired-token"); err != ErrRuntimeTokenExpired {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenExpired)
	}
}

// ---------------------------------------------------------------------------
// Log capture — store operations log errors on closed store
// ---------------------------------------------------------------------------

// TestRegisterLogsErrorOnClosedStore verifies that Register logs an error and falls back to in-memory when the store is closed.
func TestRegisterLogsErrorOnClosedStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	store.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	if !bytes.Contains(buf.Bytes(), []byte("persist runtime token failed")) {
		t.Fatal("expected error log for closed store on Register")
	}
	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() error = %v, want nil (in-memory fallback)", err)
	}
}

// TestRevokeLogsErrorOnClosedStore verifies that Revoke logs an error and falls back to in-memory invalidation when the store is closed.
func TestRevokeLogsErrorOnClosedStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	store.Close()

	service.Revoke("runtime-1")

	if !bytes.Contains(buf.Bytes(), []byte("delete runtime token failed")) {
		t.Fatal("expected error log for closed store on Revoke")
	}
	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after revoke error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestClearAllLogsErrorOnClosedStore verifies that ClearAll logs an error and falls back to in-memory invalidation when the store is closed.
func TestClearAllLogsErrorOnClosedStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	store.Close()

	service.ClearAll()

	if !bytes.Contains(buf.Bytes(), []byte("delete all runtime tokens failed")) {
		t.Fatal("expected error log for closed store on ClearAll")
	}
	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after ClearAll error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestRevokeIfMatchesLogsErrorOnClosedStore verifies that RevokeIfMatches logs a store error and falls back to memory when the store is closed.
func TestRevokeIfMatchesLogsErrorOnClosedStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	store.Close()

	if revoked := service.RevokeIfMatches("runtime-1", "token-1"); !revoked {
		t.Fatal("RevokeIfMatches() should return true from memory fallback")
	}

	if !bytes.Contains(buf.Bytes(), []byte("load runtime token failed")) {
		t.Fatal("expected error log for closed store on RevokeIfMatches")
	}
	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenInvalid {
		t.Fatalf("Validate() after revoke error = %v, want %v", err, ErrRuntimeTokenInvalid)
	}
}

// TestValidateLogsStoreErrorsOnClosedStore verifies that Validate logs store errors and falls back to in-memory state when the store is closed.
func TestValidateLogsStoreErrorsOnClosedStore(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	store.Close()

	if err := service.Validate("runtime-1", "token-1"); err != nil {
		t.Fatalf("Validate() error = %v, want nil (in-memory fallback)", err)
	}

	if !bytes.Contains(buf.Bytes(), []byte("delete expired runtime tokens failed")) &&
		!bytes.Contains(buf.Bytes(), []byte("load runtime token failed")) {
		t.Fatal("expected store error log for closed store on Validate")
	}
}

// ---------------------------------------------------------------------------
// Store errors — delete blocked by trigger
// ---------------------------------------------------------------------------

// TestRevokeIfMatchesStoreDeleteErrorViaTrigger verifies that RevokeIfMatches logs an error when the store delete is blocked by a SQL trigger.
func TestRevokeIfMatchesStoreDeleteErrorViaTrigger(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	service := New(logger, store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(time.Minute),
	})

	d, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer d.Close()

	if _, err := d.ExecContext(context.Background(),
		`CREATE TRIGGER IF NOT EXISTS block_delete BEFORE DELETE ON runtime_tokens
		 BEGIN
		     SELECT RAISE(ABORT, 'delete blocked');
		 END;`); err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}

	if revoked := service.RevokeIfMatches("runtime-1", "token-1"); !revoked {
		t.Fatal("RevokeIfMatches() should return true")
	}

	if !bytes.Contains(buf.Bytes(), []byte("delete runtime token failed")) {
		t.Fatal("expected error log for trigger-blocked delete")
	}

	record, err := store.GetRuntimeToken(context.Background(), "runtime-1")
	if err == nil {
		t.Logf("Token still in store: %+v (expected — trigger blocked delete)", record)
	}
}

// TestValidateExpiredStoreRecordWhenDeleteExpiredFails verifies that Validate correctly identifies expired tokens even when the store cleanup delete is blocked.
func TestValidateExpiredStoreRecordWhenDeleteExpiredFails(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	service.Register(runtime.ConnectDescriptor{
		RuntimeID:      "runtime-1",
		BearerToken:    "token-1",
		TokenExpiresAt: now.Add(-time.Minute),
	})

	d, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer d.Close()

	if _, err := d.ExecContext(context.Background(),
		`CREATE TRIGGER IF NOT EXISTS block_delete BEFORE DELETE ON runtime_tokens
		 BEGIN
		     SELECT RAISE(ABORT, 'delete blocked');
		 END;`); err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}

	if err := service.Validate("runtime-1", "token-1"); err != ErrRuntimeTokenExpired {
		t.Fatalf("Validate() error = %v, want %v", err, ErrRuntimeTokenExpired)
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestConcurrentRegisterAndValidate verifies that concurrent Register and Validate calls are safe and succeed under goroutine parallelism.
func TestConcurrentRegisterAndValidate(t *testing.T) {
	service := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	now := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	runtimeIDs := []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10"}
	tokens := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"}

	var wg sync.WaitGroup
	for i := range runtimeIDs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			service.Register(runtime.ConnectDescriptor{
				RuntimeID:      runtimeIDs[i],
				BearerToken:    tokens[i],
				TokenExpiresAt: now.Add(time.Minute),
			})
		}(i)
	}
	wg.Wait()

	for i := range runtimeIDs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := service.Validate(runtimeIDs[i], tokens[i]); err != nil {
				t.Errorf("Validate(%q, %q) error = %v", runtimeIDs[i], tokens[i], err)
			}
		}(i)
	}
	wg.Wait()
}
