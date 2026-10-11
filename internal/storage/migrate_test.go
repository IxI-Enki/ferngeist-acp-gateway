package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// legacySchemaStatements returns the baseline schema as it looked before the
// retired-credential and clean columns existed and while gateway_sessions still
// had leaseholder. It is derived from baselineSchema so the test tracks the v1
// shape instead of a frozen copy.
func legacySchemaStatements() []string {
	out := make([]string, 0, len(baselineSchema))
	for _, s := range baselineSchema {
		switch {
		case strings.HasPrefix(strings.TrimSpace(s), "CREATE TABLE IF NOT EXISTS paired_devices"):
			s = `CREATE TABLE IF NOT EXISTS paired_devices (
				device_id TEXT PRIMARY KEY,
				device_name TEXT NOT NULL,
				token TEXT NOT NULL,
				expires_at TEXT NOT NULL
			)`
		case strings.HasPrefix(strings.TrimSpace(s), "CREATE TABLE IF NOT EXISTS runtime_failures"):
			s = `CREATE TABLE IF NOT EXISTS runtime_failures (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				runtime_id TEXT NOT NULL,
				agent_id TEXT NOT NULL,
				agent_name TEXT NOT NULL,
				last_error TEXT NOT NULL,
				created_at TEXT NOT NULL,
				failed_at TEXT NOT NULL,
				log_preview TEXT NOT NULL DEFAULT ''
			)`
		case strings.HasPrefix(strings.TrimSpace(s), "CREATE TABLE IF NOT EXISTS gateway_sessions"):
			s = `CREATE TABLE IF NOT EXISTS gateway_sessions (
				session_id TEXT PRIMARY KEY,
				runtime_id TEXT NOT NULL,
				device_id TEXT NOT NULL,
				agent_id TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT 'active',
				leaseholder TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL,
				last_client_connect_at TEXT,
				last_client_disconnect_at TEXT,
				disconnected_since TEXT,
				metadata TEXT NOT NULL DEFAULT '{}'
			)`
		}
		out = append(out, s)
	}
	return out
}

// writeLegacyDatabase builds a pre-user_version database file: the old
// column-less schema, the retired runtime_tokens table, and one pairing row so
// the test can prove existing data survives the upgrade.
func writeLegacyDatabase(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close legacy db: %v", err)
		}
	}()

	for _, statement := range legacySchemaStatements() {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec legacy schema statement: %v", err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE runtime_tokens (
		runtime_id TEXT PRIMARY KEY, token TEXT NOT NULL, expires_at TEXT NOT NULL)`); err != nil {
		t.Fatalf("create runtime_tokens: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO paired_devices(device_id, device_name, token, expires_at) VALUES (?, ?, ?, ?)`,
		"dev-legacy", "Old Phone", "sha256:legacy", time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("insert legacy pairing: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO gateway_sessions(session_id, runtime_id, device_id, agent_id, status, leaseholder, created_at, last_client_connect_at, last_client_disconnect_at, disconnected_since)
		 VALUES ('sess-legacy', 'rt-legacy', 'dev-legacy', 'agent-1', 'failed', 'sess-legacy', ?, '', '', '')`,
		time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("insert legacy session: %v", err)
	}
}

// TestMigrateUpgradesLegacyDatabase covers the upgrade path for every shape the
// pre-user_version code produced: tables missing the retired_token/retired_until
// and clean columns, plus a lingering runtime_tokens table. After Open the
// database must be at the latest version with all columns usable and old rows
// intact.
func TestMigrateUpgradesLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy_v0.db")
	writeLegacyDatabase(t, path)

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	var version int
	if err := store.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("PRAGMA user_version: %v", err)
	}
	if version != latestMigrationVersion {
		t.Fatalf("user_version = %d, want %d", version, latestMigrationVersion)
	}

	var name string
	err = store.db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_tokens'`).Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("runtime_tokens lookup error = %v (name=%q), want sql.ErrNoRows after migration", err, name)
	}

	// The pre-existing pairing row survives, and the retired-credential columns
	// ALTERed in by the migration are writable.
	pairings, err := store.ListPairings(ctx)
	if err != nil {
		t.Fatalf("ListPairings() error = %v", err)
	}
	if len(pairings) != 1 || pairings[0].DeviceID != "dev-legacy" || pairings[0].RetiredToken != "" {
		t.Fatalf("pairings after migration = %+v, want the one legacy row with empty RetiredToken", pairings)
	}
	retiredUntil := time.Date(2026, 4, 25, 10, 0, 0, 0, time.UTC)
	if err := store.SavePairing(ctx, PairingRecord{
		DeviceID:     "dev-legacy",
		DeviceName:   "Old Phone",
		Token:        "sha256:legacy",
		ExpiresAt:    retiredUntil.Add(24 * time.Hour),
		RetiredToken: "sha256:retired",
		RetiredUntil: retiredUntil,
	}); err != nil {
		t.Fatalf("SavePairing() error = %v", err)
	}
	pairings, err = store.ListPairings(ctx)
	if err != nil {
		t.Fatalf("ListPairings() error = %v", err)
	}
	if pairings[0].RetiredToken != "sha256:retired" || !pairings[0].RetiredUntil.Equal(retiredUntil) {
		t.Fatalf("RetiredToken/RetiredUntil = %q/%v, want sha256:retired/%v",
			pairings[0].RetiredToken, pairings[0].RetiredUntil, retiredUntil)
	}

	// The clean column ALTERed into runtime_failures is usable: clean exits are
	// excluded from ListRecentRuntimeFailures but kept by ListRuntimeFailureRecords.
	now := time.Now().UTC()
	if err := store.SaveRuntimeFailure(ctx, RuntimeFailureRecord{
		RuntimeID: "rt-1", AgentID: "agent-1", AgentName: "Agent", LastError: "",
		CreatedAt: now, FailedAt: now, CleanExit: true,
	}); err != nil {
		t.Fatalf("SaveRuntimeFailure() error = %v", err)
	}
	recent, err := store.ListRecentRuntimeFailures(ctx, 10)
	if err != nil {
		t.Fatalf("ListRecentRuntimeFailures() error = %v", err)
	}
	if len(recent) != 0 {
		t.Fatalf("ListRecentRuntimeFailures() = %+v, want empty (clean exit excluded)", recent)
	}
	all, err := store.ListRuntimeFailureRecords(ctx, 10)
	if err != nil {
		t.Fatalf("ListRuntimeFailureRecords() error = %v", err)
	}
	if len(all) != 1 || !all[0].CleanExit {
		t.Fatalf("ListRuntimeFailureRecords() = %+v, want one clean-exit record", all)
	}

	// leaseholder is dropped; the session row survives it.
	var leaseholderCols int
	if err := store.db.QueryRowContext(ctx,
		`SELECT count(*) FROM pragma_table_info('gateway_sessions') WHERE name = 'leaseholder'`).Scan(&leaseholderCols); err != nil {
		t.Fatalf("table_info lookup: %v", err)
	}
	if leaseholderCols != 0 {
		t.Fatal("gateway_sessions.leaseholder survived the migration")
	}
	if sess, err := store.GetSession(ctx, "sess-legacy"); err != nil || sess.RuntimeID != "rt-legacy" {
		t.Fatalf("GetSession(sess-legacy) = (%+v, %v), want the legacy row", sess, err)
	}
}

// TestFreshDatabaseIsAtLatestVersion verifies a new database skips no migration
// and reopening it neither re-runs nor errors.
func TestFreshDatabaseIsAtLatestVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	var version int
	if err := store.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("PRAGMA user_version: %v", err)
	}
	if version != latestMigrationVersion {
		t.Fatalf("fresh user_version = %d, want %d", version, latestMigrationVersion)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if err := reopened.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("PRAGMA user_version after reopen: %v", err)
	}
	if version != latestMigrationVersion {
		t.Fatalf("reopened user_version = %d, want %d", version, latestMigrationVersion)
	}
}
