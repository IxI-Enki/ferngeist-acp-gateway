package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// SessionRecord represents a stored resilient session row in gateway_sessions.
// Nullable time fields (LastClientConnectAt, LastClientDisconnectAt, DisconnectedSince)
// use pointers so SQLITE NULL maps to Go nil without parsing zero-value timestamps.
type SessionRecord struct {
	SessionID              string
	RuntimeID              string
	DeviceID               string
	AgentID                string
	Status                 string
	Leaseholder            string
	CreatedAt              time.Time
	LastClientConnectAt    *time.Time
	LastClientDisconnectAt *time.Time
	DisconnectedSince      *time.Time
	Metadata               string
}

// SaveSession inserts or updates a session record. Uses ON CONFLICT DO UPDATE
// so the same method works for both initial creation and incremental status updates.
func (s *SQLiteStore) SaveSession(ctx context.Context, record SessionRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO gateway_sessions(session_id, runtime_id, device_id, agent_id, status, leaseholder, created_at, last_client_connect_at, last_client_disconnect_at, disconnected_since, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(session_id) DO UPDATE SET
		   status = excluded.status,
		   leaseholder = excluded.leaseholder,
		   last_client_connect_at = excluded.last_client_connect_at,
		   last_client_disconnect_at = excluded.last_client_disconnect_at,
		   disconnected_since = excluded.disconnected_since,
		   metadata = excluded.metadata`,
		record.SessionID, record.RuntimeID, record.DeviceID, record.AgentID, record.Status, record.Leaseholder,
		record.CreatedAt.UTC().Format(time.RFC3339Nano),
		timeToNullable(record.LastClientConnectAt),
		timeToNullable(record.LastClientDisconnectAt),
		timeToNullable(record.DisconnectedSince),
		record.Metadata,
	)
	return err
}

func scanSession(row *sql.Row) (SessionRecord, error) {
	var rec SessionRecord
	var createdRaw, connRaw, discRaw, sinceRaw string
	err := row.Scan(&rec.SessionID, &rec.RuntimeID, &rec.DeviceID, &rec.AgentID, &rec.Status, &rec.Leaseholder, &createdRaw, &connRaw, &discRaw, &sinceRaw, &rec.Metadata)
	if err != nil {
		return SessionRecord{}, err
	}
	rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdRaw)
	rec.LastClientConnectAt = nullableToTime(connRaw)
	rec.LastClientDisconnectAt = nullableToTime(discRaw)
	rec.DisconnectedSince = nullableToTime(sinceRaw)
	return rec, nil
}

// GetSession retrieves a single session record by its session ID.
// Returns ErrNotFound if no row exists.
func (s *SQLiteStore) GetSession(ctx context.Context, sessionID string) (SessionRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT session_id, runtime_id, device_id, agent_id, status, leaseholder, created_at, last_client_connect_at, last_client_disconnect_at, disconnected_since, metadata
		 FROM gateway_sessions WHERE session_id = ?`, sessionID)
	rec, err := scanSession(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SessionRecord{}, ErrNotFound
		}
		return SessionRecord{}, err
	}
	return rec, nil
}

// DeleteSession removes a session and, via ON DELETE CASCADE, all its
// associated inbound diagnostic rows.
func (s *SQLiteStore) DeleteSession(ctx context.Context, sessionID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM gateway_sessions WHERE session_id = ?`, sessionID)
	return err
}

// ListSessionsByDevice returns all sessions owned by a device, ordered by
// creation time descending (newest first). Used to enforce per-device limits
// and for the sessions list endpoint.
func (s *SQLiteStore) ListSessionsByDevice(ctx context.Context, deviceID string) ([]SessionRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, runtime_id, device_id, agent_id, status, leaseholder, created_at, last_client_connect_at, last_client_disconnect_at, disconnected_since, metadata
		 FROM gateway_sessions WHERE device_id = ? ORDER BY created_at DESC`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []SessionRecord
	for rows.Next() {
		var rec SessionRecord
		var createdRaw, connRaw, discRaw, sinceRaw string
		if err := rows.Scan(&rec.SessionID, &rec.RuntimeID, &rec.DeviceID, &rec.AgentID, &rec.Status, &rec.Leaseholder, &createdRaw, &connRaw, &discRaw, &sinceRaw, &rec.Metadata); err != nil {
			return nil, err
		}
		rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdRaw)
		rec.LastClientConnectAt = nullableToTime(connRaw)
		rec.LastClientDisconnectAt = nullableToTime(discRaw)
		rec.DisconnectedSince = nullableToTime(sinceRaw)
		records = append(records, rec)
	}
	return records, rows.Err()
}

// ReconcileSessionsOnStartup transitions all sessions that were active or
// disconnected at the last shutdown to failed. On restart, the backing runtime
// processes no longer exist, so these sessions are unrecoverable.
func (s *SQLiteStore) ReconcileSessionsOnStartup(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx,
		`UPDATE gateway_sessions SET status = 'failed', last_client_disconnect_at = ? WHERE status IN ('active', 'disconnected')`,
		now)
	return err
}

// AppendInboundDiagnostic stores a client-to-agent message for audit/debugging.
// Written via a buffered channel so the hot path never blocks on SQLite I/O.
func (s *SQLiteStore) AppendInboundDiagnostic(ctx context.Context, sessionID string, seq int64, payload string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO session_inbound_log(session_id, seq, payload, recorded_at) VALUES (?, ?, ?, ?)`,
		sessionID, seq, payload, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// timeToNullable formats a *time.Time for SQLite storage. Returns empty string
// for nil pointers so the column gets SQL NULL.
func timeToNullable(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// nullableToTime parses a SQLite text column back to *time.Time. Returns nil
// for empty strings (SQL NULL) or parse errors.
func nullableToTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return &t
}
