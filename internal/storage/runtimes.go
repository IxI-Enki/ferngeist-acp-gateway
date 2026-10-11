package storage

import (
	"context"
	"time"
)

// maxRuntimeFailureRecords bounds the runtime_failures table: every process
// exit appends a row, so without retention the table grows forever. Summary
// only reads the newest handful, so evicting the oldest is diagnostics-safe.
const maxRuntimeFailureRecords = 200

type RuntimeRecord struct {
	RuntimeID string
	AgentID   string
	AgentName string
	Status    string
	Command   string
	PID       int
	LastError string
	CreatedAt time.Time
}

type RuntimeFailureRecord struct {
	RuntimeID  string
	AgentID    string
	AgentName  string
	LastError  string
	CreatedAt  time.Time
	FailedAt   time.Time
	LogPreview string
	// CleanExit marks a record that captures a clean (exit code 0) process exit
	// rather than a crash. Clean-exit records are persisted so every agent death
	// leaves a diagnostic trace; they are excluded from Summary.RecentFailures.
	CleanExit bool
}

// SaveRuntime persists the latest runtime snapshot rather than a full event
// history. This keeps the store small and restart recovery simple.
func (s *SQLiteStore) SaveRuntime(ctx context.Context, record RuntimeRecord) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO runtimes(runtime_id, agent_id, agent_name, status, command, pid, last_error, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(runtime_id) DO UPDATE SET
		   agent_id = excluded.agent_id,
		   agent_name = excluded.agent_name,
		   status = excluded.status,
		   command = excluded.command,
		   pid = excluded.pid,
		   last_error = excluded.last_error,
		   created_at = excluded.created_at`,
		record.RuntimeID,
		record.AgentID,
		record.AgentName,
		record.Status,
		record.Command,
		record.PID,
		record.LastError,
		record.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLiteStore) SaveRuntimeFailure(ctx context.Context, record RuntimeFailureRecord) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO runtime_failures(runtime_id, agent_id, agent_name, last_error, created_at, failed_at, log_preview, clean)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.RuntimeID,
		record.AgentID,
		record.AgentName,
		record.LastError,
		record.CreatedAt.UTC().Format(time.RFC3339Nano),
		record.FailedAt.UTC().Format(time.RFC3339Nano),
		record.LogPreview,
		boolToSQLiteInt(record.CleanExit),
	)
	if err != nil {
		return err
	}
	// Retention: every exit appends a row, so evict everything past the
	// newest cap or the table grows without bound.
	_, err = s.db.ExecContext(
		ctx,
		`DELETE FROM runtime_failures WHERE id NOT IN (
			SELECT id FROM runtime_failures ORDER BY failed_at DESC, id DESC LIMIT ?
		)`,
		maxRuntimeFailureRecords,
	)
	return err
}

// ListRecentRuntimeFailures returns persisted failure summaries for diagnostics.
// Clean-exit records (CleanExit=true) are excluded — they are kept for audit
// trace but are not failures. It deliberately stores a bounded log preview
// instead of full runtime logs.
func (s *SQLiteStore) ListRecentRuntimeFailures(ctx context.Context, limit int) ([]RuntimeFailureRecord, error) {
	return s.listRuntimeFailureRecords(ctx, limit, false)
}

// ListRuntimeFailureRecords returns persisted process-exit records including
// clean exits, newest first. It exists so clean-exit records remain readable
// back (they are persisted for diagnostics but excluded from
// ListRecentRuntimeFailures and Summary.RecentFailures).
func (s *SQLiteStore) ListRuntimeFailureRecords(ctx context.Context, limit int) ([]RuntimeFailureRecord, error) {
	return s.listRuntimeFailureRecords(ctx, limit, true)
}

func (s *SQLiteStore) listRuntimeFailureRecords(ctx context.Context, limit int, includeClean bool) ([]RuntimeFailureRecord, error) {
	if limit <= 0 {
		limit = 5
	}

	where := ""
	if !includeClean {
		where = "WHERE clean = 0"
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT runtime_id, agent_id, agent_name, last_error, created_at, failed_at, log_preview, clean
		 FROM runtime_failures
		 `+where+`
		 ORDER BY failed_at DESC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []RuntimeFailureRecord
	for rows.Next() {
		var (
			record     RuntimeFailureRecord
			createdRaw string
			failedRaw  string
			cleanRaw   int
		)
		if err := rows.Scan(
			&record.RuntimeID,
			&record.AgentID,
			&record.AgentName,
			&record.LastError,
			&createdRaw,
			&failedRaw,
			&record.LogPreview,
			&cleanRaw,
		); err != nil {
			return nil, err
		}
		record.CleanExit = cleanRaw != 0
		record.CreatedAt, err = time.Parse(time.RFC3339Nano, createdRaw)
		if err != nil {
			return nil, err
		}
		record.FailedAt, err = time.Parse(time.RFC3339Nano, failedRaw)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}
