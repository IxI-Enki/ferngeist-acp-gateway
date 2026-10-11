package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// CustomAgentRecord is a user-defined ACP agent registered by a paired client.
// The id is server-assigned (custom-<slug>) and immutable; the record carries
// only what the registry cannot supply: a display name, the executable, its
// args, and an optional hint.
type CustomAgentRecord struct {
	ID          string
	DisplayName string
	Command     string
	Args        []string
	Hint        string
	CreatedAt   time.Time
}

// SaveCustomAgent inserts or updates a custom agent. The upsert keeps the
// original created_at so updates are distinguishable from re-registrations.
// Args are stored as a JSON array (like paired_device_scopes), normalizing a
// nil slice to '[]' to match the column default.
func (s *SQLiteStore) SaveCustomAgent(ctx context.Context, record CustomAgentRecord) error {
	argsJSON, err := json.Marshal(record.Args)
	if err != nil {
		return err
	}
	if record.Args == nil {
		argsJSON = []byte("[]")
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO custom_agents(agent_id, display_name, command, args, hint, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(agent_id) DO UPDATE SET
		   display_name = excluded.display_name,
		   command = excluded.command,
		   args = excluded.args,
		   hint = excluded.hint`,
		record.ID, record.DisplayName, record.Command, string(argsJSON),
		record.Hint, record.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLiteStore) ListCustomAgents(ctx context.Context) ([]CustomAgentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT agent_id, display_name, command, args, hint, created_at FROM custom_agents ORDER BY agent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CustomAgentRecord
	for rows.Next() {
		var r CustomAgentRecord
		var argsRaw, createdRaw string
		if err := rows.Scan(&r.ID, &r.DisplayName, &r.Command, &argsRaw, &r.Hint, &createdRaw); err != nil {
			return nil, err
		}
		if argsRaw != "" {
			if err := json.Unmarshal([]byte(argsRaw), &r.Args); err != nil {
				return nil, err
			}
		}
		r.CreatedAt, err = time.Parse(time.RFC3339Nano, createdRaw)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) GetCustomAgent(ctx context.Context, id string) (CustomAgentRecord, error) {
	var r CustomAgentRecord
	var argsRaw, createdRaw string
	err := s.db.QueryRowContext(ctx, `SELECT agent_id, display_name, command, args, hint, created_at FROM custom_agents WHERE agent_id = ?`, id).
		Scan(&r.ID, &r.DisplayName, &r.Command, &argsRaw, &r.Hint, &createdRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return CustomAgentRecord{}, ErrNotFound
	}
	if err != nil {
		return CustomAgentRecord{}, err
	}
	if argsRaw != "" {
		if err := json.Unmarshal([]byte(argsRaw), &r.Args); err != nil {
			return CustomAgentRecord{}, err
		}
	}
	r.CreatedAt, err = time.Parse(time.RFC3339Nano, createdRaw)
	if err != nil {
		return CustomAgentRecord{}, err
	}
	return r, nil
}

func (s *SQLiteStore) DeleteCustomAgent(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM custom_agents WHERE agent_id = ?`, id)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) CountCustomAgents(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM custom_agents`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
