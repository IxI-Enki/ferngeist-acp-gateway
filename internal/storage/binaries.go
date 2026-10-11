package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type AcquiredBinaryRecord struct {
	AgentID     string
	Version     string
	Path        string
	ArchiveURL  string
	InstalledAt time.Time
}

func (s *SQLiteStore) SaveAcquiredBinary(ctx context.Context, record AcquiredBinaryRecord) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO acquired_binaries(agent_id, version, path, archive_url, installed_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(agent_id) DO UPDATE SET
		   version = excluded.version,
		   path = excluded.path,
		   archive_url = excluded.archive_url,
		   installed_at = excluded.installed_at`,
		record.AgentID,
		record.Version,
		record.Path,
		record.ArchiveURL,
		record.InstalledAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLiteStore) GetAcquiredBinary(ctx context.Context, agentID string) (AcquiredBinaryRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT agent_id, version, path, archive_url, installed_at
		 FROM acquired_binaries
		 WHERE agent_id = ?`,
		agentID,
	)

	var (
		record       AcquiredBinaryRecord
		installedRaw string
	)
	if err := row.Scan(&record.AgentID, &record.Version, &record.Path, &record.ArchiveURL, &installedRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AcquiredBinaryRecord{}, ErrNotFound
		}
		return AcquiredBinaryRecord{}, err
	}
	installedAt, err := time.Parse(time.RFC3339Nano, installedRaw)
	if err != nil {
		return AcquiredBinaryRecord{}, err
	}
	record.InstalledAt = installedAt
	return record, nil
}
