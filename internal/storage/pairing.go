package storage

import (
	"context"
	"encoding/json"
	"time"
)

type PairingRecord struct {
	DeviceID       string
	DeviceName     string
	Token          string
	ExpiresAt      time.Time
	Scopes         []string
	ProofPublicKey string
	// RetiredToken is the credential hash the last refresh rotated away, and
	// RetiredUntil is the deadline until which the gateway still exchanges it for
	// the live token. A client that lost the refresh response (or fired two
	// refreshes concurrently) holds the retired token; without this it can only
	// ever see "credential invalid", which the app treats as a dead pairing.
	// An empty RetiredToken means the device has no retired credential.
	RetiredToken string
	RetiredUntil time.Time
}

func (s *SQLiteStore) SavePairing(ctx context.Context, record PairingRecord) error {
	scopesJSON, err := json.Marshal(record.Scopes)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO paired_devices(device_id, device_name, token, expires_at, retired_token, retired_until)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(device_id) DO UPDATE SET
		   device_name = excluded.device_name,
		   token = excluded.token,
		   expires_at = excluded.expires_at,
		   retired_token = excluded.retired_token,
		   retired_until = excluded.retired_until`,
		record.DeviceID,
		record.DeviceName,
		record.Token,
		record.ExpiresAt.UTC().Format(time.RFC3339Nano),
		record.RetiredToken,
		formatOptionalTime(record.RetiredUntil),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO paired_device_scopes(device_id, scopes)
		 VALUES (?, ?)
		 ON CONFLICT(device_id) DO UPDATE SET scopes = excluded.scopes`,
		record.DeviceID,
		string(scopesJSON),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO paired_device_proofs(device_id, public_key)
		 VALUES (?, ?)
		 ON CONFLICT(device_id) DO UPDATE SET public_key = excluded.public_key`,
		record.DeviceID,
		record.ProofPublicKey,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) ListPairings(ctx context.Context) ([]PairingRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.device_id, p.device_name, p.token, p.expires_at, COALESCE(s.scopes, ''), COALESCE(k.public_key, ''), p.retired_token, p.retired_until FROM paired_devices p LEFT JOIN paired_device_scopes s ON s.device_id = p.device_id LEFT JOIN paired_device_proofs k ON k.device_id = p.device_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []PairingRecord
	for rows.Next() {
		var (
			record          PairingRecord
			expiresRaw      string
			scopesRaw       string
			proofKeyRaw     string
			retiredRaw      string
			retiredUntilRaw string
		)
		if err := rows.Scan(&record.DeviceID, &record.DeviceName, &record.Token, &expiresRaw, &scopesRaw, &proofKeyRaw, &retiredRaw, &retiredUntilRaw); err != nil {
			return nil, err
		}
		record.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresRaw)
		if err != nil {
			return nil, err
		}
		if scopesRaw != "" {
			if err := json.Unmarshal([]byte(scopesRaw), &record.Scopes); err != nil {
				return nil, err
			}
		}
		record.ProofPublicKey = proofKeyRaw
		record.RetiredToken = retiredRaw
		if retiredUntilRaw != "" {
			record.RetiredUntil, err = time.Parse(time.RFC3339Nano, retiredUntilRaw)
			if err != nil {
				return nil, err
			}
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// formatOptionalTime renders a timestamp column that may be absent, using the
// same RFC3339Nano encoding as the columns that are always present.
func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

// GetPairedDeviceIDs returns the device IDs of all currently paired devices.
func (s *SQLiteStore) GetPairedDeviceIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT device_id FROM paired_devices`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *SQLiteStore) DeletePairing(ctx context.Context, deviceID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `DELETE FROM paired_device_scopes WHERE device_id = ?`, deviceID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM paired_device_proofs WHERE device_id = ?`, deviceID)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM paired_devices WHERE device_id = ?`, deviceID)
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
	return tx.Commit()
}
