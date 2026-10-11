package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
)

type GatewaySettingsRecord struct {
	RegistryURL   string
	PublicBaseURL string
	EnableLAN     bool
	GatewayName   string
}

func (s *SQLiteStore) SaveGatewaySettings(ctx context.Context, record GatewaySettingsRecord) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO gateway_settings(settings_key, registry_url, public_base_url, enable_lan, gateway_name)
		 VALUES ('default', ?, ?, ?, ?)
		 ON CONFLICT(settings_key) DO UPDATE SET
		   registry_url = excluded.registry_url,
		   public_base_url = excluded.public_base_url,
		   enable_lan = excluded.enable_lan,
		   gateway_name = excluded.gateway_name`,
		record.RegistryURL,
		record.PublicBaseURL,
		boolToSQLiteInt(record.EnableLAN),
		record.GatewayName,
	)
	return err
}

func (s *SQLiteStore) GetGatewaySettings(ctx context.Context) (GatewaySettingsRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT registry_url, public_base_url, enable_lan, gateway_name
		 FROM gateway_settings
		 WHERE settings_key = 'default'`,
	)

	var (
		record    GatewaySettingsRecord
		enableLAN int
	)
	if err := row.Scan(&record.RegistryURL, &record.PublicBaseURL, &enableLAN, &record.GatewayName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GatewaySettingsRecord{}, ErrNotFound
		}
		return GatewaySettingsRecord{}, err
	}
	record.EnableLAN = enableLAN != 0
	return record, nil
}

// EnsureGatewayID returns this gateway's stable instance identifier, generating
// and persisting one on first call. Unlike the gateway name (which the user can
// rename freely), this id never changes for the lifetime of the state database —
// it is the gateway-owned identity paired clients store to address it, e.g. to
// resolve a push back to the right server entry for deep-linking.
func (s *SQLiteStore) EnsureGatewayID(ctx context.Context) (string, error) {
	row := s.db.QueryRowContext(ctx, `SELECT gateway_id FROM gateway_identity WHERE id = 1`)
	var id string
	switch err := row.Scan(&id); {
	case err == nil:
		return id, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", err
	}

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	id = hex.EncodeToString(buf)
	// INSERT OR IGNORE guards against a concurrent first-boot insert racing us;
	// re-read so we return whichever id actually won.
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO gateway_identity(id, gateway_id) VALUES (1, ?)`, id); err != nil {
		return "", err
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT gateway_id FROM gateway_identity WHERE id = 1`).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

// EnsureVAPIDKeys returns this gateway's Web Push signing key pair, creating it
// with generate on first call. It must never change: every client's push
// subscription is bound to the public key it was created with.
func (s *SQLiteStore) EnsureVAPIDKeys(ctx context.Context, generate func() (privateKey, publicKey string, err error)) (publicKey, privateKey string, err error) {
	query := `SELECT public_key, private_key FROM vapid_keys WHERE id = 1`
	switch err := s.db.QueryRowContext(ctx, query).Scan(&publicKey, &privateKey); {
	case err == nil:
		return publicKey, privateKey, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", "", err
	}
	privateKey, publicKey, err = generate()
	if err != nil {
		return "", "", err
	}
	// Same first-boot race guard as EnsureGatewayID: keep whichever pair won.
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO vapid_keys(id, public_key, private_key) VALUES (1, ?, ?)`, publicKey, privateKey); err != nil {
		return "", "", err
	}
	if err := s.db.QueryRowContext(ctx, query).Scan(&publicKey, &privateKey); err != nil {
		return "", "", err
	}
	return publicKey, privateKey, nil
}
