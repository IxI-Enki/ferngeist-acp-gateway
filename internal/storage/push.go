package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// SaveDevicePushToken upserts a device's push registration token and the platform
// it was issued for ("android", "ios", "web", …). The platform is the routing key
// the push dispatcher uses to pick the right delivery provider, so it is stored
// alongside the token rather than inferred at send time. One token per device:
// tokens rotate, and a re-registration replaces the prior token.
func (s *SQLiteStore) SaveDevicePushToken(ctx context.Context, deviceID, token, platform string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO device_push_tokens(device_id, token, platform, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(device_id) DO UPDATE SET
		   token = excluded.token,
		   platform = excluded.platform,
		   updated_at = excluded.updated_at`,
		deviceID, token, platform, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// GetDevicePushToken returns the device's registered push token and its platform,
// or ("", "", nil) if the device has never registered one. A missing token is a
// normal state — the user may not have granted notification permission — so it is
// reported as a zero result rather than ErrNotFound, letting the push dispatcher
// skip delivery quietly.
func (s *SQLiteStore) GetDevicePushToken(ctx context.Context, deviceID string) (token, platform string, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT token, platform FROM device_push_tokens WHERE device_id = ?`, deviceID)
	if err := row.Scan(&token, &platform); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	return token, platform, nil
}

// DeleteDevicePushToken removes a device's push token. The dispatcher calls this
// when a provider reports the token as permanently unregistered (the app was
// uninstalled or the token rotated), so the dead token is not retried.
func (s *SQLiteStore) DeleteDevicePushToken(ctx context.Context, deviceID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM device_push_tokens WHERE device_id = ?`, deviceID)
	return err
}
