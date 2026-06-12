# API Surface

Ferngeist Gateway exposes two HTTP surfaces:

- **Public API** for paired clients and ACP agent control
- **Admin API** for local management and diagnostics

The public API is the one clients use most of the time. It exposes a WebSocket bridge for ACP traffic plus the endpoints needed for pairing, status, runtime control, resilient sessions, and push notification registration.

The admin API is bound to localhost and is intended for local setup, recovery, and management.

## Public API

Base path: `/v1`

### Health

- `GET /healthz`
  - Returns a simple health check response.

### Status and discovery

- `GET /v1/status`
  - Returns gateway status, build info, discovery state, remote access state, registry status, and runtime counts.

- `GET /v1/agents`
  - Returns the supported agent catalog merged with live runtime state.
  - Requires a paired client.

### Pairing

- `POST /v1/pair/start`
  - Starts a new pairing challenge.
  - Returns the challenge ID and expiration time.

- `POST /v1/pair/complete`
  - Completes pairing with a challenge ID, code, and device name.
  - Returns the issued device credential.

- `GET /v1/pair/status/{challengeId}`
  - Returns the current pairing state for a challenge.
  - Does not expose the pairing code.

### Authentication

- `POST /v1/auth/refresh`
  - Refreshes a paired device token.
  - Invalidates the old token immediately.

### Diagnostics

- `GET /v1/diagnostics/summary`
  - Returns a compact runtime health summary.
  - Requires `read` scope.

- `GET /v1/diagnostics/export`
  - Returns a full diagnostic bundle with runtime state and logs.
  - Disabled unless remote diagnostics export is allowed.
  - Requires `read` scope.

### Agent control

- `POST /v1/agents/{agentId}/start`
  - Starts the selected agent runtime.
  - Requires `control` scope.

- `POST /v1/agents/{agentId}/stop`
  - Stops the selected agent runtime.
  - Requires `control` scope.

### Runtime control

- `GET /v1/runtimes`
  - Lists managed ACP runtimes.
  - Requires a paired client.

- `GET /v1/runtimes/{runtimeId}/logs`
  - Returns buffered logs for a runtime.
  - Requires `read` scope.

- `POST /v1/runtimes/{runtimeId}/connect`
  - Creates a runtime token and starts a gateway session for ACP traffic.
  - Requires `control` scope.
  - Response:
    ```json
    {
      "runtimeId": "string",
      "protocol": "string",
      "scheme": "string",
      "host": "string",
      "websocketUrl": "string",
      "websocketPath": "string",
      "bearerToken": "string",
      "tokenExpiresAt": "2026-05-22T10:00:00Z",
      "sessionId": "string",
      "attachToken": "string"
    }
    ```
  - Creates a persistent gateway session with push notification support. The `sessionId` and `attachToken` fields are populated. The client should store `sessionId` for reconnection.
  - The `sessionId` is a **gateway session identifier** (an opaque random string that identifies the gateway's internal session object). It is NOT an ACP agent session ID — that is negotiated between the client and agent during ACP initialization and is unrelated to this field.
  - Session creation is best-effort. If it fails, the response still contains a valid connection descriptor (the `sessionId`/`attachToken` fields are simply empty).

- `POST /v1/runtimes/{runtimeId}/restart`
  - Restarts a runtime, optionally with environment overrides.
  - Requires `control` scope.
  - Runtime restart with environment overrides may be disabled by configuration.

### Gateway sessions

> **Terminology note:** A "gateway session" is a gateway-internal object that keeps a runtime alive across WebSocket disconnections. It manages a stdio pump (for agent stdout), an exclusive pipe lease, and push notification dispatch on notable events. It is not an ACP agent session — ACP agent sessions are negotiated between the client and agent during protocol initialization and are not tracked by this API.

- `POST /v1/sessions/{sessionId}/resume`
  - Prepares a disconnected gateway session for WebSocket reconnection.
  - Authenticated with the device credential (bearer token).
  - Returns a new single-use attach token.
  - Response:
    ```json
    {
      "attachToken": "string"
    }
    ```
  - Error responses:
    - `400` — session not found, device mismatch, or session is in a non-resumable status
    - `401` — invalid or missing device credential
    - `503` — session service not available

- `GET /v1/sessions`
  - Lists all gateway sessions belonging to the authenticated device.
  - Authenticated with the device credential (bearer token).
  - Results ordered by `created_at DESC` (newest first).
  - Response:
    ```json
    {
      "sessions": [
        {
          "sessionId": "string",
          "runtimeId": "string",
          "agentId": "string",
          "status": "active",
          "createdAt": "2026-05-22T10:00:00Z"
        }
      ]
    }
    ```
  - Status values: `active`, `disconnected`, `closing`, `failed`

- `DELETE /v1/sessions/{sessionId}`
  - Closes a gateway session. Stops the backing runtime, releases the pipe lease, and deletes all session data.
  - Authenticated with the device credential (bearer token).
  - Response: `204 No Content`
  - Error responses:
    - `400` — session not found or device mismatch
    - `401` — invalid or missing device credential
    - `503` — session service not available

### Push notification registration

- `POST /v1/devices/fcm-token`
  - Registers or updates the FCM push notification token for the authenticated device.
  - Authenticated with the device credential (bearer token).
  - Request body:
    ```json
    {
      "fcmToken": "string"
    }
    ```
  - Response: `204 No Content`
  - Error responses:
    - `400` — missing or empty fcmToken
    - `401` — invalid or missing device credential

### ACP bridge

- `GET /v1/acp/{runtimeId}`
  - WebSocket endpoint for ACP traffic.
  - Pass `?sessionId=<id>&attachToken=<token>` as query params. The session ID and initial attach token are obtained from `POST /v1/runtimes/{id}/connect`. For reconnects, a fresh attach token is obtained from `POST /v1/sessions/{id}/resume`.
  - On reconnect, the client is responsible for calling the ACP `session/load` method on the agent for context restoration. The gateway does not replay old frames.

### Attach tokens

Attach tokens are single-use, short-lived (5-minute TTL) nonces used to prove ownership of a gateway session at WebSocket connect time. They are:

- Minted on gateway session creation (`POST /v1/runtimes/{id}/connect`)
- Minted on gateway session resume (`POST /v1/sessions/{id}/resume`)
- Consumed on first WebSocket connect (`GET /v1/acp/{runtimeId}?sessionId=&attachToken=`)
- 64 hex characters (32 random bytes)
- Stored in memory only (not persisted to SQLite)

## Admin API

Base path: `/admin/v1`

The admin API is bound to localhost and is meant for local management only.

### Status

- `GET /admin/v1/status`
  - Returns detailed daemon status, including pairing target info and active pairing state.

### Pairing management

- `POST /admin/v1/pairings/start`
  - Starts a pairing challenge locally.
  - Returns the pairing code and deep-link payload.

- `GET /admin/v1/pairings/{challengeId}`
  - Returns pairing state for a challenge.

- `DELETE /admin/v1/pairings/{challengeId}`
  - Cancels an active pairing challenge.

### Device management

- `GET /admin/v1/devices`
  - Lists paired devices.

- `DELETE /admin/v1/devices/{deviceId}`
  - Revokes a paired device.

## Common response patterns

### Success

Most endpoints return JSON.

### Errors

Errors use a simple JSON envelope:

```json
{
  "error": "message"
}
```

### Authentication and scopes

- Some public endpoints require a paired device token.
- Some endpoints also require a scope such as `read` or `control`.
- Public-mode deployments may require proof-of-possession headers.

See [docs/security.md](docs/security.md) for the security model and remote access notes.

### WebSocket usage

The ACP WebSocket endpoint is the primary transport for agent traffic.

**Resilient gateway session flow (disconnect-tolerant):**

1. Pair a device.
2. `POST /v1/runtimes/{id}/connect` — get `sessionId`, `attachToken`, and connection details.
3. `GET /v1/acp/{runtimeId}?sessionId=<id>&attachToken=<token>` — WebSocket connect.
4. Exchange ACP messages. Disconnect does NOT kill the runtime; the gateway session stays `disconnected`. While disconnected, push notifications may be dispatched on turn complete, permission requests, or agent errors.
5. Register FCM token: `POST /v1/devices/fcm-token` (authenticated with device credential).
6. To reconnect: `POST /v1/sessions/{id}/resume` (authenticated with device credential) — get new `attachToken`.
7. `GET /v1/acp/{runtimeId}?sessionId=<id>&attachToken=<token>` — live proxying resumes. The client calls `session/load` on the agent for context restoration.
8. `DELETE /v1/sessions/{id}` — close the gateway session and stop the runtime.

## Notes

This document is intentionally high-level. For implementation details, see the code in `internal/api` and `internal/session`.
