# Architecture

Ferngeist Gateway is a self-hosted backend for ACP-compatible agents.

Its main job is to expose ACP agents through a unified WebSocket API. It discovers supported agents, launches them when needed, manages pairing so clients can talk to them through one authenticated endpoint, and keeps agents alive across WebSocket disconnections with resilient sessions, push notification wake-up, and seamless reconnection.

## Main parts

- `cmd/ferngeist` — CLI entrypoint for daemon, pairing, and device management
- `internal/api` — public and admin HTTP APIs, WebSocket bridge, session handlers
- `internal/push` — push notification service interface and log-based implementation
- `internal/gateway` — runtime token issuance, attach token and validation
- `internal/pairing` — pairing flow and device credentials
- `internal/runtime` — process supervision, lease-based transport bridging
- `internal/session` — resilient session domain: session lifecycle, stdio pump, inbound diagnostics
- `internal/token` — attach token hashing and validation
- `internal/catalog` — supported agent discovery and validation
- `internal/registry` — ACP registry fetch and cache
- `internal/storage` — SQLite persistence (sessions, pairings, runtimes, FCM tokens, inbound diagnostics)
- `internal/discovery` — LAN advertising via mDNS
- `internal/config` — configuration and persisted settings
- `internal/daemon` — wiring and startup reconciliation

## Resilient gateway sessions

Every connection creates a persistent gateway session with:

- **StdioPump** — a long-lived goroutine that drains agent stdout and forwards frames to the WebSocket when a client is attached. When no client is connected, output is discarded after end-turn detection and log append. The pump owns the pipe lifecycle and runs independently of WebSocket connectivity.
- **Lease** — the session holds an exclusive lease on the runtime's stdio pipes via `AcquireLease`/`ReleaseLease`. The runtime is not stopped on WebSocket disconnect — only the leaseholder string is cleared. Sessions always stop the runtime on close.
- **Push notifications** — when no WebSocket client is attached and the pump detects a notable event in the agent's stdout (turn complete, permission request, or agent error), it fires a callback that sends a push notification via `PushSvc.SendNotification`. The client reconnects and calls `session/load` on the agent for context restoration.
- **Inbound diagnostics** — client-to-agent frames are logged asynchronously to SQLite via a buffered channel (non-blocking, dropped on overflow with counter).
- **ACP session/close** — before stopping the runtime on session close, the gateway sends a `session/close` JSON-RPC request to the agent if it advertised `sessionCapabilities.close` during initialize. The mock agent supports this for testing.

There is no ring buffer or catchup replay. On WebSocket disconnect:
1. The pump continues running, discarding agent output.
2. On notable events (turn complete, permission request, agent error), a push notification is dispatched (if a push service is configured).
3. The client reconnects, calls `session/load` on the agent, and resumes live proxying.

## Session lifecycle

1. `POST /v1/runtimes/{id}/connect` → session created, connect descriptor with bearer token, sessionId, and attachToken returned
2. `GET /v1/acp/{runtimeId}?sessionId=<id>&attachToken=<token>` → initial WebSocket connect
3. ACP messages flow bidirectionally via pump + proxy
4. WebSocket disconnects → pump keeps running, session → `disconnected`
5. `POST /v1/sessions/{id}/resume` → new attach token minted
6. `GET /v1/acp/{runtimeId}?sessionId=&attachToken=` → sets pump client, live proxying resumes (client calls `session/load` on agent for context)
7. `DELETE /v1/sessions/{id}` → mark `closing`, stop pump, send ACP `session/close`, stop runtime (2s graceful timeout), release lease, delete from storage

### Session close ordering

1. Mark session status `closing` in SQLite
2. Stop the stdio pump (context cancellation)
3. If the agent advertised `sessionCapabilities.close`, send `{"jsonrpc":"2.0","method":"session/close",...}` to stdin
4. `StopByRuntimeID` with 2-second graceful timeout
5. Release the pipe lease
6. Delete session record from SQLite

## Data flow

1. A client pairs with the gateway.
2. The gateway stores device credentials.
3. The client requests an ACP agent through the API.
4. The gateway launches or connects to the target agent.
5. ACP traffic is bridged over WebSocket through a single authenticated endpoint.
6. The bridge survives WebSocket disconnection via the stdio pump + push notification wake-up.

## Push notifications

- `internal/push/service.go` defines the `PushService` interface with `SendNotification` and `RegisterDevice`.
- `internal/push/log.go` provides `LogPushService`, a no-op implementation that logs push events via slog. Used by default (no real FCM integration yet).
- On turn events (turn complete, permission request, or agent error) detected in the pump, it calls `PushSvc.SendNotification` with a 10-second context timeout.
- Config.PushSvc is nil-able; when nil, push is disabled.
- FCM tokens are stored via `POST /v1/devices/fcm-token` and persisted in the `device_fcm_tokens` SQLite table.

## Inbound diagnostics

Client-to-agent messages are recorded for debugging/audit. Direction is always client→agent. Never replayed. Written to SQLite `session_inbound_log` table asynchronously via a buffered channel (256 entries). Non-blocking send — if the channel is full, the frame is dropped and a counter is incremented. The hot path never blocks on I/O.

## Attach tokens

Single-use, short-lived (5-minute TTL) nonces used to prove ownership of a session at WebSocket connect time. Minted on session creation and resume. Consumed on first WebSocket connect. 64 hex characters (32 random bytes). Stored in memory only (not persisted to SQLite). Hashed via SHA-256 for storage; token service in `internal/token/`.

## Startup reconciliation

On daemon restart, all sessions in `active` or `disconnected` status are transitioned to `failed` in SQLite, since their backing processes are gone.

## Notes

- Agent support comes from the ACP registry plus local helper adapters.
- The gateway can auto-acquire managed binaries when supported.
- The Ferngeist Android app uses this service as its backend.
- See [docs/api.md](api.md) for the full API surface, including session endpoints and FCM token registration.
