// Package session manages durable, reconnectable agent sessions for the gateway.
// This file contains all RuntimeSession method implementations — lifecycle
// (Create, Resume, Attach, Detach, Close), query (ListByDevice, GetPump,
// GetSessionStatus, FindReconnectableByRuntime), notification
// (sendPushNotification, handleProcessExit), diagnostics (LogInbound),
package session

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/catalog"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/push"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/runtime"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/storage"
	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
)

// Create launches the agent and establishes a resilient session that owns its
// process. It is best-effort: if the session record cannot be saved after the
// process was launched, the process is stopped so no orphan remains.
func (rs *RuntimeSession) Create(ctx context.Context, agent catalog.Agent, deviceID string, env map[string]string) (*Session, string, error) {
	rs.mu.Lock()
	if rs.closed {
		rs.mu.Unlock()
		return nil, "", ErrSessionNotActive
	}
	if rs.cfg.MaxPerDevice > 0 {
		existing, err := rs.store.ListSessionsByDevice(ctx, deviceID)
		if err == nil {
			// Only live sessions (active or disconnected) consume a device's quota.
			// Failed and closing records are dead — they linger in storage after a
			// crash or a daemon restart's startup reconciliation, and counting them
			// would permanently exhaust the limit, blocking every new session for the
			// device even though none are actually running.
			live := 0
			for _, rec := range existing {
				if rec.Status == StatusActive || rec.Status == StatusDisconnected {
					live++
				}
			}
			if live >= rs.cfg.MaxPerDevice {
				rs.mu.Unlock()
				return nil, "", ErrSessionLimitReached
			}
		}
	}
	sessionID := generateID()
	rs.mu.Unlock()

	// Connecting starts the agent: the session owns the process it launches.
	proc, err := rs.runtimeSvc.Launch(agent, env)
	if err != nil {
		return nil, "", err
	}
	runtimeID := proc.Runtime.ID

	now := time.Now().UTC()
	rec := storage.SessionRecord{
		SessionID:   sessionID,
		RuntimeID:   runtimeID,
		DeviceID:    deviceID,
		AgentID:     agent.ID,
		Status:      StatusDisconnected,
		CreatedAt:   now,
		// Born disconnected: the reaper reclaims a session whose client never
		// attaches. AttachClient flips it to active.
		DisconnectedSince: &now,
	}
	if err := rs.store.SaveSession(ctx, rec); err != nil {
		_ = proc.Stop(2 * time.Second)
		return nil, "", err
	}

	pumpCtx, pumpCancel := context.WithCancel(context.Background())

	onPushNotification := func(e PushEvent) {
		rs.mu.Lock()
		s, ok := rs.sessions[e.SessionID]
		rs.mu.Unlock()
		if !ok || s.DeviceID == "" {
			return
		}
		// Dispatch asynchronously: this runs on the stdout drain-loop goroutine,
		// and a push is a token lookup plus a network round-trip to the provider.
		// Blocking here would stall agent stdout draining — and any attached
		// client's live stream — until the push completes or times out.
		rs.sendPushNotification(s.DeviceID, e.AcpSessionID, s.pump.AcpCwdFor(e.AcpSessionID), e.Title, e.Body, e.Category)
	}

	pump := &StdioPump{
		proc:               proc,
		runtimeID:          runtimeID,
		sessionID:          sessionID,
		agentID:            agent.ID,
		logger:             rs.logger,
		appendLog:          rs.runtimeSvc.AppendLog,
		onPushNotification: onPushNotification,
		ProgressInterval:   rs.cfg.ProgressInterval,
		frameLog:           rs.frameLog,
		loadRecovery:       newLoadRecovery(rs.logger),
	}

	go pump.StdoutDrainLoop(pumpCtx)

	sess := &Session{
		ID:             sessionID,
		RuntimeID:      runtimeID,
		DeviceID:       deviceID,
		AgentID:        agent.ID,
		Status:         StatusDisconnected,
		CreatedAt:      now,
		DisconnectedAt: &now,
		proc:           proc,
		pump:           pump,
		cancelPump:     pumpCancel,
		exited:         make(chan struct{}),
	}

	rs.mu.Lock()
	if rs.closed {
		rs.mu.Unlock()
		pumpCancel()
		_ = proc.Stop(2 * time.Second)
		_ = rs.store.DeleteSession(context.Background(), sessionID)
		return nil, "", ErrSessionNotActive
	}
	rs.sessions[sessionID] = sess
	rs.mu.Unlock()

	// The session owns the process, so it watches for the exit itself: record
	// it on the runtime and reclaim the session if the agent died on its own.
	go rs.watchProcess(sess, proc)

	attachToken, err := rs.tokenSvc.Mint(sessionID, deviceID, 5*time.Minute)
	if err != nil {
		attachToken = "" // best-effort: session is created but reconnection requires a token
	}

	return sess, attachToken, nil
}

// watchProcess waits for the session's agent process to exit and records the
// exit. The session owns the process, so this is the replacement for the old
// supervisor exit callback.
func (rs *RuntimeSession) watchProcess(sess *Session, proc *runtime.Process) {
	<-proc.Done()
	intentional := sess.stopping.Load()
	rs.runtimeSvc.RecordExit(sess.RuntimeID, proc.Err(), intentional)
	rs.handleProcessExit(sess, intentional)
	if sess.exited != nil {
		close(sess.exited)
	}
}

// sendPushNotification dispatches a push notification asynchronously with a 10s
// timeout so a slow or failing provider never blocks the caller. No-op when push
// notifications are not configured (PushSvc is nil).
func (rs *RuntimeSession) sendPushNotification(deviceID, acpSessionID, cwd, title, body, category string) {
	if rs.cfg.PushSvc == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := rs.cfg.PushSvc.Notify(ctx, deviceID, push.Notification{
			Title:     title,
			Body:      body,
			Category:  category,
			ServerID:  rs.cfg.GatewayID,
			SessionID: acpSessionID,
			// The client resumes the chat with this cwd; without it the load fails.
			Cwd: cwd,
		})
		if err != nil {
			rs.logger.Warn("push notification failed", "device_id", deviceID, "category", category, "error", err)
		}
	}()
}

// handleProcessExit reclaims a session whose agent process died. It detects
// exits not preceded by a close (which sets StatusClosing) and, for a genuine
// crash, removes the session from the registry and store and fires a crash push.
// An intentional exit finds the session already removed and no-ops.
func (rs *RuntimeSession) handleProcessExit(sess *Session, intentional bool) {
	rs.mu.Lock()
	var deviceIDForPush string
	var acpSessionIDForPush string
	var cwdForPush string
	var crashed bool
	var agentForPush string
	if s, ok := rs.sessions[sess.ID]; ok {
		s.mu.Lock()
		// A session still present and not closing died on its own. Close and the
		// reaper mark the session closing *before* stopping the process, so an
		// intentional exit never reaches here with a live registry entry.
		if s.Status != StatusClosing {
			crashed = true
			// Vestigial: this in-memory write is not persisted (the session is
			// deleted below), but kept for any diagnostic that reads the
			// in-memory status before reclamation completes.
			s.Status = StatusFailed
			deviceIDForPush = s.DeviceID
			// ACP session id (the id the client navigates by), for the crash push.
			acpSessionIDForPush = s.pump.AcpSessionID()
			cwdForPush = s.pump.AcpCwdFor(acpSessionIDForPush)
			agentForPush = s.pump.AgentName("Your agent")
		}
		s.mu.Unlock()
		if crashed {
			// A crashed session is dead and unrecoverable — its agent process is
			// gone. Reclaim it fully so it cannot linger as a "failed" record. The
			// crash push below still fires so the client is notified.
			if s.cancelPump != nil {
				s.cancelPump()
			}
			delete(rs.sessions, sess.ID)
			_ = rs.store.DeleteSession(context.Background(), sess.ID)
		}
	}
	rs.mu.Unlock()

	// Notify on a genuine crash (not an intentional stop) regardless of client
	// attachment; the client decides whether to surface it based on its own
	// foreground/background state. Dispatched asynchronously in sendPushNotification.
	if crashed && !intentional && deviceIDForPush != "" {
		rs.sendPushNotification(deviceIDForPush, acpSessionIDForPush, cwdForPush, "Agent Crashed", agentForPush+" has stopped unexpectedly.", push.CategoryAgentCrash)
	}
}

// FindReconnectableByRuntime returns the ID of the existing reconnectable
// session (active or disconnected) for a runtime owned by the given device, if
// one exists. A runtime has at most one session, because connecting starts the
// agent and the session owns its single process. This lets a reconnect reuse
// the live session — and its still-running agent — instead of launching a
// second process.
func (rs *RuntimeSession) FindReconnectableByRuntime(runtimeID, deviceID string) (string, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, sess := range rs.sessions {
		sess.mu.Lock()
		match := sess.RuntimeID == runtimeID && sess.DeviceID == deviceID &&
			(sess.Status == StatusActive || sess.Status == StatusDisconnected)
		id := sess.ID
		sess.mu.Unlock()
		if match {
			return id, true
		}
	}
	return "", false
}

// WorkingDir returns the ACP project directory for the session bound to the
// given runtime, or ErrSessionNotFound if no session references that runtime,
// or ErrCwdUnknown if the session exists but the client has not issued
// session/new yet (so params.cwd was never captured). A non-empty acpSessionID
// selects that ACP session's cwd; empty means the most recently opened one.
func (rs *RuntimeSession) WorkingDir(runtimeID, acpSessionID string) (string, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, sess := range rs.sessions {
		// RuntimeID is immutable after Create, but hold sess.mu for the read to
		// match FindReconnectableByRuntime's locking discipline.
		sess.mu.Lock()
		match := sess.RuntimeID == runtimeID
		sess.mu.Unlock()
		if match {
			cwd := sess.pump.AcpCwd()
			if acpSessionID != "" {
				cwd = sess.pump.AcpCwdFor(acpSessionID)
			}
			if cwd == "" {
				return "", ErrCwdUnknown
			}
			return cwd, nil
		}
	}
	return "", ErrSessionNotFound
}

// Resume mints a new single-use attach token for reconnecting to an existing session.
func (rs *RuntimeSession) Resume(ctx context.Context, sessionID, deviceID string) (string, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	sess, ok := rs.sessions[sessionID]
	if !ok {
		return "", ErrSessionNotFound
	}

	if sess.DeviceID != deviceID {
		return "", ErrDeviceMismatch
	}

	if sess.Status != StatusActive && sess.Status != StatusDisconnected {
		return "", ErrSessionNotActive
	}

	token, err := rs.tokenSvc.Mint(sessionID, deviceID, 5*time.Minute)
	if err != nil {
		return "", err
	}
	return token, nil
}

// AttachClient validates the attach token and claims the session for a new
// client, taking over from any previously-bound connection. It returns the
// session's RuntimeID (so the handler can verify the path parameter) and a
// connection generation that the caller must pass to BindConn and DetachClient
// so a superseded connection cannot bind or detach over a newer one.
//
// Takeover (rather than rejecting with a 409) is deliberate: a resilient
// session's previous connection is almost always a dead mobile socket the
// gateway has not yet observed as closed (half-open TCP, no FIN). Refusing the
// reconnect would strand the session until the dead peer is detected, so a valid
// attach always wins and evicts the stale connection.
func (rs *RuntimeSession) AttachClient(ctx context.Context, sessionID, attachToken string) (string, int64, error) {
	validatedSessionID, claimDeviceID, err := rs.tokenSvc.Validate(attachToken)
	if err != nil {
		return "", 0, ErrAttachTokenInvalid
	}
	if validatedSessionID != sessionID {
		return "", 0, ErrAttachTokenInvalid
	}

	rs.mu.Lock()
	sess, ok := rs.sessions[sessionID]
	rs.mu.Unlock()

	if !ok {
		return "", 0, ErrSessionNotFound
	}

	sess.mu.Lock()
	if sess.DeviceID != claimDeviceID {
		sess.mu.Unlock()
		return "", 0, ErrDeviceMismatch
	}
	if sess.Status == StatusFailed || sess.Status == StatusClosing {
		sess.mu.Unlock()
		return "", 0, ErrSessionNotActive
	}

	// Claim the pump for this attach: bump the connection generation and evict
	// any previous client. The live conn is bound later by BindConn, once the
	// WebSocket upgrade succeeds; until then the pump has no client. The call
	// sits under sess.mu so the generation bump and the Status transition are
	// atomic with respect to other attach/detach calls (lock order: sess.mu →
	// clientMu, never inverted).
	gen := sess.pump.Attach()
	sess.Status = StatusActive
	sess.DisconnectedAt = nil
	record := storage.SessionRecord{
		SessionID:   sess.ID,
		RuntimeID:   sess.RuntimeID,
		DeviceID:    sess.DeviceID,
		AgentID:     sess.AgentID,
		Status:      StatusActive,
		CreatedAt:   sess.CreatedAt,
	}
	runtimeID := sess.RuntimeID
	sess.mu.Unlock()

	now := time.Now().UTC()
	record.LastClientConnectAt = &now
	if err := rs.store.SaveSession(ctx, record); err != nil {
		return "", 0, fmt.Errorf("save session: %w", err)
	}

	return runtimeID, gen, nil
}

// BindConn attaches the upgraded WebSocket to the session pump for the given
// generation. It returns false if a newer attach has already superseded this
// generation, in which case the caller should discard the connection. The
// generation fence lives on the pump, so a stale bind can neither bind a conn
// nor touch session status.
func (rs *RuntimeSession) BindConn(sessionID string, conn *websocket.Conn, gen int64) bool {
	rs.mu.Lock()
	sess, ok := rs.sessions[sessionID]
	var pump *StdioPump
	if ok {
		// sess.pump is immutable after Create; read it under the registry lock
		// so the read is covered by the same happens-before edge as the lookup.
		pump = sess.pump
	}
	rs.mu.Unlock()
	if !ok {
		return false
	}

	return pump.Bind(conn, gen)
}

// DetachClient marks the session as disconnected, but only when gen still
// matches the current connection generation. A superseded (evicted) connection
// calling DetachClient is a no-op, so it cannot clobber the connection that
// replaced it. The pump keeps running regardless.
func (rs *RuntimeSession) DetachClient(sessionID string, gen int64) error {
	rs.mu.Lock()
	sess, ok := rs.sessions[sessionID]
	rs.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
	}

	// Hold sess.mu across the pump's generation fence and the Status
	// transition, so a stale detach cannot interleave with a concurrent
	// AttachClient: if the pump's Detach(gen) rejects (a newer Attach bumped
	// the generation), nothing transitions; if it accepts, the Status write
	// happens while AttachClient's sess.mu is held, so a newer attach cannot
	// be clobbered back to Disconnected. Lock order: sess.mu → clientMu,
	// never inverted.
	sess.mu.Lock()
	pump := sess.pump

	// Stale detach (from a superseded connection's handler): the pump's
	// generation fence rejects it, so an evicted connection cannot clobber the
	// one that replaced it. Nothing to transition.
	if !pump.Detach(gen) {
		sess.mu.Unlock()
		return nil
	}

	now := time.Now().UTC()
	sess.Status = StatusDisconnected
	sess.DisconnectedAt = &now
	record := storage.SessionRecord{
		SessionID:              sess.ID,
		RuntimeID:              sess.RuntimeID,
		DeviceID:               sess.DeviceID,
		AgentID:                sess.AgentID,
		Status:                 StatusDisconnected,
		CreatedAt:              sess.CreatedAt,
		LastClientDisconnectAt: &now,
		DisconnectedSince:      &now,
	}
	sess.mu.Unlock()

	// Persist while holding rs.mu so a concurrent crash reclamation
	// (handleProcessExit, which deletes the session and its store record under
	// rs.mu) cannot interleave: if the session has been reclaimed, the
	// membership check fails and we skip the save instead of resurrecting a
	// deleted session with a stale "disconnected" record. Bound the write so a
	// slow store cannot stall session operations indefinitely.
	rs.mu.Lock()
	if _, ok := rs.sessions[sessionID]; !ok {
		rs.mu.Unlock()
		return ErrSessionNotFound
	}
	saveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rs.store.SaveSession(saveCtx, record); err != nil {
		cancel()
		rs.mu.Unlock()
		return fmt.Errorf("save disconnected session: %w", err)
	}
	cancel()
	rs.mu.Unlock()

	return nil
}

// Close terminates a session after checking that it belongs to the device.
func (rs *RuntimeSession) Close(ctx context.Context, sessionID, deviceID string) error {
	rs.mu.Lock()
	sess, ok := rs.sessions[sessionID]
	if !ok {
		rs.mu.Unlock()
		return ErrSessionNotFound
	}
	if sess.DeviceID != deviceID {
		rs.mu.Unlock()
		return ErrDeviceMismatch
	}
	rs.mu.Unlock()

	return rs.closeSession(ctx, sess)
}

// StopByRuntimeID closes the session bound to runtimeID, stopping its agent
// process. Runtimes are gateway-global, so any control-scoped client may stop
// one. Returns the runtime's final record.
func (rs *RuntimeSession) StopByRuntimeID(ctx context.Context, runtimeID string) (runtime.Runtime, error) {
	rs.mu.Lock()
	sess, ok := rs.sessionByRuntimeLocked(runtimeID)
	rs.mu.Unlock()
	if !ok {
		return runtime.Runtime{}, ErrSessionNotFound
	}
	if err := rs.closeSession(ctx, sess); err != nil {
		return runtime.Runtime{}, err
	}
	return rs.runtimeSvc.Get(runtimeID)
}

// StopByAgent closes every session for the agent, stopping each agent process.
// It returns the newest stopped runtime.
func (rs *RuntimeSession) StopByAgent(ctx context.Context, agentID string) (runtime.Runtime, error) {
	rs.mu.Lock()
	var targets []*Session
	for _, s := range rs.sessions {
		if s.AgentID == agentID {
			targets = append(targets, s)
		}
	}
	rs.mu.Unlock()
	if len(targets) == 0 {
		return runtime.Runtime{}, ErrSessionNotFound
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].CreatedAt.After(targets[j].CreatedAt) })

	newestID := targets[0].RuntimeID
	for _, s := range targets {
		if err := rs.closeSession(ctx, s); err != nil {
			rs.logger.Warn("failed to stop session for agent", "session_id", s.ID, "agent_id", agentID, "error", err)
		}
	}
	newest, err := rs.runtimeSvc.Get(newestID)
	if err != nil {
		return runtime.Runtime{}, err
	}
	return newest, nil
}

// sessionByRuntimeLocked returns the session bound to runtimeID. Caller holds rs.mu.
func (rs *RuntimeSession) sessionByRuntimeLocked(runtimeID string) (*Session, bool) {
	for _, s := range rs.sessions {
		if s.RuntimeID == runtimeID {
			return s, true
		}
	}
	return nil, false
}

// closeSession tears down one session and stops the agent process it owns. The
// caller must not hold rs.mu. Ordering keeps teardown safe: the session is
// marked closing (persisted) and removed from the registry before any agent
// I/O, so the exit watcher (which fires while the process is stopped) finds no
// live session and records the exit as intentional.
func (rs *RuntimeSession) closeSession(ctx context.Context, sess *Session) error {
	// Step 1: mark the session as closing and its exit intentional, so no
	// concurrent operation treats it as live and the exit watcher records a
	// clean stop even if the process dies during teardown.
	sess.stopping.Store(true)
	sess.mu.Lock()
	sess.Status = StatusClosing
	sess.mu.Unlock()

	if err := rs.store.SaveSession(ctx, storage.SessionRecord{
		SessionID: sess.ID,
		RuntimeID: sess.RuntimeID,
		DeviceID:  sess.DeviceID,
		AgentID:   sess.AgentID,
		Status:    StatusClosing,
		CreatedAt: sess.CreatedAt,
	}); err != nil {
		rs.logger.Error("failed to persist closing status", "error", err)
	}

	// Step 2: stop the stdout drain loop so no new frames enter the buffer.
	if sess.cancelPump != nil {
		sess.cancelPump()
	}

	// Step 3: drop the session from the registry before the agent I/O below.
	rs.mu.Lock()
	delete(rs.sessions, sess.ID)
	rs.mu.Unlock()

	// Step 4: If the agent supports session/close, send one last ACP request
	// so it can cancel in-progress work before the process is killed.
	// Uses acp.CloseSessionRequest for typed param construction. Routed through
	// the pump (WriteSessionClose) so the frame log records the teardown
	// handshake; the frame is gateway-originated, so it deliberately skips the
	// client-snooping in WriteToAgent.
	//
	// The sessionId must be the agent-side ACP id the client negotiated via
	// session/new (snooped by the pump) — the gateway's own resilient
	// sessionID is a different namespace and an agent that doesn't recognize
	// it will silently no-op the close, skipping the cancel-in-flight-work
	// handshake before the process is killed.
	if sess.pump.SupportsClose() {
		acpID := sess.pump.AcpSessionID()
		if acpID == "" {
			// Fallback: no session/new response observed (defensive; the
			// agent accepted initialize, so it should have one).
			acpID = sess.ID
		}
		closeMsg, _ := json.Marshal(struct {
			JSONRPC string                  `json:"jsonrpc"`
			Method  string                  `json:"method"`
			ID      string                  `json:"id"`
			Params  acp.CloseSessionRequest `json:"params"`
		}{
			JSONRPC: "2.0",
			Method:  "session/close",
			ID:      "gw-close-" + sess.ID,
			Params:  acp.CloseSessionRequest{SessionId: acp.SessionId(acpID)},
		})
		written := make(chan struct{})
		go func() {
			defer close(written)
			_ = sess.pump.WriteSessionClose(closeMsg)
		}()
		select {
		case <-written:
		case <-time.After(closeWriteTimeout):
			rs.logger.Warn("agent did not accept session/close in time; stopping process", "runtime_id", sess.RuntimeID)
		}
	}

	// Step 5: stop the process the session owns; the exit is already marked
	// intentional (Step 1). Wait for the watcher to record the exit so the
	// runtime is observably stopped when this returns.
	if err := sess.proc.Stop(2 * time.Second); err != nil {
		rs.logger.Warn("agent process did not stop cleanly", "runtime_id", sess.RuntimeID, "error", err)
	}
	if sess.exited != nil {
		select {
		case <-sess.exited:
		case <-time.After(5 * time.Second):
			rs.logger.Warn("timed out waiting for process exit to be recorded", "runtime_id", sess.RuntimeID)
		}
	}

	// Step 6: delete from storage — cascades to outbound/inbound rows.
	if err := rs.store.DeleteSession(ctx, sess.ID); err != nil {
		rs.logger.Error("failed to delete session from store during close", "error", err, "session_id", sess.ID)
	}

	return nil
}

// ListByDevice returns summaries of all sessions owned by a device.
func (rs *RuntimeSession) ListByDevice(ctx context.Context, deviceID string) ([]SessionSummary, error) {
	records, err := rs.store.ListSessionsByDevice(ctx, deviceID)
	if err != nil {
		return nil, err
	}
	summaries := make([]SessionSummary, 0, len(records))
	for _, rec := range records {
		summaries = append(summaries, SessionSummary{
			SessionID: rec.SessionID,
			RuntimeID: rec.RuntimeID,
			AgentID:   rec.AgentID,
			Status:    rec.Status,
			CreatedAt: rec.CreatedAt,
		})
	}
	return summaries, nil
}

// GetPump returns the StdioPump for a session (used by HTTP handlers to write
// to the agent via WriteToAgent/MaybeReplayInitialize). The pump's connection
// lifecycle is driven through RuntimeSession's AttachClient/BindConn/DetachClient.
func (rs *RuntimeSession) GetPump(sessionID string) (*StdioPump, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	sess, ok := rs.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return sess.pump, nil
}

// GetSessionStatus returns the current status of a session.
func (rs *RuntimeSession) GetSessionStatus(sessionID string) (string, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	sess, ok := rs.sessions[sessionID]
	if !ok {
		return "", ErrSessionNotFound
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.Status, nil
}

// LogInbound asynchronously records a client->agent frame for audit purposes.
// It is non-blocking — if the diagnostic channel is full the frame is dropped
// and the dropped counter is incremented. The inbound sequence counter is
// automatically incremented per-session.
func (rs *RuntimeSession) LogInbound(sessionID string, payload string) {
	rs.mu.Lock()
	sess, ok := rs.sessions[sessionID]
	rs.mu.Unlock()
	if !ok {
		return
	}
	seq := sess.inboundSeq.Add(1)
	rs.inbound.send(inboundDiagnostic{SessionID: sessionID, Seq: seq, Payload: payload})
}

// reaperLoop periodically scans for sessions that have been disconnected longer
// than MaxDisconnected and closes them. Uses a single ticker goroutine instead
// of per-session time.AfterFunc timers, consistent with the supervisor's prune pattern.
func (rs *RuntimeSession) reaperLoop(ctx context.Context) {
	interval := rs.cfg.ReaperInterval
	if interval <= 0 {
		interval = defaultReaperInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	maxDisc := rs.cfg.MaxDisconnected
	if maxDisc <= 0 {
		maxDisc = defaultMaxDisconnected
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs.reapExpired(maxDisc)
		}
	}
}

func (rs *RuntimeSession) reapExpired(maxDisc time.Duration) {
	now := time.Now().UTC()

	rs.mu.Lock()
	toClose := make(map[string]*Session)
	for id, sess := range rs.sessions {
		sess.mu.Lock()
		status := sess.Status
		discAt := sess.DisconnectedAt
		sess.mu.Unlock()

		if status == StatusDisconnected && discAt != nil {
			// Use the later of DisconnectedAt and lastStdoutAt so that an
			// actively-streaming agent extends the grace period automatically.
			discTime := *discAt
			if lastStdout := sess.pump.LastStdoutAt(); !lastStdout.IsZero() && lastStdout.After(discTime) {
				discTime = lastStdout
			}
			grace := maxDisc
			if sess.pump.AwaitingClient() {
				// A prompt the user has not answered yet is likely why they will
				// come back (the push told them); keep the agent waiting longer.
				grace = max(grace, awaitingClientGrace)
			}
			if now.Sub(discTime) > grace {
				sess.mu.Lock()
				sess.Status = StatusClosing
				sess.mu.Unlock()
				toClose[id] = sess
			}
		}
	}
	rs.mu.Unlock()

	for _, sess := range toClose {
		if err := rs.closeSession(context.Background(), sess); err != nil {
			rs.logger.Warn("failed to reap expired session", "session_id", sess.ID, "error", err)
		}
	}
}

// Shutdown stops the reaper, cancels all active session pumps, stops every
// agent process, and stops the inbound diagnostic writer.
func (rs *RuntimeSession) Shutdown() {
	if rs.cancelReaper != nil {
		rs.cancelReaper()
	}
	rs.mu.Lock()
	rs.closed = true
	sessions := make([]*Session, 0, len(rs.sessions))
	for _, sess := range rs.sessions {
		sessions = append(sessions, sess)
	}
	rs.mu.Unlock()

	for _, sess := range sessions {
		if err := rs.closeSession(context.Background(), sess); err != nil {
			rs.logger.Warn("failed to stop session during shutdown", "session_id", sess.ID, "error", err)
		}
	}

	if rs.inbound != nil {
		rs.inbound.stop()
	}
	if rs.frameLog != nil {
		_ = rs.frameLog.close()
		rs.frameLog = nil
	}
}
