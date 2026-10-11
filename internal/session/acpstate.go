package session

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
)

// This file holds the pump's ACP layer: the frame pipeline that every agent
// stdout line runs through, and the state that pipeline maintains. One struct
// (acpState) with one mutex holds everything the gateway remembers about the
// conversation on one agent process.

// frameProbe is the single-parse view of an ACP frame. The hot path previously
// paid 5-6 json.Unmarshal scans per stdout line (turn activity, turn complete,
// permission request, JSON-RPC error, progress event, load-recovery buffering),
// each with its own []byte(line) copy. Every stage now reads this one parse, so
// per-frame CPU/GC is one scan + one copy; rare paths (typed initialize
// capabilities, load-success synthesis) keep their second parse off the hot
// path.
type frameProbe struct {
	Method string           `json:"method"`
	ID     *json.RawMessage `json:"id"`
	Result *struct {
		ProtocolVersion *int           `json:"protocolVersion"`
		SessionID       string         `json:"sessionId"`
		StopReason      acp.StopReason `json:"stopReason"`
		Usage           *acp.Usage     `json:"usage"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Params *struct {
		SessionID string `json:"sessionId"`
		// The client request a request-scoped elicitation/create is tied to.
		RequestID json.RawMessage `json:"requestId"`
		Update    *struct {
			Discriminator string `json:"sessionUpdate"`
			ToolCallID    string `json:"toolCallId"`
			Status        string `json:"status"`
			Title         string `json:"title"`
			Kind          string `json:"kind"`
			// Content is RawMessage because agents emit both a single
			// object and an array; a typed slice would fail the whole
			// probe parse on the object shape (history buffering must
			// never depend on content shape).
			Content json.RawMessage `json:"content"`
		} `json:"update"`
	} `json:"params"`
}

// parseFrameProbe parses one frame once. ok=false means invalid JSON; callers
// treat it as opaque (no stage claims it, it forwards unchanged).
func parseFrameProbe(data []byte) (frameProbe, bool) {
	var probe frameProbe
	if err := json.Unmarshal(data, &probe); err != nil {
		return frameProbe{}, false
	}
	return probe, true
}

// frame is one agent stdout frame plus what the pipeline decided about it.
type frame struct {
	// raw is the frame's bytes: one copy, shared by every stage.
	raw []byte
	// line is the frame as it will be forwarded: a stage may replace it.
	line  string
	probe frameProbe
	ok    bool
	// origin is the client request this frame replies to, if any, and isReply
	// whether it is a reply at all.
	origin  pendingRequest
	isReply bool
	// out is what the client receives: one frame normally, a resume replay
	// burst after a rejected duplicate session/load.
	out []string
	// replaced marks a frame this pipeline rewrote itself (swallowed-failure
	// error), which has already raised its own notification.
	replaced bool
	// resumed marks the out burst as a gateway-synthesized resume replay: the
	// load recovery stage replaced the frame with buffered history plus a
	// success it built itself.
	resumed bool
}

// handleStdoutLine processes one complete agent stdout frame. The frame is
// parsed once and passed through a fixed ordered list of stages:
//
//	request ids → ACP state → load recovery → notifications → forward
//
// Every stage reads the same parse, so no two stages can disagree about what
// the frame is, and each stage is a plain function call rather than a hook.
func (p *StdioPump) handleStdoutLine(line string) {
	// Track when the agent last produced output, so the reaper can
	// distinguish abandoned sessions from actively-streaming ones.
	p.lastStdoutMu.Lock()
	p.lastStdoutAt = time.Now()
	p.lastStdoutMu.Unlock()

	if p.appendLog != nil {
		p.appendLog(p.runtimeID, "acp.stdout", line)
	}
	if p.frameLog != nil {
		p.frameLog.append(p.agentID, p.runtimeID, p.sessionID, "out", []byte(line))
	}

	f := &frame{line: line, raw: []byte(line), out: []string{line}}
	f.probe, f.ok = parseFrameProbe(f.raw)
	// Request ids: which client request (if any) this frame answers. Read-only,
	// so later stages can use the answer without consuming it.
	f.origin, f.isReply = p.reqIDs.peek(f.probe)

	p.observeACPState(f)
	p.applyLoadRecovery(f)
	p.notifyFrame(f)
	p.forward(f)
}

// observeACPState is the ACP-state stage: it feeds the frame into acpState
// (initialize cache, agent name, session id, cwds, turn state) and, when the
// agent swallowed a failed LLM call, replaces the frame with a JSON-RPC error
// so the client sees the turn fail instead of an empty success.
func (p *StdioPump) observeACPState(f *frame) {
	p.acp.observeInitialize(f.probe, f.raw)
	p.acp.observeSessionID(f.probe)
	p.acp.bindPendingCwd(f.probe)
	replacement, handled := p.observeTurn(f.probe, f.origin, f.isReply)
	if !handled {
		return
	}
	f.line, f.raw, f.out, f.replaced = replacement, []byte(replacement), []string{replacement}, true
	// The replacement is a synthesized frame; re-probe it cheaply so the
	// stages below see the frame actually forwarded.
	if rp, ok := parseFrameProbe(f.raw); ok {
		f.probe, f.ok = rp, true
	}
}

// applyLoadRecovery is the load-recovery stage: it buffers conversation history
// so a reconnecting client can be re-hydrated even when the agent rejects a
// duplicate session/load as "already loaded". Normally the frame passes through
// as-is; a rejected duplicate is replaced by the buffered history and a
// synthesized success, opened by an explicit `_ferngeist/resumed` notification
// (see LoadRecovery and docs/api.md).
func (p *StdioPump) applyLoadRecovery(f *frame) {
	if p.loadRecovery == nil {
		return
	}
	if replacements, handled := p.loadRecovery.OnFrame(f.line, f.probe, f.ok); handled {
		f.out = replacements
		f.resumed = true
	}
}

// forward is the last stage: it routes the frame(s) to the attached client.
// It restores the client's own request id on a reply, withholds a reply for a
// superseded connection (a finished prompt there becomes a _ferngeist/turn_ended
// notification), re-sends the agent's unanswered requests behind a session/load
// reply, scopes a request-bound elicitation, and queues the result to the
// client's writer goroutine. A client whose backlog overflows is disconnected
// rather than sent a stream with holes in it.
func (p *StdioPump) forward(f *frame) {
	var evict *websocket.Conn
	p.clientMu.Lock()
	// Under clientMu: a reply routed for one generation must not reach the next.
	out := p.reqIDs.reply(p.connGen, f.probe, f.out)
	bound := p.client != nil && p.writerQ != nil
	// The agent's unanswered requests are not in load history, so the reply that
	// restores their session carries them. Only for the connection that asked,
	// and only once the load succeeded (a recovered "already loaded" counts).
	if f.isReply && f.origin.method == "session/load" && f.origin.gen == p.connGen &&
		bound && (f.resumed || f.probe.Error == nil) {
		out = p.reqIDs.redeliver(p.connGen, f.origin.loadSession, out)
	}
	var deliveredGen int64
	if bound {
		deliveredGen = p.connGen
	}
	p.reqIDs.trackAgentRequest(f.probe, f.line, deliveredGen)
	if bound {
		out = p.reqIDs.scopeElicitation(p.connGen, f.probe, out)
		evict = p.queueLocked(out)
	}
	p.clientMu.Unlock()
	if evict != nil {
		_ = evict.CloseNow()
	}
}

// prepareOutbound rewrites one client→agent frame for the current connection
// and records what it says about the conversation: the request id it is given,
// the ACP session and project directory it names, a prompt starting a turn, and
// anything the re-load replay has to remember.
func (p *StdioPump) prepareOutbound(payload []byte) []byte {
	p.clientMu.Lock()
	gen := p.connGen
	p.clientMu.Unlock()
	payload = p.reqIDs.outbound(gen, payload)
	p.acp.observeInbound(payload)
	if p.loadRecovery != nil {
		p.loadRecovery.OnOutbound(payload)
	}
	return payload
}

// observeTurn records content on the active turn and evaluates the prompt
// result. A success with end_turn, zero usage, and no content is how some
// agents (e.g. Opencode) report a swallowed LLM failure; in that case the frame
// is replaced with a JSON-RPC error (same request id) so the client sees the
// turn as failed, and handled reports the replacement. Log and push happen
// outside the state lock: the callback reaches network code, and the next
// prompt must never queue behind push I/O.
func (p *StdioPump) observeTurn(probe frameProbe, origin pendingRequest, isReply bool) (out string, handled bool) {
	if !p.acp.observeTurn(probe, origin, isReply) {
		return "", false
	}
	p.logger.Warn("agent returned empty success; rewriting as JSON-RPC error "+
		"(likely swallowed LLM failure: rate limit / invalid model setting)",
		"runtime_id", p.runtimeID, "session_id", p.sessionID)
	p.notifySwallowedFailure(probe, origin)
	frame, ok := swallowedFailureFrame(probe)
	if !ok {
		p.logger.Error("failed to marshal swallowed-failure error frame")
		return "", false // forward path treats ""+false as no replacement
	}
	return frame, true
}

// swallowedFailureFrame builds the JSON-RPC error that replaces an empty
// success, keeping the request id so the client correlates it with its prompt.
func swallowedFailureFrame(probe frameProbe) (string, bool) {
	if probe.ID == nil {
		return "", false
	}
	errFrame := map[string]any{
		"jsonrpc": "2.0",
		"id":      *probe.ID,
		"error": map[string]any{
			"code":    -32000,
			"message": "Agent produced no output for this turn. The model call likely failed (rate limit or invalid model setting); see the gateway logs for details.",
		},
	}
	outBytes, err := json.Marshal(errFrame)
	if err != nil {
		return "", false
	}
	return string(outBytes), true
}

// acpState is everything the gateway remembers about the ACP conversation on one
// agent process: the agent's initialize response and name, whether the agent
// accepts session/close, the ACP session id and per-session working directories
// it has seen, turn state for swallowed-failure detection, and the kinds of the
// tool calls currently running. One struct, one mutex — every field is small and
// read on the stdout hot path, so splitting the lock would only add lock
// ordering to get wrong. The zero value is ready.
type acpState struct {
	mu sync.Mutex

	// initResponse is the agent's `initialize` response. A reconnecting client
	// re-runs the ACP handshake, but the agent process is already initialized —
	// forwarding a second `initialize` can make a strict agent error out and
	// exit. The pump replays this response (with the new request's id) instead
	// and never forwards the duplicate.
	initResponse []byte
	// agentName is the human-readable name from initialize's agentInfo, "" until
	// observed.
	agentName string
	// closeSupported records that initialize advertised
	// sessionCapabilities.close.
	closeSupported bool

	// sessionID is the ACP session id ("ses_…"), observed from the agent's
	// session/new response or from any client frame that names one. It is sent
	// as the push SessionID so notifications match the id the client navigates
	// and keys ActiveChat by — the gateway's own resilient session id is a
	// different namespace and never matches the client.
	sessionID string
	// cwd is the working directory of the most recently opened or loaded ACP
	// session — the anchor for the gateway's file/diff/status workspace
	// endpoints. Empty until the client opens a project.
	cwd string
	// cwdBySession maps each ACP session id to its own cwd, so the workspace
	// endpoints can target one chat when the agent hosts several. session/load
	// carries both; session/new only learns its id from the agent's response,
	// so its cwd waits in pendingCwd, keyed by the raw request id, until then.
	// ponytail: never pruned, bounded by the sessions one agent process opens.
	cwdBySession map[string]string
	pendingCwd   map[string]string

	// Swallowed-failure detection. Some agents (e.g. Opencode) never send a
	// JSON-RPC error for a failed LLM call — they return a success with
	// stopReason=end_turn, zero usage, and no streamed content. The pump tracks
	// whether the current turn produced any visible output so such failures can
	// be flagged.
	turnActive     bool // a session/prompt is awaiting its result
	turnHadContent bool // an agent_message_chunk or tool_call arrived this turn

	// toolKinds remembers each running tool call's kind: a tool_call_update
	// usually omits it. Entries leave when the call completes or fails.
	toolKinds map[string]string
}

// observeInitialize caches the agent's `initialize` response — identified by the
// presence of result.protocolVersion, which only initialize responses carry —
// along with the agent name and close capability it advertises.
func (s *acpState) observeInitialize(probe frameProbe, raw []byte) {
	s.mu.Lock()
	cached := s.initResponse != nil
	s.mu.Unlock()
	if cached {
		return
	}
	if probe.Result == nil || probe.Result.ProtocolVersion == nil {
		return
	}

	s.mu.Lock()
	s.initResponse = append([]byte(nil), raw...)
	s.mu.Unlock()

	var typed struct {
		Result *acp.InitializeResponse `json:"result"`
	}
	if err := json.Unmarshal(raw, &typed); err != nil || typed.Result == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if typed.Result.AgentCapabilities.SessionCapabilities.Close != nil {
		s.closeSupported = true
	}
	if info := typed.Result.AgentInfo; info != nil {
		name := info.Name
		if info.Title != nil && *info.Title != "" {
			name = *info.Title
		}
		s.agentName = name
	}
}

// initializeResponse returns the cached initialize response, or nil.
func (s *acpState) initializeResponse() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initResponse
}

// name is the agent's display name, or fallback before initialize was observed.
func (s *acpState) name(fallback string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agentName == "" {
		return fallback
	}
	return s.agentName
}

// closes reports whether the agent advertised sessionCapabilities.close.
func (s *acpState) closes() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeSupported
}

// observeSessionID caches the ACP session id the agent's session/new response
// returns. Only that response carries a top-level result.sessionId, so this
// never misfires on other frames, and the id is captured once for the
// session's lifetime.
func (s *acpState) observeSessionID(probe frameProbe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessionID != "" {
		return
	}
	if probe.Result == nil || probe.Result.SessionID == "" {
		return
	}
	s.sessionID = probe.Result.SessionID
}

// bindPendingCwd binds a pending session/new cwd to the session id the agent's
// response assigns. The request id is the one written to the agent (after
// rewriting), so the agent echoes it verbatim. An error response just drops the
// pending entry.
func (s *acpState) bindPendingCwd(probe frameProbe) {
	if probe.Method != "" || probe.ID == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(*probe.ID)
	cwd, ok := s.pendingCwd[key]
	if !ok {
		return
	}
	delete(s.pendingCwd, key)
	if probe.Result == nil || probe.Result.SessionID == "" {
		return
	}
	if s.cwdBySession == nil {
		s.cwdBySession = make(map[string]string)
	}
	s.cwdBySession[probe.Result.SessionID] = cwd
}

// observeInbound records what one client→agent frame says: the ACP session id it
// targets, the working directory of a session/new, session/load or
// session/resume, and a session/prompt starting a turn. One parse for all three.
//
// The id matters on reconnects: the outbound path only sees session/new
// responses, so a resilient reconnect that restores context via session/load
// would otherwise never be observed and the push SessionID would be empty. cwd
// is set once when the client opens the project, so it is re-captured on each
// such request and a project switch updates it.
func (s *acpState) observeInbound(payload []byte) {
	var req struct {
		Method string           `json:"method"`
		ID     *json.RawMessage `json:"id"`
		Params *struct {
			SessionID string `json:"sessionId"`
			Cwd       string `json:"cwd"`
		} `json:"params"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Method == "session/prompt" {
		s.turnActive = true
		s.turnHadContent = false
	}
	if req.Params == nil {
		return
	}
	if req.Params.SessionID != "" && s.sessionID == "" {
		s.sessionID = req.Params.SessionID
	}
	switch req.Method {
	case "session/new", "session/load", "session/resume":
	default:
		return
	}
	cwd := req.Params.Cwd
	if cwd == "" {
		return
	}
	s.cwd = cwd
	if req.Method == "session/new" {
		if req.ID != nil {
			if s.pendingCwd == nil {
				s.pendingCwd = make(map[string]string)
			}
			s.pendingCwd[string(*req.ID)] = cwd
		}
		return
	}
	if req.Params.SessionID != "" {
		if s.cwdBySession == nil {
			s.cwdBySession = make(map[string]string)
		}
		s.cwdBySession[req.Params.SessionID] = cwd
	}
}

// observeTurn records turn state from one frame and reports whether the prompt
// result is a clean success with no output — the swallowed-failure signature.
func (s *acpState) observeTurn(probe frameProbe, origin pendingRequest, isReply bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if probe.Params != nil && probe.Params.Update != nil {
		switch probe.Params.Update.Discriminator {
		case "agent_message_chunk", "tool_call", "tool_call_update":
			s.turnHadContent = true
		}
		return false
	}

	// Prompt result: only judge when a turn was tracked and the response is a
	// clean success (a JSON-RPC error already pushes its own notification). A
	// reply known to answer another request (a set_mode reply mid-turn) is not
	// the prompt result; an unknown id is still judged.
	if !s.turnActive || (isReply && origin.method != "session/prompt") ||
		probe.ID == nil || probe.Result == nil || probe.Error != nil {
		return false
	}
	s.turnActive = false
	// Any non-zero token field counts as usage: agents may populate
	// input/output without the unstable totalTokens field.
	used := probe.Result.Usage != nil && (probe.Result.Usage.TotalTokens != 0 ||
		probe.Result.Usage.InputTokens != 0 || probe.Result.Usage.OutputTokens != 0)
	return probe.Result.StopReason == "end_turn" && !used && !s.turnHadContent
}

// toolKindFor resolves the kind of a tool call: the frame's own kind when it
// carries one (remembering it for later updates, which usually omit it), else
// the kind seen before.
func (s *acpState) toolKindFor(callID, kind string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kind != "" {
		if s.toolKinds == nil {
			s.toolKinds = make(map[string]string)
		}
		s.toolKinds[callID] = kind
		return kind
	}
	return s.toolKinds[callID]
}

// forgetTool drops a finished tool call's remembered kind.
func (s *acpState) forgetTool(callID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.toolKinds, callID)
}

// AgentName is the agent's name for push text, e.g. "Claude Code", or fallback
// when initialize carried no agentInfo.
func (p *StdioPump) AgentName(fallback string) string {
	return p.acp.name(fallback)
}

// SupportsClose reports whether the agent advertised sessionCapabilities.close.
func (p *StdioPump) SupportsClose() bool {
	return p.acp.closes()
}

// AcpSessionID returns the observed ACP session id, or "" if none has been
// observed yet.
func (p *StdioPump) AcpSessionID() string {
	p.acp.mu.Lock()
	defer p.acp.mu.Unlock()
	return p.acp.sessionID
}

// AcpCwd returns the ACP session working directory, or "" if the client has not
// issued session/new or session/load yet. With several sessions on one agent
// this is whichever was opened last; AcpCwdFor targets one.
func (p *StdioPump) AcpCwd() string {
	p.acp.mu.Lock()
	defer p.acp.mu.Unlock()
	return p.acp.cwd
}

// AcpCwdFor returns the cwd of one ACP session, or "" if the gateway has not
// seen that session opened or loaded.
func (p *StdioPump) AcpCwdFor(acpSessionID string) string {
	p.acp.mu.Lock()
	defer p.acp.mu.Unlock()
	return p.acp.cwdBySession[acpSessionID]
}

func (p *StdioPump) initResponseCached() bool {
	return p.acp.initializeResponse() != nil
}

// MaybeReplayInitialize intercepts a client `initialize` request. If the agent
// has already been initialized (its response is cached), it answers the client
// directly with that cached response — rewritten to carry the request's id — and
// returns true so the caller does not forward a duplicate `initialize` to the
// agent. It returns false for non-initialize frames, or for the first
// `initialize` (no cache yet), which must reach the agent normally.
func (p *StdioPump) MaybeReplayInitialize(payload []byte) bool {
	var req struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(payload, &req); err != nil || req.Method != "initialize" {
		return false
	}

	cached := p.acp.initializeResponse()
	if cached == nil {
		return false
	}

	out, ok := rewriteResponseID(cached, req.ID)
	if !ok {
		return false // malformed cache: fall back to forwarding to the agent
	}

	p.clientMu.Lock()
	client := p.client
	p.clientMu.Unlock()
	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), acpWebSocketWriteTimeout)
		err := client.Write(ctx, websocket.MessageText, out)
		cancel()
		if err != nil {
			p.logger.Warn("replay initialize write failed", "error", err)
		}
	}
	return true
}
