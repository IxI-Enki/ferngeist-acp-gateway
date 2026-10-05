package session

import (
	"encoding/json"
	"strconv"
	"sync"
)

// turnEndedMethod tells a client that a prompt sent by an earlier connection has
// finished. That turn's real reply carries the old connection's request id, which
// the reattached client never issued, so the reply alone would leave its
// transcript streaming forever. Underscore-prefixed per ACP's extension rule.
const turnEndedMethod = "_ferngeist/turn_ended"

// requestIDs gives every client request a gateway-unique id on its way to the
// agent and restores the client's own id on the reply.
//
// Each client connection numbers its requests from 1. Without translation a
// request still in flight from a dropped connection shares an id with the new
// connection's requests: the agent sees two live requests with one id, and the
// old reply lands on whichever new request reused it. The zero value is ready.
type requestIDs struct {
	mu      sync.Mutex
	next    int64
	pending map[string]pendingRequest // agent-side id -> origin
	byOwner map[string]string         // owner key -> agent-side id, for $/cancel_request
}

type pendingRequest struct {
	gen      int64
	clientID json.RawMessage
	owner    string
	// The ACP session a session/prompt drives; empty for every other method.
	promptSession string
}

func ownerKey(gen int64, clientID json.RawMessage) string {
	return strconv.FormatInt(gen, 10) + ":" + string(clientID)
}

// outbound rewrites a client->agent frame for connection gen: a request gets a
// fresh id, and a $/cancel_request is pointed at the id its target was given.
// Replies to the agent's own requests carry the agent's ids and pass unchanged,
// as does anything that does not parse.
func (r *requestIDs) outbound(gen int64, payload []byte) []byte {
	var msg map[string]json.RawMessage
	if json.Unmarshal(payload, &msg) != nil {
		return payload
	}
	var method string
	if json.Unmarshal(msg["method"], &method) != nil || method == "" {
		return payload
	}
	clientID, isRequest := msg["id"]
	if !isRequest {
		if method == "$/cancel_request" && r.retargetCancel(gen, msg) {
			return marshalOr(msg, payload)
		}
		return payload
	}

	var params struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(msg["params"], &params)
	origin := pendingRequest{gen: gen, clientID: clientID, owner: ownerKey(gen, clientID)}
	if method == "session/prompt" {
		origin.promptSession = params.SessionID
	}

	r.mu.Lock()
	r.next++
	agentID := strconv.FormatInt(r.next, 10)
	if r.pending == nil {
		r.pending = make(map[string]pendingRequest)
		r.byOwner = make(map[string]string)
	}
	r.pending[agentID] = origin
	r.byOwner[origin.owner] = agentID
	r.mu.Unlock()

	msg["id"] = json.RawMessage(agentID)
	return marshalOr(msg, payload)
}

func (r *requestIDs) retargetCancel(gen int64, msg map[string]json.RawMessage) bool {
	var params map[string]json.RawMessage
	if json.Unmarshal(msg["params"], &params) != nil || params["requestId"] == nil {
		return false
	}
	r.mu.Lock()
	agentID, ok := r.byOwner[ownerKey(gen, params["requestId"])]
	r.mu.Unlock()
	if !ok {
		return false
	}
	params["requestId"] = json.RawMessage(agentID)
	raw, err := json.Marshal(params)
	if err != nil {
		return false
	}
	msg["params"] = raw
	return true
}

// reply routes the agent's reply to a translated request. frames is what the
// pump would send for it, the reply last. A reply for the connection that asked
// gets its client id back; one for an earlier connection is withheld, and a
// finished prompt becomes a turnEndedMethod notification instead. Frames that
// are not replies to a translated request pass unchanged.
func (r *requestIDs) reply(currentGen int64, probe frameProbe, frames []string) []string {
	if probe.Method != "" || probe.ID == nil || len(frames) == 0 {
		return frames
	}
	agentID := responseIDKey(*probe.ID)
	r.mu.Lock()
	origin, ok := r.pending[agentID]
	if ok {
		delete(r.pending, agentID)
		delete(r.byOwner, origin.owner)
	}
	r.mu.Unlock()
	if !ok {
		return frames
	}

	if origin.gen != currentGen {
		if origin.promptSession == "" {
			return nil
		}
		return []string{turnEndedFrame(origin.promptSession, probe)}
	}
	last := len(frames) - 1
	if out, rewritten := rewriteResponseID([]byte(frames[last]), origin.clientID); rewritten {
		frames[last] = string(out)
	}
	return frames
}

func turnEndedFrame(sessionID string, probe frameProbe) string {
	stopReason := "end_turn"
	if probe.Result != nil && probe.Result.StopReason != "" {
		stopReason = string(probe.Result.StopReason)
	}
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  turnEndedMethod,
		"params":  map[string]string{"sessionId": sessionID, "stopReason": stopReason},
	})
	return string(out)
}

func marshalOr(msg map[string]json.RawMessage, fallback []byte) []byte {
	out, err := json.Marshal(msg)
	if err != nil {
		return fallback
	}
	return out
}
