package session

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// newTestLoadRecovery builds a LoadRecovery with a discard logger, suitable for
// exercising the re-load recovery policy in isolation.
func newTestLoadRecovery() *LoadRecovery {
	return newLoadRecovery(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func loadRequest(id, sessionID string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":` + id + `,"method":"session/load","params":{"sessionId":"` + sessionID + `"}}`)
}

func updateFrame(sessionID, text string) string {
	return `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"` + sessionID +
		`","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"` + text + `"}}}}`
}

// TestRecoverAlreadyLoaded covers the core fix: after a first successful load
// (whose history is buffered), a second load that the agent rejects as "already
// loaded" is transparently recovered by replaying the buffered history followed
// by a synthesized success carrying the second request's id.
func TestRecoverAlreadyLoaded(t *testing.T) {
	r := newTestLoadRecovery()
	const sid = "ses_abc"

	// First load: record the request, stream history, observe the success.
	r.OnOutbound(loadRequest("1", sid))
	h1 := updateFrame(sid, "What is this?")
	h2 := updateFrame(sid, "It is Ferngeist.")
	r.OnFrame(h1)
	r.OnFrame(h2)

	firstSuccess := `{"jsonrpc":"2.0","id":1,"result":{"modes":{"currentModeId":"chat","availableModes":[]}}}`
	if frames, handled := r.OnFrame(firstSuccess); handled {
		t.Fatalf("first successful load must pass through unchanged, got replacement frames: %v", frames)
	}

	// Second load: agent rejects the duplicate; the recovery module must restore.
	r.OnOutbound(loadRequest("2", sid))
	alreadyLoaded := `{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"Session ses_abc is already loaded"}}`
	frames, handled := r.OnFrame(alreadyLoaded)
	if !handled {
		t.Fatal("expected the already-loaded error to be recovered")
	}
	if len(frames) != 3 {
		t.Fatalf("expected 2 history frames + 1 success, got %d frames: %v", len(frames), frames)
	}
	if frames[0] != h1 || frames[1] != h2 {
		t.Fatalf("buffered history not replayed in order:\n got %q, %q", frames[0], frames[1])
	}

	// The synthesized success must reuse the cached result shape and carry id 2.
	var got struct {
		ID     json.Number     `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal([]byte(frames[2]), &got); err != nil {
		t.Fatalf("synthesized success is not valid JSON: %v", err)
	}
	if got.ID.String() != "2" {
		t.Fatalf("synthesized success must echo the second request id, got id=%s", got.ID)
	}
	if got.Error != nil {
		t.Fatalf("synthesized response must not carry an error: %s", frames[2])
	}
	if !strings.Contains(string(got.Result), "currentModeId") {
		t.Fatalf("synthesized success should reuse the cached load result, got result=%s", got.Result)
	}
}

// TestRecoverFallsBackToNullResult verifies that when no first-load response was
// cached (e.g. the agent was already loaded before this pump observed a success),
// recovery still produces a valid null-result success rather than surfacing the error.
func TestRecoverFallsBackToNullResult(t *testing.T) {
	r := newTestLoadRecovery()
	const sid = "ses_xyz"

	r.OnOutbound(loadRequest("7", sid))
	frames, handled := r.OnFrame(
		`{"jsonrpc":"2.0","id":7,"error":{"message":"session is already loaded"}}`)
	if !handled {
		t.Fatal("expected recovery even without buffered history or cached response")
	}
	if len(frames) != 1 {
		t.Fatalf("expected a single synthesized success frame, got %d: %v", len(frames), frames)
	}
	var got struct {
		ID     json.Number     `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(frames[0]), &got); err != nil {
		t.Fatalf("fallback success is not valid JSON: %v", err)
	}
	if got.ID.String() != "7" || string(got.Result) != "null" {
		t.Fatalf("expected {id:7, result:null}, got %s", frames[0])
	}
}

// TestUnrelatedLoadErrorPassesThrough ensures a load failure that is not an
// already-loaded rejection is surfaced to the client unchanged.
func TestUnrelatedLoadErrorPassesThrough(t *testing.T) {
	r := newTestLoadRecovery()
	r.OnOutbound(loadRequest("3", "ses_def"))
	if _, handled := r.OnFrame(
		`{"jsonrpc":"2.0","id":3,"error":{"message":"unknown session"}}`); handled {
		t.Fatal("a non already-loaded error must not be recovered")
	}
}

// TestRecoverIgnoresFramesWithoutPendingLoad guards the hot path: ordinary
// streaming frames with no in-flight load are never rewritten.
func TestRecoverIgnoresFramesWithoutPendingLoad(t *testing.T) {
	r := newTestLoadRecovery()
	if _, handled := r.OnFrame(updateFrame("ses_ghi", "hello")); handled {
		t.Fatal("frames must pass through when no session/load is in flight")
	}
}

// TestBufferLoadHistoryEvictsOldest verifies the byte cap drops the oldest
// frames while always retaining at least the most recent one.
func TestBufferLoadHistoryEvictsOldest(t *testing.T) {
	r := newTestLoadRecovery()
	const sid = "ses_big"
	big := strings.Repeat("x", maxLoadHistoryBytes)
	r.OnFrame(updateFrame(sid, "first"))
	r.OnFrame(updateFrame(sid, big))

	r.mu.Lock()
	defer r.mu.Unlock()
	if got := len(r.history[sid]); got != 1 {
		t.Fatalf("expected oldest frame evicted leaving 1, got %d", got)
	}
	if !strings.Contains(r.history[sid][0].line, "xxxx") {
		t.Fatal("expected the most recent (large) frame to be retained")
	}
}

func promptRequest(sessionID, text string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":9,"method":"session/prompt","params":{"sessionId":"` + sessionID +
		`","prompt":[{"type":"text","text":"` + text + `"}]}}`)
}

// A live prompt is a request, never echoed as an update, so the replay must
// carry it itself or consecutive replies arrive with no user turn between them.
func TestRecoverReplaysPromptsAsUserMessages(t *testing.T) {
	r := newTestLoadRecovery()
	const sid = "ses_live"
	r.OnOutbound(promptRequest(sid, "first question"))
	r.OnFrame(updateFrame(sid, "first answer"))
	r.OnOutbound(promptRequest(sid, "second question"))
	r.OnFrame(updateFrame(sid, "second answer"))

	r.OnOutbound(loadRequest("2", sid))
	frames, handled := r.OnFrame(`{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"already loaded"}}`)
	if !handled || len(frames) != 5 {
		t.Fatalf("expected 4 history frames + success, got %d: %v", len(frames), frames)
	}
	for i, want := range []string{"first question", "first answer", "second question", "second answer"} {
		if !strings.Contains(frames[i], want) {
			t.Fatalf("frame %d should carry %q, got %s", i, want, frames[i])
		}
	}
	if !strings.Contains(frames[2], `"user_message_chunk"`) {
		t.Fatalf("prompt should replay as a user message, got %s", frames[2])
	}
}

// Past the cap the buffer drops whole turns, so a replay never opens mid-reply.
func TestBufferLoadHistoryEvictsToATurnBoundary(t *testing.T) {
	r := newTestLoadRecovery()
	const sid = "ses_turns"
	half := strings.Repeat("x", maxLoadHistoryBytes/2)
	r.OnOutbound(promptRequest(sid, "q1"))
	r.OnFrame(updateFrame(sid, half))
	r.OnFrame(updateFrame(sid, "end of reply one"))
	r.OnOutbound(promptRequest(sid, "q2"))
	r.OnFrame(updateFrame(sid, half))

	r.mu.Lock()
	defer r.mu.Unlock()
	first := r.history[sid][0]
	if !first.user || !strings.Contains(first.line, "q2") {
		t.Fatalf("history should start at the second prompt, starts with %s", first.line)
	}
}
