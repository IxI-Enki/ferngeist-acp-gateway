package session

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestPumpBurstReplaySurvivesSlowClient is a regression test for the mock
// bulk-transcript corruption (missing messages, text glued onto the wrong
// turn): a session/load history replay arrives as one ~2000-frame burst, far
// faster than the phone drains its WebSocket. Every frame must reach the
// client, in order. Dropped agent chunks make the app glue the surrounding
// user turns into one bubble; dropped user chunks glue agent replies.
func TestPumpBurstReplaySurvivesSlowClient(t *testing.T) {
	serverCh := make(chan *websocket.Conn, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, aErr := websocket.Accept(w, r, nil)
		if aErr != nil {
			return
		}
		serverCh <- c
	}))
	defer s.Close()

	wsURL := "ws://" + s.Listener.Addr().String() + "/"
	client, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	client.SetReadLimit(-1)

	sc := <-serverCh
	if sc == nil {
		t.Fatal("server connection not established")
	}

	pump := &StdioPump{
		runtimeID: "rt-burst",
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	gen := pump.Attach()
	if !pump.Bind(sc, gen) {
		t.Fatal("Bind should succeed")
	}

	// Past the old 8192-frame queue: a long transcript replays one frame per
	// streamed chunk, so real replays exceed any fixed frame cap.
	const frames = 20000
	frame := func(i int) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"mock_sess_bulk","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"Turn %d"}}}}`, i)
	}
	for i := 0; i < frames; i++ {
		pump.handleStdoutLine(frame(i))
	}

	// Slow reader (~1ms per 10 frames): the burst is enqueued in milliseconds, so a
	// queue smaller than the burst drops frames and this loop times out.
	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()
	for i := 0; i < frames; i++ {
		_, msg, err := client.Read(readCtx)
		if err != nil {
			t.Fatalf("client read frame %d of %d: %v (frames dropped before reaching the client)", i, frames, err)
		}
		if string(msg) != frame(i) {
			t.Fatalf("frame %d mismatch: got %.60q, want %.60q", i, string(msg), frame(i))
		}
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	if got := pump.DroppedFrames(); got != 0 {
		t.Fatalf("DroppedFrames() = %d, want 0", got)
	}

	// Tear down the writer goroutine: Detach cancels its context (it flushes
	// the empty queue and exits), then close both ends of the socket.
	pump.Detach(gen)
	_ = sc.Close(websocket.StatusNormalClosure, "")
}
