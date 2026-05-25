package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// websocketWriteContext returns a context with the configured ACP WebSocket
// write timeout. Each outgoing message write is bounded by this deadline.
func websocketWriteContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), acpWebSocketWriteTimeout)
}

// proxyWebSocketToStdio adapts ACP-over-WebSocket client messages into the
// newline-delimited stdio framing used by CLI ACP servers. It also mirrors the
// raw client payload into the runtime log buffer as `acp.stdin` traffic.
// The loop exits on normal WebSocket closure or any read/write error.
//
// Parameters writeFunc and closeFunc replace the old io.WriteCloser to support
// both legacy (stdin.Write + stdin.Close) and resilient (pump.WriteToAgent +
// no-op close) paths without duplicating the read loop.
//
// The optional logInbound callback is called for each inbound frame and is used
// by the resilient session path to asynchronously record client->agent diagnostics.
func proxyWebSocketToStdio(src *websocket.Conn, writeFunc func([]byte) error, closeFunc func(), runtimeID string, appendLog func(string, string, string), logInbound func([]byte), done chan<- error) {
	defer closeFunc()
	for {
		// ACP sessions can stay quiet between turns. Do not treat a short idle
		// period as a dead websocket just because no client frame arrived yet.
		messageType, payload, err := src.Read(context.Background())
		if err != nil {
			if closeStatus := websocket.CloseStatus(err); closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
				done <- io.EOF
				return
			}
			done <- err
			return
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}
		if appendLog != nil {
			appendLog(runtimeID, "acp.stdin", string(payload))
		}
		if logInbound != nil {
			logInbound(payload)
		}
		if err := writeFunc(payload); err != nil {
			done <- err
			return
		}
	}
}

// proxyStdioToWebSocket performs the reverse adaptation by streaming each stdio
// line as one WebSocket text frame. Each line is also mirrored into the runtime
// log buffer as `acp.stdout` traffic before being forwarded to the client.
// The scanner buffer is capped at 1MB to match the WebSocket read limit.
func proxyStdioToWebSocket(src io.Reader, dst *websocket.Conn, runtimeID string, appendLog func(string, string, string), done chan<- error) {
	scanner := bufio.NewScanner(src)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if appendLog != nil {
			appendLog(runtimeID, "acp.stdout", line)
		}
		ctx, cancel := websocketWriteContext()
		err := dst.Write(ctx, websocket.MessageText, []byte(line))
		cancel()
		if err != nil {
			if closeStatus := websocket.CloseStatus(err); closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
				done <- io.EOF
				return
			}
			done <- err
			return
		}
	}
	if err := scanner.Err(); err != nil {
		done <- err
		return
	}
	done <- io.EOF
}

func (s *Server) attachStdioRuntime(w http.ResponseWriter, r *http.Request, runtimeID string) (*websocket.Conn, io.WriteCloser, io.ReadCloser, func(), error) {
	stdin, stdout, release, err := s.runtime.AttachStdio(runtimeID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return nil, nil, nil, nil, err
	}

	clientConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		release()
		s.logger.Error("websocket upgrade failed", slog.String("error", err.Error()))
		return nil, nil, nil, nil, err
	}
	clientConn.SetReadLimit(acpWebSocketReadLimit)
	return clientConn, stdin, stdout, release, nil
}

// websocketScheme determines the WebSocket scheme (ws/wss) to advertise in
// connection responses. It respects X-Forwarded-Proto for reverse proxy setups,
// falling back to the presence of TLS on the direct connection.
func websocketScheme(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); forwarded != "" {
		switch strings.ToLower(forwarded) {
		case "https", "wss":
			return "wss"
		case "http", "ws":
			return "ws"
		}
	}
	if r.TLS != nil {
		return "wss"
	}
	return "ws"
}

// websocketHost returns the host that clients should use to reach this gateway.
// It respects X-Forwarded-Host for reverse proxy configurations.
func websocketHost(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); forwarded != "" {
		return forwarded
	}
	return r.Host
}

// websocketHostWithPath returns the direct websocket endpoint without embedding
// auth material into the URL. Clients should use the returned bearer token via
// Authorization headers when opening the ACP socket.
func websocketHostWithPath(r *http.Request, path string) string {
	return fmt.Sprintf("%s%s", websocketHost(r), path)
}

func absoluteWebSocketURL(r *http.Request, path string) string {
	return fmt.Sprintf("%s://%s", websocketScheme(r), websocketHostWithPath(r, path))
}
