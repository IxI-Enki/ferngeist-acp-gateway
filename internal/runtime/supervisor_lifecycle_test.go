package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/catalog"
)

// TestFailedRuntimesPrunedAfterRetention fails while prune only takes
// StatusStopped: a long-dead StatusFailed entry (and its log ring, agent
// index, and exit callback) must leave memory after the retention window,
// while a fresh failure is retained for diagnostics.
func TestFailedRuntimesPrunedAfterRetention(t *testing.T) {
	supervisor := NewSupervisor(slog.New(slog.NewTextHandler(io.Discard, nil)), ".", nil, nil)
	now := time.Now().UTC()
	supervisor.now = func() time.Time { return now }

	seed := func(id, status string, lastFailure, stoppedAt time.Time) {
		supervisor.runtimes[id] = Runtime{
			ID: id, AgentID: "mock-acp", AgentName: "Mock",
			Status: status, CreatedAt: now.Add(-2 * time.Hour),
			LastFailureAt: lastFailure, StoppedAt: stoppedAt,
		}
		supervisor.logs[id] = []LogEntry{{Timestamp: now, Stream: "stderr", Message: "boom"}}
		supervisor.addRuntimeByAgentLocked("mock-acp", id)
		supervisor.onExitCallbacks[id] = func(string, bool) {}
	}
	seed("rt-old-failed", StatusFailed, now.Add(-time.Hour), time.Time{})
	seed("rt-fresh-failed", StatusFailed, now, time.Time{})
	seed("rt-old-stopped", StatusStopped, time.Time{}, now.Add(-time.Hour))

	got := supervisor.List()
	if len(got) != 1 || got[0].ID != "rt-fresh-failed" {
		ids := make([]string, 0, len(got))
		for _, r := range got {
			ids = append(ids, r.ID)
		}
		t.Fatalf("List() = %v, want [rt-fresh-failed]", ids)
	}
	for _, id := range []string{"rt-old-failed", "rt-old-stopped"} {
		if _, ok := supervisor.logs[id]; ok {
			t.Errorf("logs[%q] survives prune, want evicted", id)
		}
		if _, ok := supervisor.onExitCallbacks[id]; ok {
			t.Errorf("onExitCallbacks[%q] survives prune, want evicted", id)
		}
		for _, rid := range supervisor.runtimeIDsForAgentLocked("mock-acp") {
			if rid == id {
				t.Errorf("runtimeByAgent still references %q after prune", id)
			}
		}
	}
	if _, ok := supervisor.logs["rt-fresh-failed"]; !ok {
		t.Error("logs[rt-fresh-failed] pruned too early, want retained")
	}
}

// TestShutdownClearsLogRings fails while Shutdown drops runtimes/processes
// but leaves every 200-entry log ring (and exit callback) behind.
func TestShutdownClearsLogRings(t *testing.T) {
	supervisor := NewSupervisor(slog.New(slog.NewTextHandler(io.Discard, nil)), ".", nil, nil)
	supervisor.runtimes["rt-1"] = Runtime{
		ID: "rt-1", AgentID: "mock-acp", AgentName: "Mock",
		Status: StatusRunning, CreatedAt: time.Now().UTC(),
	}
	supervisor.logs["rt-1"] = []LogEntry{{Timestamp: time.Now().UTC(), Stream: "stdout", Message: "hi"}}
	supervisor.onExitCallbacks["rt-1"] = func(string, bool) {}

	if err := supervisor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if len(supervisor.runtimes) != 0 {
		t.Fatalf("runtimes = %d entries after Shutdown, want 0", len(supervisor.runtimes))
	}
	if len(supervisor.logs) != 0 {
		t.Fatalf("logs = %d orphaned rings after Shutdown, want 0", len(supervisor.logs))
	}
	if len(supervisor.onExitCallbacks) != 0 {
		t.Fatalf("onExitCallbacks = %d entries after Shutdown, want 0", len(supervisor.onExitCallbacks))
	}
}

// TestRestartAfterShutdownSpawnsNothing fails while restartAfterBackoff sleeps
// past Shutdown and then launches: the relaunched process is owned by nobody
// and its runtime entry resurrects maps Shutdown just drained.
func TestRestartAfterShutdownSpawnsNothing(t *testing.T) {
	baseDir := t.TempDir()
	binDir := filepath.Join(baseDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	source := `package main
import (
	"bufio"
	"os"
)
func main() {
	r := bufio.NewReader(os.Stdin)
	for {
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
	}
}
`
	srcPath := filepath.Join(baseDir, "sleeper.go")
	if err := os.WriteFile(srcPath, []byte(source), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	outputPath := filepath.Join(binDir, namedBinary("shutdown-sleeper-agent"))
	build := exec.Command("go", "build", "-o", outputPath, srcPath)
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build sleeper agent error = %v", err)
	}

	supervisor := NewSupervisor(slog.New(slog.NewTextHandler(io.Discard, nil)), baseDir, nil, nil)
	// Whatever the outcome, leave no process behind (covers the pre-fix
	// failure mode where the restart resurrects a live runtime).
	t.Cleanup(func() { _ = supervisor.Shutdown(context.Background()) })

	agent := catalog.Agent{
		ID: "sleeper-acp", DisplayName: "Sleeper ACP", Detected: true,
		Security: catalog.SecurityConfig{AllowsRemoteStart: true},
		Launch: catalog.LaunchConfig{
			Mode: "process", Command: filepath.Join("bin", namedBinary("shutdown-sleeper-agent")),
			Restart: catalog.RestartConfig{Mode: "on_failure", MaxRetries: 5, BackoffSeconds: 2},
		},
	}
	pending := Runtime{
		ID: "rt-pending-restart", AgentID: agent.ID, AgentName: agent.DisplayName,
		Status: StatusStarting, CreatedAt: time.Now().UTC(), RestartAttempts: 1,
	}
	supervisor.mu.Lock()
	supervisor.runtimes[pending.ID] = pending
	supervisor.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		supervisor.restartAfterBackoff(pending, agent, nil)
	}()
	// Shut down mid-backoff: the sleeper must never launch.
	time.Sleep(200 * time.Millisecond)
	if err := supervisor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("restartAfterBackoff did not return after Shutdown")
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if len(supervisor.runtimes) != 0 {
		t.Fatalf("runtimes = %d entries after shutdown-mid-backoff, want 0 (orphan relaunch)", len(supervisor.runtimes))
	}
	if len(supervisor.processes) != 0 {
		t.Fatalf("processes = %d entries after shutdown-mid-backoff, want 0 (orphan process)", len(supervisor.processes))
	}
}
