package session

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/catalog"
	"go.uber.org/goleak"
)

// mockAgentPath is the absolute path to the mock-stdio-agent binary, built once
// in TestMain and reused by every test that needs a real launchable agent.
var mockAgentPath string

// TestMain builds the mock agent once, then fails the package's test run if any
// goroutine (pump, writer, reaper, keepalive, process watcher) is still alive
// after all tests complete.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ferngeist-mock-agent")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mkdtemp:", err)
		os.Exit(1)
	}
	bin := filepath.Join(dir, "mock-stdio-agent")
	if goruntime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin,
		"github.com/arafatamim/ferngeist-acp-gateway/cmd/mock-stdio-agent")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build mock agent: %v\n%s", err, out)
		os.Exit(1)
	}
	mockAgentPath = bin
	// goleak.VerifyTestMain exits the process itself, so the binary's temp dir
	// is intentionally left for the OS to reclaim (a deferred cleanup would
	// never run).
	goleak.VerifyTestMain(m)
}

// mockAgent returns a launchable catalog agent backed by the once-built mock
// binary. An absolute command path is used verbatim by the runtime's
// resolveLaunch, so the supervisor starts exactly this executable.
func mockAgent(t *testing.T) catalog.Agent {
	t.Helper()
	if mockAgentPath == "" {
		t.Fatal("mock agent binary was not built (TestMain)")
	}
	return catalog.Agent{
		ID:          "mock-acp",
		DisplayName: "Mock ACP",
		Detected:    true,
		Security:    catalog.SecurityConfig{AllowsRemoteStart: true},
		Launch:      catalog.LaunchConfig{Mode: "process", Command: mockAgentPath},
	}
}
