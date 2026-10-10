package runtime

import (
	"io"
)

// Pipes is the interface for leased stdio pipes. LeasedPipes satisfies this.
type Pipes interface {
	WriteToAgent(payload []byte) error
	Release() error
}

// LeasedPipes wraps a runtime's stdio pipes with an explicit release mechanism.
// It does NOT expose Stdin directly for inbound writes — use WriteToAgent.
// Release only clears the leaseholder: the owning session's pump keeps running
// and continues to own the pipe.
type LeasedPipes struct {
	Stdin       io.WriteCloser
	Stdout      io.ReadCloser
	RuntimeID   string
	leaseholder string
	supervisor  *Supervisor
	released    bool
}

// WriteToAgent writes an inbound frame to the agent's stdin followed by a newline.
func (lp *LeasedPipes) WriteToAgent(payload []byte) error {
	if _, err := lp.Stdin.Write(append(payload, '\n')); err != nil {
		return err
	}
	return nil
}

// Release clears the leaseholder string. The pump continues to own the pipe.
//
// Release is nil-safe: if the supervisor backing store is unavailable (nil
// supervisor, e.g. from a test mock), the release is a no-op.
func (lp *LeasedPipes) Release() error {
	if lp.released {
		return nil
	}
	lp.released = true
	if lp.supervisor != nil {
		lp.supervisor.mu.Lock()
		if existing, exists := lp.supervisor.processes[lp.RuntimeID]; exists {
			existing.leaseholder = ""
		}
		lp.supervisor.mu.Unlock()
	}
	return nil
}

// AcquireLease grants exclusive use of a runtime's stdio pipes to the given
// leaseholder (a session ID). The returned LeasedPipes must be released
// explicitly via ReleaseLease or the Release() method when the session closes.
func (s *Supervisor) AcquireLease(runtimeID, leaseholder string) (Pipes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneStoppedRuntimesLocked(s.now().UTC())

	r, ok := s.runtimes[runtimeID]
	if !ok {
		return nil, ErrRuntimeNotFound
	}
	if r.Status != StatusRunning {
		return nil, ErrRuntimeNotRunning
	}

	handle, ok := s.processes[runtimeID]
	if !ok || handle.stdin == nil || handle.stdout == nil {
		return nil, ErrRuntimeNotConnectable
	}
	if handle.leaseholder != "" {
		return nil, ErrRuntimeLeaseHeld
	}
	handle.leaseholder = leaseholder

	return &LeasedPipes{
		Stdin:       handle.stdin,
		Stdout:      handle.stdout,
		RuntimeID:   runtimeID,
		leaseholder: leaseholder,
		supervisor:  s,
	}, nil
}

// ReleaseLease releases the lease held by leaseholder on the given runtime.
// Returns ErrRuntimeNotFound if the runtime doesn't exist, or ErrRuntimeLeaseHeld
// if a different leaseholder owns the lease.
func (s *Supervisor) ReleaseLease(runtimeID, leaseholder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle, ok := s.processes[runtimeID]
	if !ok {
		return ErrRuntimeNotFound
	}
	if handle.leaseholder != leaseholder {
		return ErrRuntimeLeaseHeld
	}
	handle.leaseholder = ""
	return nil
}

// OnProcessExit registers a callback to be invoked when the runtime process exits.
// This allows the session module to mark sessions as failed when their backing
// agent dies. The callback receives the runtime ID and whether the exit was
// intentional (a stop or restart) rather than a crash.
func (s *Supervisor) OnProcessExit(runtimeID string, callback func(string, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onExitCallbacks[runtimeID] = callback
}
