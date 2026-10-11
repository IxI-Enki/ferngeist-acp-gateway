package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/arafatamim/ferngeist-acp-gateway/internal/pairing"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/storage"
	"github.com/arafatamim/ferngeist-acp-gateway/internal/workspace"
)

// worktreeResponse is one gateway-managed git worktree. Path is what the
// client passes as the session/new cwd to run an agent in it.
type worktreeResponse struct {
	ID         string    `json:"id"`
	Repo       string    `json:"repo"`
	Path       string    `json:"path"`
	Branch     string    `json:"branch"`
	BaseCommit string    `json:"baseCommit"`
	CreatedAt  time.Time `json:"createdAt"`
	// Ahead is the number of commits on the branch since BaseCommit; Dirty
	// reports uncommitted changes. Both are filled only by the list endpoint.
	Ahead *int  `json:"ahead,omitempty"`
	Dirty *bool `json:"dirty,omitempty"`
}

type worktreeCreateRequest struct {
	Repo   string `json:"repo"`
	Base   string `json:"base"`
	Branch string `json:"branch"`
}

func toWorktreeResponse(r storage.WorktreeRecord) worktreeResponse {
	return worktreeResponse{ID: r.ID, Repo: r.Repo, Path: r.Path, Branch: r.Branch, BaseCommit: r.BaseCommit, CreatedAt: r.CreatedAt}
}

// POST /v1/worktrees {repo, base?, branch?}
// Creates a new branch at base (default: the repo's HEAD) checked out in a
// fresh worktree at <repo>/.worktrees/<branch>, where it is easy to find
// outside the gateway.
func (s *Server) handleWorktreeCreate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayScope(w, r, pairing.ScopeControl); !ok {
		return
	}
	var req worktreeCreateRequest
	r.Body = http.MaxBytesReader(w, r.Body, jsonBodyLimit)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	rec, err := s.workspaceSvc.Create(r.Context(), workspace.CreateRequest{Repo: req.Repo, Base: req.Base, Branch: req.Branch})
	if err != nil {
		writeWorkspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorktreeResponse(rec))
}

// GET /v1/worktrees
// Lists managed worktrees with their ahead/dirty state. Records whose
// directory was deleted out from under the gateway are dropped (and git's
// bookkeeping pruned) instead of being listed.
func (s *Server) handleWorktreeList(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayScope(w, r, pairing.ScopeRead); !ok {
		return
	}
	statuses, err := s.workspaceSvc.List(r.Context())
	if err != nil {
		writeWorkspaceError(w, err)
		return
	}
	out := make([]worktreeResponse, 0, len(statuses))
	for _, status := range statuses {
		resp := toWorktreeResponse(status.WorktreeRecord)
		resp.Ahead = status.Ahead
		resp.Dirty = status.Dirty
		out = append(out, resp)
	}
	writeJSON(w, http.StatusOK, out)
}

// DELETE /v1/worktrees/{worktreeId}?force=true
// Removes the worktree directory. Without force, git refuses when it has
// uncommitted changes (409). The branch is deleted only when git considers it
// merged (git branch -d); otherwise it is kept so committed work survives.
func (s *Server) handleWorktreeDelete(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayScope(w, r, pairing.ScopeControl); !ok {
		return
	}
	id := r.PathValue("worktreeId")
	branchDeleted, err := s.workspaceSvc.Delete(r.Context(), id, r.URL.Query().Get("force") == "true")
	if err != nil {
		writeWorkspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id, "branchDeleted": branchDeleted})
}

// writeWorkspaceError maps a workspace service error to the HTTP status it
// carries, falling back to 500 for anything untyped.
func writeWorkspaceError(w http.ResponseWriter, err error) {
	var workspaceError workspace.Error
	if errors.As(err, &workspaceError) {
		writeError(w, workspaceError.StatusCode(), workspaceError.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "workspace operation failed")
}
