package api

import "net/http"

// Admin transport for custom agents. The admin API listens on localhost only,
// so its handlers carry no scope gate: they are trusted exactly like the other
// adminMux handlers (pairing, devices). The create/update/delete routes reuse
// the public handlers from custom_agents.go verbatim, so the status codes
// cannot drift between the two transports.

// handleAdminCustomAgentList returns the full catalog (embedded, registry and
// custom) in the same envelope /v1/agents uses, so clients parse one shape. It
// serves the catalog view without the live runtime merge: the host CLI lists
// what can be launched, not what is currently running.
func (s *Server) handleAdminCustomAgentList(w http.ResponseWriter, _ *http.Request) {
	agents := s.customAgents().List()
	response := make([]agentRuntimeState, 0, len(agents))
	for _, agent := range agents {
		response = append(response, agentRuntimeState{Agent: agent})
	}
	writeJSON(w, http.StatusOK, agentsResponse{Agents: response})
}
