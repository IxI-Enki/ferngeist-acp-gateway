package workspace

import (
	"context"
	"slices"
	"strings"
)

// ListFiles lists the project's files for GET /v1/runtimes/{id}/files/list:
// tracked plus untracked-but-not-ignored paths under cwd, repository-relative
// with / separators and relative to cwd itself, deduplicated, sorted, cut at
// limit. truncated is true only when more than limit unique paths existed.
// limit <= 0 means no cap. A non-repo cwd (and git missing from PATH) is a 422
// — no walking-the-filesystem fallback: without git there are no ignore rules,
// and an unfiltered walk of node_modules would be both huge and useless.
func ListFiles(ctx context.Context, cwd string, limit int) (files []string, truncated bool, err error) {
	// -z keeps paths with spaces or non-ASCII characters unquoted and
	// unambiguous, as CompareChanges already relies on; RunGit pins
	// core.quotePath=false and runs with -C dir, so paths arrive unquoted and
	// cwd-relative.
	out, err := RunGit(ctx, cwd, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ".")
	if err != nil {
		return nil, false, errStatus(422, err.Error())
	}
	// Dedupe: a path can arrive both from --cached and --others during a
	// merge. The result is never nil: the wire contract for an empty listing
	// is "files": [].
	paths := splitNullPaths(out)
	unique := make([]string, 0, len(paths))
	seen := make(map[string]bool, len(paths))
	for _, p := range paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		unique = append(unique, p)
	}
	slices.Sort(unique)
	if limit > 0 && len(unique) > limit {
		return unique[:limit], true, nil
	}
	return unique, false, nil
}

// splitNullPaths splits `-z` git output on NUL and drops the trailing empty
// entry. Interior entries are never empty: -z keeps every path whole and
// unquoted, even when it contains spaces or non-ASCII characters.
func splitNullPaths(out string) []string {
	paths := strings.Split(out, "\x00")
	if len(paths) > 0 && paths[len(paths)-1] == "" {
		paths = paths[:len(paths)-1]
	}
	return paths
}
