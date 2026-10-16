package workspace

import "testing"

// TestParseGitStatusMarksIndexChangesStaged covers the Staged flag across every
// porcelain=v2 line format: a change is staged only when the index side (X) of
// the XY pair is non-'.', never for unmerged entries or untracked files.
func TestParseGitStatusMarksIndexChangesStaged(t *testing.T) {
	out := "1 M. N... 100644 100644 100644 a b staged.go\n" +
		"1 .M N... 100644 100644 100644 a b worktree.go\n" +
		"1 MM N... 100644 100644 100644 a b both.go\n" +
		"2 R. N... 100644 100644 100644 a b R100 renamed.go\torig.go\n" +
		"2 RM N... 100644 100644 100644 a b R100 renamed_mod.go\torig.go\n" +
		"2 C. N... 100644 100644 100644 a b C75 copied.go\torig.go\n" +
		"u UU N... 100644 100644 100644 100644 a b c conflict.go\n" +
		"? new.go\n"
	got := map[string]bool{}
	for _, f := range ParseGitStatus(out) {
		got[f.Path] = f.Staged
	}
	want := map[string]bool{
		"staged.go":      true,  // index modified only
		"worktree.go":    false, // worktree modified only
		"both.go":        true,  // index and worktree modified
		"renamed.go":     true,  // staged rename
		"renamed_mod.go": true,  // staged rename, then worktree edit
		"copied.go":      true,  // staged copy
		"conflict.go":    false, // unmerged: no index/worktree split to report
		"new.go":         false, // untracked
	}
	for p, w := range want {
		if got[p] != w {
			t.Errorf("%s staged = %v, want %v", p, got[p], w)
		}
	}
}
