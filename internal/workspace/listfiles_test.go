package workspace

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// listRepoAt builds a temp git repo at the given directory and returns a git
// runner bound to it, mirroring the historyRepo fixture.
func listRepoAt(t *testing.T, dir string) (string, func(...string) string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	git("config", "commit.gpgSign", "false")
	return dir, git
}

// listRepo is listRepoAt with a fresh temp directory.
func listRepo(t *testing.T) (dir string, git func(...string) string) {
	t.Helper()
	return listRepoAt(t, t.TempDir())
}

// gitLoose runs git and ignores a non-zero exit, for fixtures that expect a
// failing command (e.g. a conflicted merge).
func gitLoose(dir string, args ...string) string {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, _ := cmd.CombinedOutput()
	return strings.TrimSpace(string(out))
}

// TestListFiles_TrackedUntrackedIgnored pins the listing rule: tracked plus
// untracked files, never ignored ones, paths with spaces and non-ASCII
// characters unquoted and intact.
func TestListFiles_TrackedUntrackedIgnored(t *testing.T) {
	ctx := context.Background()
	dir, git := listRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "committed.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("build/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "build"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "build", "out.txt"), []byte("build\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "committed.txt", ".gitignore")
	git("commit", "-qm", "init")
	if err := os.WriteFile(filepath.Join(dir, "wip.txt"), []byte("w\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "with space.txt"), []byte("s\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wörk.txt"), []byte("ö\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, truncated, err := ListFiles(ctx, dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".gitignore", "committed.txt", "wip.txt", "with space.txt", "wörk.txt"}
	if !slices.Equal(files, want) {
		t.Errorf("ListFiles = %q, want %q", files, want)
	}
	if truncated {
		t.Error("truncated = true, want false")
	}
}

// TestListFiles_DeduplicatesCachedAndOthers pins that a path arriving from
// both --cached and --others (mid-merge states) is listed once.
func TestListFiles_DeduplicatesCachedAndOthers(t *testing.T) {
	ctx := context.Background()
	dir, git := listRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "a.txt")
	git("commit", "-qm", "base")
	git("checkout", "-q", "-b", "other")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("other\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("commit", "-aqm", "other side")
	git("checkout", "-q", "main")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("commit", "-aqm", "main side")
	gitLoose(dir, "merge", "other") // conflict on a.txt; the exit 1 is expected

	files, _, err := ListFiles(ctx, dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, f := range files {
		if f == "a.txt" {
			count++
		}
	}
	if count == 0 {
		t.Fatalf("a.txt missing from list: %q", files)
	}
	if count > 1 {
		t.Errorf("a.txt listed %d times, want once: %q", count, files)
	}
}

// TestListFiles_SubdirectoryCwd pins that paths are relative to the cwd the
// listing ran in — the same root /files resolves `?path=` against.
func TestListFiles_SubdirectoryCwd(t *testing.T) {
	ctx := context.Background()
	dir, git := listRepo(t)
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "top.txt"), []byte("t\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "nested.txt"), []byte("n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "top.txt", "sub/nested.txt")
	git("commit", "-qm", "init")

	files, _, err := ListFiles(ctx, filepath.Join(dir, "sub"), 100)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(files, []string{"nested.txt"}) {
		t.Errorf("ListFiles(subdir) = %q, want [nested.txt]", files)
	}
}

// TestListFiles_CapTruncates pins the cut with a lowered limit.
func TestListFiles_CapTruncates(t *testing.T) {
	ctx := context.Background()
	dir, git := listRepo(t)
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("add", "a.txt", "b.txt", "c.txt")
	git("commit", "-qm", "init")

	files, truncated, err := ListFiles(ctx, dir, 2)
	if err != nil || !slices.Equal(files, []string{"a.txt", "b.txt"}) || !truncated {
		t.Errorf("capped ListFiles = %q %v (err %v), want [a.txt b.txt] true", files, truncated, err)
	}

	// Exact-at-limit is NOT truncated.
	files, truncated, err = ListFiles(ctx, dir, 3)
	if err != nil || !slices.Equal(files, []string{"a.txt", "b.txt", "c.txt"}) || truncated {
		t.Errorf("exact-at-limit ListFiles = %q %v (err %v), want [a.txt b.txt c.txt] false", files, truncated, err)
	}
}

// TestListFiles_EmptyRepoReturnsEmptyNonNil pins the brand-new repo: `"files":
// []` on the wire, which means a non-nil empty slice from ListFiles.
func TestListFiles_EmptyRepoReturnsEmptyNonNil(t *testing.T) {
	ctx := context.Background()
	dir, _ := listRepo(t) // git init, nothing committed
	files, truncated, err := ListFiles(ctx, dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if files == nil || len(files) != 0 || truncated {
		t.Errorf("empty repo ListFiles = %q %v, want empty non-nil slice, false", files, truncated)
	}
}

// TestListFiles_NonRepoIs422 pins the error mapping: a non-repo cwd (and a
// missing git binary) is the git endpoints' 422, not 500.
func TestListFiles_NonRepoIs422(t *testing.T) {
	if _, _, err := ListFiles(context.Background(), t.TempDir(), 100); statusOf(err) != http.StatusUnprocessableEntity {
		t.Errorf("non-repo err = %v, want 422", err)
	}
}

// TestListFiles_IncludesSubmoduleGitlink pins that a submodule shows up as its
// gitlink path: opening it via /files answers "400 path is a directory", which
// the spec accepts.
func TestListFiles_IncludesSubmoduleGitlink(t *testing.T) {
	ctx := context.Background()
	dir, git := listRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "root.txt"), []byte("r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "root.txt")
	git("commit", "-qm", "init")
	nested := filepath.Join(t.TempDir(), "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	nestedDir, gitNested := listRepoAt(t, nested)
	if err := os.WriteFile(filepath.Join(nestedDir, "inner.txt"), []byte("i\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitNested("add", "inner.txt")
	gitNested("commit", "-qm", "init")
	git("-c", "protocol.file.allow=always", "submodule", "add", "-q", nested, "nested")
	git("commit", "-qm", "add submodule")

	files, _, err := ListFiles(ctx, dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(files, "nested") {
		t.Errorf("gitlink path %q missing from %q", "nested", files)
	}
}
