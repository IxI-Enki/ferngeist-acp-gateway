package workspace

import (
	"context"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestResolveRef pins the injection boundary for the compare parameters: a
// branch, a slash-named branch, a tag, a remote-tracking ref and a full sha all
// resolve to the same commit, and every malformed or unknown name is a 400.
func TestResolveRef(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	git("commit", "-q", "--allow-empty", "-m", "one")
	head := git("rev-parse", "HEAD")
	git("branch", "feat/x")
	git("tag", "v1")
	git("update-ref", "refs/remotes/origin/main", head)

	for _, name := range []string{"main", "feat/x", "v1", "origin/main", head} {
		got, err := ResolveRef(ctx, dir, name)
		if err != nil || got != head {
			t.Errorf("ResolveRef(%q) = %q, %v; want %q", name, got, err, head)
		}
	}

	for _, name := range []string{
		"",          // empty
		"-x",        // option-like
		"a b",       // space
		"a\tb",      // tab
		"a\nb",      // control character
		"a\x7fb",    // DEL
		"a..b",      // revision range
		"main@{0}",  // reflog syntax
		"main~1",    // parent syntax
		"main^",     // parent syntax
		"main:file", // object syntax
		"main?x",    // ref-name punctuation
		"main*x",    // ref-name punctuation
		"main[0]",   // ref-name punctuation
		`main\back`, // ref-name punctuation
	} {
		if _, err := ResolveRef(ctx, dir, name); statusOf(err) != http.StatusBadRequest {
			t.Errorf("ResolveRef(%q) err = %v, want 400", name, err)
		}
	}

	if _, err := ResolveRef(ctx, dir, "nope"); statusOf(err) != http.StatusBadRequest {
		t.Errorf("unknown ref err = %v, want 400", err)
	}
}

// TestResolveRefNonRepo pins that a cwd outside a repository is a 422, not the
// "unknown ref" 400: the name never had a chance to resolve.
func TestResolveRefNonRepo(t *testing.T) {
	if _, err := ResolveRef(context.Background(), t.TempDir(), "main"); statusOf(err) != http.StatusUnprocessableEntity {
		t.Errorf("non-repo err = %v, want 422", err)
	}
}

// TestResolveRefQualifiedName pins the disambiguation remedy for a short name a
// branch and a tag share: only the fully-qualified refs/heads/<name> is the
// branch, while the bare name resolves by git's own precedence (the tag wins).
func TestResolveRefQualifiedName(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	git("commit", "-q", "--allow-empty", "-m", "one")
	branch := git("rev-parse", "HEAD")
	git("branch", "same")
	git("commit", "-q", "--allow-empty", "-m", "two")
	tag := git("rev-parse", "HEAD")
	git("tag", "same")

	got, err := ResolveRef(ctx, dir, "refs/heads/same")
	if err != nil || got != branch {
		t.Errorf("ResolveRef(refs/heads/same) = %q, %v; want %q", got, err, branch)
	}
	if got, err := ResolveRef(ctx, dir, "same"); err != nil {
		t.Errorf("ResolveRef(same) err = %v, want the tag's tip", err)
	} else if got != tag {
		t.Errorf("ResolveRef(same) = %q, want the tag's tip %q", got, tag)
	}
}

// TestNameStatusChanges pins the parse shared by Changes and CompareChanges: a
// rename/copy reports its destination path, keeps no OldPath, and paths already
// in seen are skipped and recorded.
func TestNameStatusChanges(t *testing.T) {
	out := "M\ta.txt\n" +
		"A\tb.txt\n" +
		"D\tc.txt\n" +
		"R100\told.txt\tnew.txt\n" +
		"C75\tsrc.txt\tcopy.txt\n"
	seen := map[string]bool{"a.txt": true}
	got := nameStatusChanges(out, seen)
	want := map[string]string{"b.txt": "A", "c.txt": "D", "new.txt": "R", "copy.txt": "C"}
	if len(got) != len(want) {
		t.Fatalf("changes = %+v, want %+v", got, want)
	}
	for _, f := range got {
		if want[f.Path] != f.Status || f.OldPath != "" {
			t.Errorf("%s = %+v, want status %q and no oldPath", f.Path, f, want[f.Path])
		}
	}
	if !seen["new.txt"] {
		t.Error("seen does not record parsed paths")
	}
}

// TestCompareChanges pins the phantom-row exclusion the spec calls for: a file
// dirty against HEAD but identical to the target must not be listed, the
// status letters come from `git diff <ref>` rather than porcelain, a target-only
// file is a deletion, and nothing is staged.
func TestCompareChanges(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	write(t, filepath.Join(dir, "kept.txt"), "one\n")
	write(t, filepath.Join(dir, "moved.txt"), "moved\n")
	git("add", ".")
	git("commit", "-q", "-m", "root")
	git("branch", "target")

	// The target advances: kept.txt gains a line, target-only.txt appears.
	git("checkout", "-q", "target")
	write(t, filepath.Join(dir, "kept.txt"), "one\ntwo\n")
	write(t, filepath.Join(dir, "target-only.txt"), "new\n")
	git("add", ".")
	git("commit", "-q", "-m", "target work")
	git("checkout", "-q", "main")

	// The worktree: kept.txt holds the target's content (dirty vs HEAD,
	// identical to target), moved.txt is renamed, wip.bin is a binary untracked
	// file, and target-only.txt is gone.
	write(t, filepath.Join(dir, "kept.txt"), "one\ntwo\n")
	git("mv", "moved.txt", "renamed.txt")
	write(t, filepath.Join(dir, "wip.txt"), "wip\n")
	write(t, filepath.Join(dir, "clip.bin"), "\x00\x01\x02")

	changed, err := CompareChanges(ctx, dir, "target")
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]ChangedFile{}
	for _, f := range changed {
		byPath[f.Path] = f
	}
	if f, ok := byPath["kept.txt"]; ok {
		t.Errorf("kept.txt listed: dirty against HEAD but identical to target: %+v", f)
	}
	if f, ok := byPath["renamed.txt"]; !ok || f.Status != "R" || f.OldPath != "" {
		t.Errorf("renamed.txt = %+v (present=%v), want status R and no oldPath", f, ok)
	}
	if f, ok := byPath["wip.txt"]; !ok || f.Status != "?" || f.Added != 1 || f.Binary {
		t.Errorf("wip.txt = %+v, want untracked with 1 added line", f)
	}
	if f, ok := byPath["clip.bin"]; !ok || !f.Binary {
		t.Errorf("clip.bin = %+v (present=%v), want binary", f, ok)
	}
	if f, ok := byPath["target-only.txt"]; !ok || f.Status != "D" || f.Removed != 1 {
		t.Errorf("target-only.txt = %+v (present=%v), want D with 1 removed", f, ok)
	}
	for _, f := range changed {
		if f.Staged {
			t.Errorf("%s staged = true, want false (compare mode is flat)", f.Path)
		}
	}
}

// TestCompareChangesScopedToSubdirectory pins that paths are cwd-relative and
// changes outside cwd are dropped, from a subdirectory cwd.
func TestCompareChangesScopedToSubdirectory(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	write(t, filepath.Join(dir, "sub", "a.txt"), "1\n")
	write(t, filepath.Join(dir, "top.txt"), "top\n")
	git("add", ".")
	git("commit", "-q", "-m", "root")
	git("branch", "target")
	git("checkout", "-q", "target")
	write(t, filepath.Join(dir, "top.txt"), "target top\n")
	git("commit", "-qam", "target work")
	git("checkout", "-q", "main")

	sub := filepath.Join(dir, "sub")
	write(t, filepath.Join(sub, "a.txt"), "1\n2\n")
	write(t, filepath.Join(sub, "new.txt"), "n\n")
	write(t, filepath.Join(dir, "wip-top.txt"), "top wip\n")

	changed, err := CompareChanges(ctx, sub, "target")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]ChangedFile{}
	for _, f := range changed {
		got[f.Path] = f
	}
	if f, ok := got["a.txt"]; !ok || f.Added != 1 || f.Removed != 0 {
		t.Errorf("a.txt = %+v (present=%v), want cwd-relative path with 1 added", f, ok)
	}
	if f, ok := got["new.txt"]; !ok || f.Status != "?" || f.Added != 1 {
		t.Errorf("new.txt = %+v (present=%v), want untracked with 1 added line", f, ok)
	}
	for _, p := range []string{"top.txt", "wip-top.txt"} {
		if f, ok := got[p]; ok {
			t.Errorf("%s listed from a subdirectory cwd: %+v", p, f)
		}
	}
}

// TestCompareChangesAgainstOwnBranch pins the degenerate target: comparing HEAD
// against itself is today's working-tree view, not an error and not empty.
func TestCompareChangesAgainstOwnBranch(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	write(t, filepath.Join(dir, "a.txt"), "one\n")
	git("add", ".")
	git("commit", "-q", "-m", "root")
	git("branch", "same")
	write(t, filepath.Join(dir, "a.txt"), "two\n")
	write(t, filepath.Join(dir, "wip.txt"), "wip\n")

	changed, err := CompareChanges(ctx, dir, "same")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]ChangedFile{}
	for _, f := range changed {
		got[f.Path] = f
	}
	if f, ok := got["a.txt"]; !ok || f.Status != "M" || f.Added != 1 || f.Removed != 1 {
		t.Errorf("a.txt = %+v (present=%v), want M with 1/1", f, ok)
	}
	if f, ok := got["wip.txt"]; !ok || f.Status != "?" {
		t.Errorf("wip.txt = %+v (present=%v), want untracked", f, ok)
	}
}

// TestRefs pins the picker's data: branches, remotes and tags are classified,
// origin/HEAD-style aliases are dropped, the checked-out branch is marked, and
// default prefers origin/HEAD's branch over a local main/master.
func TestRefs(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	git("commit", "-q", "--allow-empty", "-m", "one")
	git("branch", "feat/x")
	git("tag", "v1")
	origin := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", "--bare", origin).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	git("remote", "add", "origin", origin)
	git("push", "-q", "origin", "main")
	git("symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")

	payload, err := Refs(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Ref{}
	for _, r := range payload.Refs {
		got[r.Name] = r
	}
	if r := got["main"]; r.Kind != "branch" || !r.IsHead {
		t.Errorf("main = %+v, want branch with isHead", r)
	}
	if r := got["feat/x"]; r.Kind != "branch" || r.IsHead {
		t.Errorf("feat/x = %+v, want branch without isHead", r)
	}
	if r := got["origin/main"]; r.Kind != "remote" || r.IsHead {
		t.Errorf("origin/main = %+v, want remote", r)
	}
	if r, ok := got["origin/HEAD"]; ok {
		t.Errorf("origin/HEAD listed, want the alias dropped: %+v", r)
	}
	if r := got["v1"]; r.Kind != "tag" {
		t.Errorf("v1 = %+v, want tag", r)
	}
	if payload.Default != "main" {
		t.Errorf("Default = %q, want main", payload.Default)
	}

	if _, err := Refs(ctx, t.TempDir()); statusOf(err) != http.StatusUnprocessableEntity {
		t.Errorf("non-repo err = %v, want 422", err)
	}
}

// TestRefsDefaultFallback pins the rest of the default ladder: a local
// main/master when there is no origin/HEAD, and "" when neither exists.
func TestRefsDefaultFallback(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	git("commit", "-q", "--allow-empty", "-m", "one")

	payload, err := Refs(ctx, dir)
	if err != nil || payload.Default != "main" {
		t.Fatalf("Refs = %+v, %v; want Default main", payload, err)
	}
	git("branch", "-m", "main", "trunk")
	payload, err = Refs(ctx, dir)
	if err != nil || payload.Default != "" {
		t.Fatalf("Refs = %+v, %v; want empty Default", payload, err)
	}
}

// TestRefsEmptyRepository pins the no-refs case: with nothing to pick, the
// picker's chip shows only "Working tree", so the list is empty and there is no
// default target.
func TestRefsEmptyRepository(t *testing.T) {
	ctx := context.Background()
	dir, _ := historyRepo(t)

	payload, err := Refs(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Refs) != 0 || payload.Default != "" {
		t.Fatalf("Refs = %+v, want no refs and empty Default", payload)
	}
}

// TestCompareChangesTargetPathUntrackedLocally pins the collision between the
// two halves of the compare set: a path the target tracks but that exists here
// untracked must be listed once, as "?", not twice, and must not be reported as
// a deletion while its content sits on disk.
func TestCompareChangesTargetPathUntrackedLocally(t *testing.T) {
	ctx := context.Background()
	dir, git := historyRepo(t)
	write(t, filepath.Join(dir, "f.txt"), "v1\n")
	git("add", ".")
	git("commit", "-q", "-m", "init")
	git("branch", "target")
	git("checkout", "-q", "target")
	write(t, filepath.Join(dir, "only.txt"), "target\n")
	write(t, filepath.Join(dir, "f.txt"), "v2\n")
	git("add", ".")
	git("commit", "-q", "-m", "target work")
	git("checkout", "-q", "main")
	write(t, filepath.Join(dir, "only.txt"), "local untracked\n")

	changed, err := CompareChanges(ctx, dir, "target")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, f := range changed {
		if f.Path != "only.txt" {
			continue
		}
		count++
		if f.Status != "?" {
			t.Errorf("only.txt status = %q, want ? (it exists on disk, untracked)", f.Status)
		}
		if f.Added != 1 || f.Removed != 0 {
			t.Errorf("only.txt = %+v, want 1 added line read from disk", f)
		}
	}
	if count != 1 {
		t.Fatalf("only.txt listed %d times, want once: %+v", count, changed)
	}
}
