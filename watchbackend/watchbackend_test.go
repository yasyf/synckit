package watchbackend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// shrinkRewatch shrinks the fsnotify sweep interval so recovery tests observe a
// re-walk in well under a test deadline, restoring the default on cleanup.
func shrinkRewatch(t *testing.T) {
	t.Helper()
	prev := rewatchInterval
	rewatchInterval = 40 * time.Millisecond
	t.Cleanup(func() { rewatchInterval = prev })
}

// fsHarness runs Run on a background goroutine and buffers its fired ids so
// a test can await events and drain between phases.
type fsHarness struct {
	events chan string
	done   chan error
}

func startFsnotify(t *testing.T, dirsByID map[string][]string) *fsHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &fsHarness{events: make(chan string, 128), done: make(chan error, 1)}
	go func() {
		h.done <- Run(ctx, dirsByID, func(id string) {
			select {
			case h.events <- id:
			default:
			}
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-h.done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return h
}

func (h *fsHarness) drain() {
	for {
		select {
		case <-h.events:
		default:
			return
		}
	}
}

// drainUntilQuiet consumes events until none arrive for quiet, so a stale burst
// (e.g. a delete storm) cannot satisfy a later assertion.
func (h *fsHarness) drainUntilQuiet(quiet time.Duration) {
	for {
		select {
		case <-h.events:
		case <-time.After(quiet):
			return
		}
	}
}

// collectFires waits up to firstWait for the first event, then tallies every id until
// quiet elapses with no further event, returning the exact per-id counts. Blocking for
// the first fire keeps a slow recovery sweep from being mistaken for silence; a settled
// sweep re-fires nothing, so the quiet window closes the tally.
func (h *fsHarness) collectFires(firstWait, quiet time.Duration) map[string]int {
	counts := map[string]int{}
	select {
	case got := <-h.events:
		counts[got]++
	case <-time.After(firstWait):
		return counts
	}
	for {
		select {
		case got := <-h.events:
			counts[got]++
		case <-time.After(quiet):
			return counts
		}
	}
}

// awaitEvent waits up to 2s for id to fire, running poke (if non-nil) before each
// poll so a test can keep nudging a dir until a late watch goes live.
func awaitEvent(t *testing.T, h *fsHarness, id string, poke func()) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if poke != nil {
			poke()
		}
		select {
		case got := <-h.events:
			if got == id {
				return
			}
		case <-time.After(25 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for fsnotify event %q", id)
		}
	}
}

// awaitBoth pokes until both ids have fired within the deadline, proving a single
// event under an overlapped dir fanned out to every covering id.
func awaitBoth(t *testing.T, h *fsHarness, id1, id2 string, poke func()) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	seen := map[string]bool{}
	for {
		poke()
		select {
		case got := <-h.events:
			seen[got] = true
			if seen[id1] && seen[id2] {
				return
			}
		case <-time.After(25 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for both %q and %q to fire (saw %v)", id1, id2, seen)
		}
	}
}

// awaitOnly pokes until want fires, then asserts other never fires within a grace
// window, proving the event routed to only the one covering id.
func awaitOnly(t *testing.T, h *fsHarness, want, other string, poke func()) {
	t.Helper()
	awaitEvent(t, h, want, poke)
	grace := time.After(200 * time.Millisecond)
	for {
		select {
		case got := <-h.events:
			if got == other {
				t.Fatalf("event fired %q, want only %q", other, want)
			}
		case <-grace:
			return
		}
	}
}

// createPoke returns a poke that creates a fresh file in dir each call, so every
// poke is a Create event that fires on both kqueue and inotify once dir is watched.
func createPoke(t *testing.T, dir string) func() {
	t.Helper()
	var n int
	return func() {
		n++
		p := filepath.Join(dir, fmt.Sprintf("poke-%d", n))
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("write poke %s: %v", p, err)
		}
	}
}

// writePoke returns a poke that rewrites file with fresh content each call, so every
// poke is a modification that fires the file's own watch on both kqueue and inotify.
func writePoke(t *testing.T, file string) func() {
	t.Helper()
	var n int
	return func() {
		n++
		if err := os.WriteFile(file, []byte(fmt.Sprintf("v%d", n)), 0o600); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
	}
}

func TestRunFsnotify(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	events := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, map[string][]string{"x": {dir}}, func(id string) {
			events <- id
		})
	}()

	// Give the watcher a moment to register before writing.
	deadline := time.After(2 * time.Second)
	var fired bool
	for !fired {
		if err := os.WriteFile(file, []byte("changed"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		select {
		case id := <-events:
			if id != "x" {
				t.Fatalf("onEvent id = %q, want %q", id, "x")
			}
			fired = true
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("timed out waiting for fsnotify event")
		}
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunFsnotifyRecoversDeletedDir(t *testing.T) {
	shrinkRewatch(t)
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("seed nested: %v", err)
	}
	h := startFsnotify(t, map[string][]string{"x": {root}})

	// The tree is live: a create under the root fires.
	awaitEvent(t, h, "x", createPoke(t, root))

	// Delete the whole tree; fsnotify self-cleans the watches on root and nested.
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove root: %v", err)
	}
	// Drain the delete burst until quiet so a stale delete callback cannot satisfy
	// the recovery assertions below; sweeps over the missing root fire nothing.
	h.drainUntilQuiet(200 * time.Millisecond)

	// Recreate the tree; the sweep must re-walk the root and re-establish both watches.
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("recreate nested: %v", err)
	}

	// Fire-on-rewatch: the sweep fires the id as it re-adds each recreated dir.
	awaitEvent(t, h, "x", nil)

	// Re-walk proof: the recreated nested dir is watched again in its own right, so a
	// create inside it (which only its own watch can observe) fires.
	h.drain()
	awaitEvent(t, h, "x", createPoke(t, nested))
}

func TestRunFsnotifyPicksUpLateDir(t *testing.T) {
	shrinkRewatch(t)
	parent := t.TempDir()
	late := filepath.Join(parent, "late") // does not exist when Run starts
	sentinel := t.TempDir()               // exists at startup, a deterministic barrier
	h := startFsnotify(t, map[string][]string{"late": {late}, "sentinel": {sentinel}})

	// The startup walk and the select loop share one goroutine, so a sentinel event
	// can only arrive once startup finished — proving the late root was already
	// recorded as not-yet-watchable, no sleep-based race with the startup walk.
	awaitEvent(t, h, "sentinel", createPoke(t, sentinel))
	h.drain()

	if err := os.Mkdir(late, 0o700); err != nil {
		t.Fatalf("create late dir: %v", err)
	}

	// Fire-on-add: the sweep re-walks the declared root, finds it now present, watches
	// it, and fires the id.
	awaitEvent(t, h, "late", nil)

	// The newly watched dir delivers events.
	h.drain()
	awaitEvent(t, h, "late", createPoke(t, late))
}

func TestRunFsnotifyWatchesNestedDirs(t *testing.T) {
	// A huge sweep interval guarantees the nested watch can only come from the Create
	// fast-path, never a sweep re-walk.
	prev := rewatchInterval
	rewatchInterval = 10 * time.Second
	t.Cleanup(func() { rewatchInterval = prev })

	root := t.TempDir()
	h := startFsnotify(t, map[string][]string{"x": {root}})

	// Establish the root watch.
	awaitEvent(t, h, "x", createPoke(t, root))
	h.drain()

	// Create root/a: the root's watch observes it, and the fast-path adds a watch on
	// it (before firing) and fires x. Awaiting x proves root/a is now watched.
	a := filepath.Join(root, "a")
	if err := os.Mkdir(a, 0o700); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	awaitEvent(t, h, "x", nil)
	h.drain()

	// Create root/a/b: only root/a's own watch can observe this, so a fired x proves
	// the fast-path watched root/a rather than a sweep.
	b := filepath.Join(a, "b")
	if err := os.Mkdir(b, 0o700); err != nil {
		t.Fatalf("mkdir b: %v", err)
	}
	awaitEvent(t, h, "x", nil)

	// root/a/b is watched in its own right: a create inside it (which only its own
	// watch can observe) fires — the fast-path recursed into the new subtree.
	h.drain()
	awaitEvent(t, h, "x", createPoke(t, b))
}

func TestRunFsnotifySymlinkRoot(t *testing.T) {
	shrinkRewatch(t)
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Declare the symlink itself as the root: the walker's os.Stat follows it, so the
	// real directory is watched under the declared (symlink) path.
	h := startFsnotify(t, map[string][]string{"x": {link}})

	// A write through the symlink fires: fsnotify reports the event under the watched
	// (symlink) path, which is the routing key.
	awaitEvent(t, h, "x", createPoke(t, link))
}

func TestRunFsnotifyOverlappingRoots(t *testing.T) {
	shrinkRewatch(t)
	parent := t.TempDir()
	child := filepath.Join(parent, "child")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	h := startFsnotify(t, map[string][]string{"A": {parent}, "B": {child}})

	// A write in the child dir is covered by both roots — A via its recursive walk of
	// parent, and B as its own declared root — so both ids fire for the one event.
	awaitBoth(t, h, "A", "B", createPoke(t, child))

	// Drain the child-poke burst until quiet so a stray in-flight B cannot leak into
	// the parent-only phase below.
	h.drainUntilQuiet(200 * time.Millisecond)

	// A write directly in parent, outside child, is covered only by A.
	awaitOnly(t, h, "A", "B", createPoke(t, parent))
}

func TestRunFsnotifyRecoveryFansOutToOverlappingOwners(t *testing.T) {
	shrinkRewatch(t)
	parent := t.TempDir()
	nested := filepath.Join(parent, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("seed nested: %v", err)
	}
	// A covers parent recursively; B declares the overlapped nested dir directly.
	h := startFsnotify(t, map[string][]string{"A": {parent}, "B": {nested}})

	// Both trees are live: a write in the overlapped nested dir fires both owners.
	awaitBoth(t, h, "A", "B", createPoke(t, nested))
	h.drainUntilQuiet(200 * time.Millisecond)

	// Delete the whole tree; fsnotify self-cleans the watches on parent and nested.
	if err := os.RemoveAll(parent); err != nil {
		t.Fatalf("remove parent: %v", err)
	}
	h.drainUntilQuiet(200 * time.Millisecond)

	// Recreate as one atomic rename: a sweep tick mid-MkdirAll would hand nested's
	// recovery to the Create fast-path, which fires only parent's owners, starving B.
	stage := parent + ".stage"
	if err := os.MkdirAll(filepath.Join(stage, "nested"), 0o700); err != nil {
		t.Fatalf("stage tree: %v", err)
	}
	if err := os.Rename(stage, parent); err != nil {
		t.Fatalf("rename stage: %v", err)
	}

	// The fan-out shape is exact: A owns parent and nested, so it fires once per
	// recovered dir (2); B owns nested alone, so it fires once (1). No owner is missed
	// and none double-fires.
	counts := h.collectFires(2*time.Second, 3*rewatchInterval)
	if counts["A"] != 2 || counts["B"] != 1 || len(counts) != 2 {
		t.Fatalf("recovery fires = %v, want map[A:2 B:1]", counts)
	}
}

func TestRunFsnotifySameIDOverlapDedupes(t *testing.T) {
	shrinkRewatch(t)
	parent := t.TempDir()
	child := filepath.Join(parent, "child")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	// x declares parent and its own child: overlapping same-id roots.
	h := startFsnotify(t, map[string][]string{"x": {parent, child}})

	// Live: a write in child (covered by both declarations) fires x.
	awaitEvent(t, h, "x", createPoke(t, child))
	h.drainUntilQuiet(200 * time.Millisecond)

	// Delete the whole tree so no surviving watch observes the recreate — recovery must
	// flow through the sweep, which re-adds parent and child in one pass. (A nested-only
	// delete would be recovered by the Create fast-path instead, never exercising the
	// sweep's per-id dedupe.)
	if err := os.RemoveAll(parent); err != nil {
		t.Fatalf("remove parent: %v", err)
	}
	h.drainUntilQuiet(200 * time.Millisecond)

	// Atomic rename again: recovery via the Create fast-path would also total 2,
	// hiding the dedupe regression.
	stage := parent + ".stage"
	if err := os.MkdirAll(filepath.Join(stage, "child"), 0o700); err != nil {
		t.Fatalf("stage tree: %v", err)
	}
	if err := os.Rename(stage, parent); err != nil {
		t.Fatalf("rename stage: %v", err)
	}

	// Both same-id declarations re-add child from the shared stale snapshot, but the
	// per-id sweep dedupe collapses the duplicate: x fires once for parent and once for
	// child — exactly 2. Without the dedupe, child would fire twice (total 3).
	counts := h.collectFires(2*time.Second, 3*rewatchInterval)
	if counts["x"] != 2 || len(counts) != 1 {
		t.Fatalf("recovery fires = %v, want map[x:2]", counts)
	}
}

func TestRunFsnotifyDoesNotFollowNestedSymlink(t *testing.T) {
	shrinkRewatch(t)
	root := t.TempDir()
	outside := t.TempDir() // a separate tree the fast-path must not recurse into
	deep := filepath.Join(outside, "deep")
	if err := os.MkdirAll(deep, 0o700); err != nil {
		t.Fatalf("seed deep: %v", err)
	}
	h := startFsnotify(t, map[string][]string{"x": {root}})

	// Establish the root watch.
	awaitEvent(t, h, "x", createPoke(t, root))
	h.drain()

	// A symlink to the outside dir, created inside the root: the Create fast-path Lstats
	// it, sees a symlink (not a real dir), and must not recurse — so no persistent watch
	// is planted on the target's subtree.
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	// The symlink Create is itself an event under the watched root; drain it.
	h.drainUntilQuiet(200 * time.Millisecond)

	// A write in a nested subdir of the symlink target must not fire: only the buggy
	// os.Stat fast-path, recursing through the symlink, would have watched it. (A write
	// directly in the target is not asserted here — the kqueue backend follows the
	// symlink to track the watched root's own entries, one level deep, regardless of
	// this fix; the defect is the recursive descent into the target's subtree.)
	if err := os.WriteFile(filepath.Join(deep, "data"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write deep: %v", err)
	}
	select {
	case got := <-h.events:
		t.Fatalf("event fired %q for a write under an un-watched symlink target subtree", got)
	case <-time.After(300 * time.Millisecond):
	}

	// Sanity: the root itself still fires on a direct write.
	awaitEvent(t, h, "x", createPoke(t, root))
}

func TestRunWatchedFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Declare the regular file itself, not its parent: walkDirs visits it alone and
	// watches it individually, so the parent dir stays unwatched.
	h := startFsnotify(t, map[string][]string{"x": {file}})

	// A write to the declared file fires its own watch. (Parent unwatched, so there is
	// no sibling to isolate here — that is TestRunFileAndDirOverlap's job.)
	awaitEvent(t, h, "x", writePoke(t, file))
}

func TestRunWatchedFileRecoversReplace(t *testing.T) {
	shrinkRewatch(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	h := startFsnotify(t, map[string][]string{"x": {file}})

	// Live: a write to the watched file fires.
	awaitEvent(t, h, "x", writePoke(t, file))
	h.drainUntilQuiet(200 * time.Millisecond)

	// Replace atomically: rename a staged sibling over the watched path. The old inode is
	// unlinked (watch torn down); only the sweep recovers the new one, parent unwatched.
	tmp := filepath.Join(dir, "data.tmp")
	if err := os.WriteFile(tmp, []byte("replaced"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := os.Rename(tmp, file); err != nil {
		t.Fatalf("rename over file: %v", err)
	}

	// Fire-on-converge: the first callback after the replace — old-inode teardown or the
	// sweep re-adding the new inode — is the convergence trigger, the behavioral contract.
	awaitEvent(t, h, "x", nil)
	h.drainUntilQuiet(200 * time.Millisecond)

	// The watch is re-established on the new inode: a subsequent write fires again.
	awaitEvent(t, h, "x", writePoke(t, file))
}

func TestRunWatchedFilePicksUpLateFile(t *testing.T) {
	shrinkRewatch(t)
	dir := t.TempDir()
	late := filepath.Join(dir, "data") // does not exist when Run starts
	sentinel := t.TempDir()            // exists at startup, a deterministic barrier
	h := startFsnotify(t, map[string][]string{"late": {late}, "sentinel": {sentinel}})

	// The startup walk and the select loop share one goroutine, so a sentinel event can
	// only arrive once startup finished — proving the late path was already recorded as
	// not-yet-watchable, no sleep-based race with the startup walk.
	awaitEvent(t, h, "sentinel", createPoke(t, sentinel))
	h.drain()

	if err := os.WriteFile(late, []byte("init"), 0o600); err != nil {
		t.Fatalf("create late file: %v", err)
	}

	// Fire-on-add: the sweep re-walks the declared path, finds the file now present,
	// watches it, and fires the id. The parent dir is unwatched, so only the sweep can
	// pick it up.
	awaitEvent(t, h, "late", nil)

	// The newly watched file delivers events: a subsequent write fires.
	h.drain()
	awaitEvent(t, h, "late", writePoke(t, late))
}

func TestRunFileAndDirOverlap(t *testing.T) {
	parent := t.TempDir()
	file := filepath.Join(parent, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// A watches the parent dir recursively; B declares the file inside it directly.
	h := startFsnotify(t, map[string][]string{"A": {parent}, "B": {file}})

	// Prove both watches are live: a create in parent fires A, and B's file watch is
	// established in the same synchronous startup pass. Then settle.
	awaitEvent(t, h, "A", createPoke(t, parent))
	h.drainUntilQuiet(200 * time.Millisecond)

	// One write to the file must reach BOTH owners: B via its exact-path watch, A via the
	// parent-dir ids in the union. Collecting with nil poke proves one write fanned out.
	if err := os.WriteFile(file, []byte("changed"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	counts := h.collectFires(2*time.Second, 200*time.Millisecond)
	if counts["A"] < 1 || counts["B"] < 1 {
		t.Fatalf("fires = %v, want both A>=1 and B>=1", counts)
	}

	// A sibling create in parent (a path other than the watched file) has no exact match,
	// so it fires only A's dir watch, never B's file watch.
	h.drainUntilQuiet(200 * time.Millisecond)
	awaitOnly(t, h, "A", "B", createPoke(t, parent))
}

func TestRunSameIDFileAndDirFiresOnce(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// x declares BOTH the parent dir and the file: the file matches x as an exact-path
	// owner and a parent-dir owner, but the union must collapse them to one fire per event.
	h := startFsnotify(t, map[string][]string{"x": {dir, file}})

	// Prove the watches are live (a create in dir fires x, and the file watch is set in the
	// same synchronous startup pass), then settle.
	awaitEvent(t, h, "x", createPoke(t, dir))
	h.drainUntilQuiet(200 * time.Millisecond)

	// A single append is one write syscall — exactly one kqueue NOTE_WRITE — so darwin can
	// assert an exact count; os.WriteFile's O_TRUNC could add a NOTE_ATTRIB event and race.
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // G304: file is a test-owned temp path
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatalf("append file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	counts := h.collectFires(2*time.Second, 200*time.Millisecond)
	if runtime.GOOS == "darwin" {
		// kqueue delivers one coalesced event, so the union fires x exactly once. A
		// double-fire bug (per exact owner AND per dir owner) would give 2 — the discriminator.
		if counts["x"] != 1 || len(counts) != 1 {
			t.Fatalf("fires = %v, want map[x:1]", counts)
		}
		return
	}
	// inotify reports the modify on both the dir and file descriptors, so an exact count is
	// meaningless here; assert at-least-once and that only x fired.
	if counts["x"] < 1 || len(counts) != 1 {
		t.Fatalf("fires = %v, want x>=1 and only x", counts)
	}
}
