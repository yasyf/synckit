package hostregistry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/clirunner"
)

func TestWithExecRunnerOwnsOnlyMeshCLIProcessFiles(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("WithExecRunner: %v", err)
	}
	directory, err := Mesh.Dir()
	if err != nil {
		t.Fatalf("Mesh.Dir: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	owned := map[string]bool{"cli-processes.lock": true, "cli-processes.db": true, "cli-processes.db.lock": true}
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if !owned[entry.Name()] {
			t.Fatalf("unexpected CLI owner file %q", entry.Name())
		}
		seen[entry.Name()] = true
	}
	if !seen["cli-processes.lock"] || !seen["cli-processes.db.lock"] {
		t.Fatalf("CLI owner files = %v, want both the scope lock and the record lock", seen)
	}
	rootEntries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	if len(rootEntries) != 1 || rootEntries[0].Name() != MeshName {
		t.Fatalf("config root entries = %v", rootEntries)
	}
}

func TestWithExecRunnerRejectsEscapedRunner(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var escaped Runner
	if err := WithExecRunner(context.Background(), func(runner Runner) error {
		escaped = runner
		return nil
	}); err != nil {
		t.Fatalf("WithExecRunner: %v", err)
	}
	if _, err := escaped.Local(context.Background(), "true"); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("escaped Local = %v, want ErrRunnerClosed", err)
	}
	if _, err := escaped.SSH(context.Background(), "peer", "true"); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("escaped SSH = %v, want ErrRunnerClosed", err)
	}
}

func TestWithExecRunnerSerializesOwners(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	entered := make(chan struct{})
	release := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- WithExecRunner(context.Background(), func(Runner) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := WithExecRunner(ctx, func(Runner) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended owner = %v, want context deadline", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first owner: %v", err)
	}
	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("owner remained locked: %v", err)
	}
}

func TestWithExecRunnerCleansEveryCallbackExit(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	sentinel := errors.New("callback failed")

	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("success: %v", err)
	}
	if err := WithExecRunner(context.Background(), func(Runner) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic was not propagated")
			}
		}()
		_ = WithExecRunner(context.Background(), func(Runner) error { panic("boom") })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := WithExecRunner(ctx, func(runner Runner) error {
		_, err := runner.Local(ctx, "sleep", "9999")
		return err
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context exit = %v, want the deadline surfaced", err)
	}
	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("owner remained locked after exits: %v", err)
	}
}

// TestCLIProcessScopeSettlesAdoptedProcess proves the CLI process scope's ownership seam
// over the mesh directory: a process adopted into the scope is terminated and proven gone
// when the scope exits, and the next scope over the same record path has nothing left to
// reclaim.
func TestCLIProcessScopeSettlesAdoptedProcess(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	directory, err := Mesh.Dir()
	if err != nil {
		t.Fatalf("Mesh.Dir: %v", err)
	}
	process, waited := startAdoptable(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clirunner.WithOwned(ctx, directory, func(owned *daemonkit.Owned) error {
		_, err := owned.Adopt(ctx, process.Pid)
		return err
	}); err != nil {
		t.Fatalf("WithOwned: %v", err)
	}
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the adopted process outlived the CLI process scope that owned it")
	}
	if err := clirunner.WithOwned(ctx, directory, func(owned *daemonkit.Owned) error {
		if left := owned.Reclaimed(); len(left) != 0 {
			return fmt.Errorf("the next scope reclaimed %+v, want the settled scope to have left its record clean", left)
		}
		return nil
	}); err != nil {
		t.Fatalf("next CLI process scope: %v", err)
	}
}

// TestCLIProcessScopeReclaimsAPriorGenerationsProcess proves the reason the CLI
// scope keeps a durable record at all: a generation that adopted a process and
// died without settling leaves it running, and the next scope over the same
// record kills it and reports it as reclaimed.
func TestCLIProcessScopeReclaimsAPriorGenerationsProcess(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	directory, err := Mesh.Dir()
	if err != nil {
		t.Fatalf("Mesh.Dir: %v", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create mesh directory: %v", err)
	}
	process, waited := startAdoptable(t)

	stub := exec.Command( //nolint:gosec // fixed test binary and arguments
		buildOwnStub(t), filepath.Join(directory, "cli-processes.db"), strconv.Itoa(process.Pid),
	)
	if out, err := stub.CombinedOutput(); err != nil {
		t.Fatalf("prior generation: %v\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var reclaimed []daemonkit.Reclaimed
	if err := clirunner.WithOwned(ctx, directory, func(owned *daemonkit.Owned) error {
		reclaimed = owned.Reclaimed()
		return nil
	}); err != nil {
		t.Fatalf("WithOwned: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].PID != process.Pid {
		t.Fatalf("reclaimed = %+v, want exactly the prior generation's process %d", reclaimed, process.Pid)
	}
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the prior generation's process outlived the scope that reclaimed it")
	}
}

// startAdoptable starts one long-lived session leader for a scope to own and
// reaps it however the test ends. The returned channel closes once it has been
// waited on, so a test can prove it is gone rather than merely signalled.
func startAdoptable(t *testing.T) (*os.Process, <-chan struct{}) {
	t.Helper()
	cmd := exec.Command("sleep", "9999") //nolint:gosec // fixed test command
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start adopted process: %v", err)
	}
	waited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(waited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Error("adopted process cleanup did not settle")
		}
	})
	return cmd.Process, waited
}

func buildOwnStub(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), "ownstub")
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/ownstub") //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build ownstub: %v\n%s", err, out)
	}
	return bin
}
