package hostregistry

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
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
	seen := make(map[string]bool)
	for _, entry := range entries {
		if entry.Name() != "cli-processes.lock" && entry.Name() != "cli-workers.db" {
			t.Fatalf("unexpected CLI owner file %q", entry.Name())
		}
		seen[entry.Name()] = true
	}
	if !seen["cli-processes.lock"] || !seen["cli-workers.db"] {
		t.Fatalf("CLI owner files = %v", seen)
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
	}); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, worker.ErrBudgetTooSmall) {
		t.Fatalf("context exit = %v, want deadline or bounded-budget rejection", err)
	}
	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("owner remained locked after exits: %v", err)
	}
}

func TestWithExecRunnerRecoversPriorTrackedProcess(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	directory, err := Mesh.Dir()
	if err != nil {
		t.Fatalf("Mesh.Dir: %v", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	cmd := exec.Command("sleep", "9999") //nolint:gosec // fixed test command
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start prior process: %v", err)
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
			t.Error("prior process cleanup did not settle")
		}
	})
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "cli-workers.db")},
		Generation: "prior-generation",
	}
	if _, err := reaper.TrackGroup(context.Background(), cmd.Process.Pid, proc.RecoveryTask); err != nil {
		t.Fatalf("track prior process: %v", err)
	}
	if err := WithExecRunner(context.Background(), func(Runner) error { return nil }); err != nil {
		t.Fatalf("recover prior process: %v", err)
	}
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("prior tracked process remained live")
	}
}
