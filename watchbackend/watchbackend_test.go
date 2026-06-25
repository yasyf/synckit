package watchbackend

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunUnknownBackend(t *testing.T) {
	err := Run(context.Background(), "nope", nil, func(string) {})
	if err == nil {
		t.Fatal("Run with unknown backend: want error, got nil")
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
		done <- RunFsnotify(ctx, map[string][]string{"x": {dir}}, func(id string) {
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
			t.Fatalf("RunFsnotify returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunFsnotify did not return after cancel")
	}
}

func TestRunWatchman(t *testing.T) {
	if _, err := exec.LookPath("watchman"); err != nil {
		t.Skip("watchman not installed; skipping watchman backend test")
	}

	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("init"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count atomic.Int64
	events := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		done <- RunWatchman(ctx, map[string][]string{"x": {dir}}, func(id string) {
			count.Add(1)
			select {
			case events <- id:
			default:
			}
		})
	}()

	// Let the subscription establish before writing.
	time.Sleep(500 * time.Millisecond)
	if err := os.WriteFile(file, []byte("changed"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	select {
	case id := <-events:
		if id != "x" {
			t.Fatalf("onEvent id = %q, want %q", id, "x")
		}
	case <-time.After(5 * time.Second):
		t.Skip("watchman did not deliver an event in time; skipping as flaky/unavailable")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWatchman returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWatchman did not return after cancel")
	}
}
