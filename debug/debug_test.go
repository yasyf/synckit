package debug

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// completedDumps lists the finished dump files under dir, ignoring the in-flight
// "dump-*.tmp" a write renames away — so a poll never observes a partial dump.
func completedDumps(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dump dir: %v", err)
	}
	var out []os.DirEntry
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "goroutine-") && strings.HasSuffix(e.Name(), ".txt") {
			out = append(out, e)
		}
	}
	return out
}

// TestDumpOnSIGUSR1 arms the handler, sends the process SIGUSR1, and asserts a completed
// goroutine dump file lands under dir. The default signal handling is untouched, so the
// test process keeps running past the signal.
func TestDumpOnSIGUSR1(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := DumpOnSIGUSR1(ctx, dir); err != nil {
		t.Fatalf("DumpOnSIGUSR1: %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill SIGUSR1: %v", err)
	}

	var dumps []os.DirEntry
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dumps = completedDumps(t, dir)
		if len(dumps) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(dumps) != 1 {
		t.Fatalf("completed dump files = %d, want exactly 1", len(dumps))
	}

	name := dumps[0].Name()
	data, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // G304: test reads a file it wrote in its own temp dir.
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	if !strings.Contains(string(data), "goroutine") {
		t.Fatalf("dump %q missing a goroutine stack header", name)
	}
}

// TestDumpOnSIGUSR1StopsOnCtxCancel proves the listener goroutine and its SIGUSR1
// registration are released when the ctx is cancelled, so an early serve() return never
// leaks them and a later serve in-process re-registers cleanly.
func TestDumpOnSIGUSR1StopsOnCtxCancel(t *testing.T) {
	// Warm up the os/signal machinery — its global goroutine is a process-lifetime
	// singleton — so the leak check measures only the listener goroutine.
	warm, warmCancel := context.WithCancel(context.Background())
	if err := DumpOnSIGUSR1(warm, t.TempDir()); err != nil {
		t.Fatalf("DumpOnSIGUSR1 warmup: %v", err)
	}
	warmCancel()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	if err := DumpOnSIGUSR1(ctx, dir); err != nil {
		t.Fatalf("DumpOnSIGUSR1: %v", err)
	}
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	exited := false
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			exited = true
			break
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if !exited {
		t.Fatalf("goroutines = %d after ctx cancel, want <= %d (listener leaked)", runtime.NumGoroutine(), before)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill SIGUSR1 after ctx cancel: %v", err)
	}
	deadline = time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if dumps := completedDumps(t, dir); len(dumps) != 0 {
			t.Fatalf("completed dump files after ctx cancel = %d, want 0", len(dumps))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
