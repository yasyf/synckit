package hostregistry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

// testCfg is the Config the in-package tests drive; Name selects the per-tool
// config subdir under the temp XDG_CONFIG_HOME each test sets.
var testCfg = Config{Name: "synckit"}

func TestWithLockRunsFnAndCreatesLockFile(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	ran := false
	if err := testCfg.WithLock(context.Background(), func() error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if !ran {
		t.Error("WithLock: fn did not run")
	}

	dir, err := testCfg.Dir()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, lockFile)); err != nil {
		t.Errorf("lock file missing: %v", err)
	}

	if err := testCfg.WithLock(context.Background(), func() error { return nil }); err != nil {
		t.Errorf("second WithLock: %v", err)
	}
}

func TestWithLockContendedReturnsErrLockBusy(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	dir, err := testCfg.Dir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	// Hold the lock via an independent flock handle on the same file, simulating
	// another process holding the reconcile lock.
	holder := flock.New(filepath.Join(dir, lockFile))
	locked, err := holder.TryLock()
	if err != nil {
		t.Fatalf("hold lock: %v", err)
	}
	if !locked {
		t.Fatal("could not acquire lock to hold")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ran := false
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- testCfg.WithLock(ctx, func() error {
			ran = true
			return nil
		})
	}()

	select {
	case err := <-done:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("contended WithLock took %s, want fast failure", elapsed)
		}
		if !errors.Is(err, ErrLockBusy) {
			t.Fatalf("WithLock err = %v, want ErrLockBusy", err)
		}
		if ran {
			t.Error("fn ran despite the lock being held")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("contended WithLock blocked past its deadline")
	}

	// Release the held lock; a fresh acquire must now succeed.
	if err := holder.Unlock(); err != nil {
		t.Fatalf("release held lock: %v", err)
	}
	acquired := false
	if err := testCfg.WithLock(context.Background(), func() error {
		acquired = true
		return nil
	}); err != nil {
		t.Fatalf("WithLock after release: %v", err)
	}
	if !acquired {
		t.Error("fn did not run after the lock was released")
	}
}

func TestPathAndSockPath(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	dir, err := testCfg.Dir()
	if err != nil {
		t.Fatal(err)
	}
	path, err := testCfg.Path()
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, stateFile) {
		t.Errorf("Path = %q, want %q", path, filepath.Join(dir, stateFile))
	}
	sock, err := testCfg.SockPath()
	if err != nil {
		t.Fatal(err)
	}
	if sock != filepath.Join(dir, sockFile) {
		t.Errorf("SockPath = %q, want %q", sock, filepath.Join(dir, sockFile))
	}
}

func TestDirHonorsConfigName(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	base := os.Getenv("XDG_CONFIG_HOME")
	for _, name := range []string{"synckit", "reposync", "cookiesync"} {
		dir, err := Config{Name: name}.Dir()
		if err != nil {
			t.Fatalf("Dir(%q): %v", name, err)
		}
		if want := filepath.Join(base, name); dir != want {
			t.Fatalf("Config{%q}.Dir() = %q, want %q", name, dir, want)
		}
	}
}
