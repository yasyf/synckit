package hostregistry

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/yasyf/daemonkit/proc"
)

// testCfg is the Config the in-package tests drive; Name selects the per-tool
// config subdir under the temp XDG_CONFIG_HOME each test sets, and Binary is the
// name the verify/install probes shell over ssh.
var testCfg = Config{Name: "synckit", Binary: "synckit", State: Mesh.State}

func initializeTestState(t *testing.T, cfg Config) {
	t.Helper()
	if err := cfg.InitializeState(context.Background()); err != nil {
		t.Fatalf("InitializeState: %v", err)
	}
}

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

// TestErrLockBusyAliasesProc pins that hostregistry.ErrLockBusy is the same sentinel
// as proc.ErrLockBusy, so a downstream errors.Is(err, hostregistry.ErrLockBusy) — which
// reposync re-aliases as state.ErrLockBusy — keeps holding across the daemonkit swap.
func TestErrLockBusyAliasesProc(t *testing.T) {
	if !errors.Is(ErrLockBusy, proc.ErrLockBusy) {
		t.Fatalf("hostregistry.ErrLockBusy (%v) is not proc.ErrLockBusy (%v)", ErrLockBusy, proc.ErrLockBusy)
	}
	if !errors.Is(ErrLockBusy, proc.ErrLockBusy) {
		t.Error("errors.Is(hostregistry.ErrLockBusy, proc.ErrLockBusy) must hold")
	}
}

func TestFlatStateIsRejectedWithoutRepair(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	path, err := testCfg.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	flat := []byte(`{"self":"yasyf@old","hosts":[],"addrs":{}}`)
	if err := os.WriteFile(path, flat, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := testCfg.Load(); !errors.Is(err, ErrStateSchema) {
		t.Fatalf("Load flat state = %v, want ErrStateSchema", err)
	}
	after, err := os.ReadFile(path) //nolint:gosec // temp state path from the fixed test Config
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, flat) {
		t.Fatal("rejected flat state was mutated")
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

func TestDirHonorsDirEnvOverride(t *testing.T) {
	const envName = "COOKIESYNC_CONFIG_DIR"
	override := t.TempDir()

	tests := []struct {
		name    string
		cfg     Config
		envVal  string
		wantDir func(base string) string
	}{
		{
			name:    "override wins over XDG and default, returned verbatim",
			cfg:     Config{Name: "cookiesync", DirEnv: envName},
			envVal:  override,
			wantDir: func(string) string { return override },
		},
		{
			name:    "DirEnv set but env empty falls through to XDG",
			cfg:     Config{Name: "cookiesync", DirEnv: envName},
			envVal:  "",
			wantDir: func(base string) string { return filepath.Join(base, "cookiesync") },
		},
		{
			name:    "zero-value DirEnv ignores the env, uses XDG unchanged",
			cfg:     Config{Name: "cookiesync"},
			envVal:  override, // set, but a zero-value Config must never read it
			wantDir: func(base string) string { return filepath.Join(base, "cookiesync") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			t.Setenv("XDG_CONFIG_HOME", base)
			t.Setenv(envName, tt.envVal)
			got, err := tt.cfg.Dir()
			if err != nil {
				t.Fatalf("Dir: %v", err)
			}
			if want := tt.wantDir(base); got != want {
				t.Fatalf("Dir() = %q, want %q", got, want)
			}
		})
	}
}
