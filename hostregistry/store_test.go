package hostregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

// testCfg is the Config the in-package tests drive; Name selects the per-tool
// config subdir under the temp XDG_CONFIG_HOME each test sets, and Binary is the
// name the verify/install probes shell over ssh.
var testCfg = Config{Name: "synckit", Binary: "synckit"}

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

// TestUpdateRawPreservesForeignKeysBothDirections is the sharpest invariant of
// the shared writer: a write that touches one slice of state.json leaves every
// other key byte-for-byte intact. It proves both directions — an identity write
// preserves repos/settings/default_location, and a domain write preserves
// self/hosts. The seed is written through UpdateRaw itself so the on-disk
// formatting is already canonical, making an exact-bytes comparison meaningful.
func TestUpdateRawPreservesForeignKeysBothDirections(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	seed := map[string]json.RawMessage{
		"self":             json.RawMessage(`"yasyf@old"`),
		"hosts":            json.RawMessage(`["yasyf@a"]`),
		"default_location": json.RawMessage(`"~/Code"`),
		"repos":            json.RawMessage(`[{"relpath":"cc-review","origin":"https://github.com/yasyf/cc-review.git","trunk":"main","local_only":false}]`),
		"settings":         json.RawMessage(`{"interval":"15m0s","idle_threshold":"5m0s"}`),
	}
	if err := testCfg.UpdateRaw(context.Background(), func(raw map[string]json.RawMessage) error {
		for k, v := range seed {
			raw[k] = v
		}
		return nil
	}); err != nil {
		t.Fatalf("seed UpdateRaw: %v", err)
	}

	identityKeys := []string{"self", "hosts"}
	domainKeys := []string{"repos", "settings", "default_location"}

	// Direction 1: an identity write mutates only self/hosts; the domain keys
	// must be byte-for-byte unchanged.
	before := readState(t)
	if err := testCfg.UpdateRaw(context.Background(), func(raw map[string]json.RawMessage) error {
		raw["self"] = json.RawMessage(`"yasyf@new"`)
		raw["hosts"] = json.RawMessage(`["yasyf@a","yasyf@b"]`)
		return nil
	}); err != nil {
		t.Fatalf("identity UpdateRaw: %v", err)
	}
	after := readState(t)
	assertKeysByteEqual(t, "identity write", before, after, domainKeys)
	assertKeysChanged(t, "identity write", before, after, identityKeys)

	// Direction 2: a domain write mutates only repos/settings/default_location;
	// the identity keys (now yasyf@new) must be byte-for-byte unchanged.
	before = readState(t)
	if err := testCfg.UpdateRaw(context.Background(), func(raw map[string]json.RawMessage) error {
		raw["repos"] = json.RawMessage(`[{"relpath":"notes","origin":"","trunk":"","local_only":true}]`)
		raw["settings"] = json.RawMessage(`{"interval":"30m0s","idle_threshold":"5m0s"}`)
		raw["default_location"] = json.RawMessage(`"~/Work"`)
		return nil
	}); err != nil {
		t.Fatalf("domain UpdateRaw: %v", err)
	}
	after = readState(t)
	assertKeysByteEqual(t, "domain write", before, after, identityKeys)
	assertKeysChanged(t, "domain write", before, after, domainKeys)
}

func readState(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	path, err := testCfg.Path()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // G304: test reads a file from a test-controlled temp dir.
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse state: %v", err)
	}
	return raw
}

func assertKeysByteEqual(t *testing.T, label string, before, after map[string]json.RawMessage, keys []string) {
	t.Helper()
	for _, key := range keys {
		if !bytes.Equal(before[key], after[key]) {
			t.Fatalf("%s: %s changed (not byte-for-byte preserved):\n before: %s\n  after: %s", label, key, before[key], after[key])
		}
	}
}

func assertKeysChanged(t *testing.T, label string, before, after map[string]json.RawMessage, keys []string) {
	t.Helper()
	for _, key := range keys {
		if bytes.Equal(before[key], after[key]) {
			t.Fatalf("%s: %s did not change, want the write to have updated it: %s", label, key, after[key])
		}
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
