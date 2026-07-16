package authkit

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// stageHelper builds a fake Caskroom staging of authkit.app under a fresh
// HOMEBREW_PREFIX and returns the app bundle path.
func stageHelper(t *testing.T) string {
	t.Helper()
	prefix := t.TempDir()
	t.Setenv("HOMEBREW_PREFIX", prefix)
	t.Setenv(HelperEnvVar, "")
	app := filepath.Join(prefix, "Caskroom", "authkit", "0.1.0", "authkit.app")
	binDir := filepath.Join(app, "Contents", "MacOS")
	if err := os.MkdirAll(binDir, 0o755); err != nil { //nolint:gosec // test bundle must be traversable
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "authkit"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil { //nolint:gosec // test stub must be executable
		t.Fatalf("write helper: %v", err)
	}
	return app
}

func TestHelperDiscoveryViaCaskroomGlob(t *testing.T) {
	app := stageHelper(t)

	got, err := HelperAppPath()
	if err != nil {
		t.Fatalf("HelperAppPath: %v", err)
	}
	if got != app {
		t.Fatalf("HelperAppPath = %q, want %q", got, app)
	}
	bin, err := RequireHelper()
	if err != nil {
		t.Fatalf("RequireHelper: %v", err)
	}
	if want := filepath.Join(app, "Contents", "MacOS", "authkit"); bin != want {
		t.Fatalf("RequireHelper = %q, want %q", bin, want)
	}
}

func TestHelperEnvVarOverridesDiscovery(t *testing.T) {
	stageHelper(t)
	override := filepath.Join(t.TempDir(), "authkit")
	if err := os.WriteFile(override, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil { //nolint:gosec // test stub must be executable
		t.Fatalf("write override: %v", err)
	}
	t.Setenv(HelperEnvVar, override)

	bin, err := RequireHelper()
	if err != nil {
		t.Fatalf("RequireHelper: %v", err)
	}
	if bin != override {
		t.Fatalf("RequireHelper = %q, want the %s override %q", bin, HelperEnvVar, override)
	}
}

func TestRequireHelperFailsClosedWhenMissing(t *testing.T) {
	t.Setenv("HOMEBREW_PREFIX", t.TempDir())
	t.Setenv(HelperEnvVar, "")

	_, err := RequireHelper()
	var helperErr *HelperError
	if !errors.As(err, &helperErr) {
		t.Fatalf("RequireHelper with no staged bundle = %v, want *HelperError", err)
	}
}

func TestRequireHelperFailsClosedOnBrokenOverride(t *testing.T) {
	t.Setenv(HelperEnvVar, filepath.Join(t.TempDir(), "missing-authkit"))

	_, err := RequireHelper()
	var helperErr *HelperError
	if !errors.As(err, &helperErr) {
		t.Fatalf("RequireHelper on a dangling override = %v, want *HelperError", err)
	}
}

func TestHelperAppPathSkipsBundleWithoutExecutable(t *testing.T) {
	prefix := t.TempDir()
	t.Setenv("HOMEBREW_PREFIX", prefix)
	t.Setenv(HelperEnvVar, "")
	// A staged bundle missing its inner binary must not resolve.
	hollow := filepath.Join(prefix, "Caskroom", "authkit", "0.1.0", "authkit.app", "Contents", "MacOS")
	if err := os.MkdirAll(hollow, 0o755); err != nil { //nolint:gosec // test bundle must be traversable
		t.Fatalf("mkdir: %v", err)
	}

	_, err := HelperAppPath()
	var helperErr *HelperError
	if !errors.As(err, &helperErr) {
		t.Fatalf("HelperAppPath on a hollow bundle = %v, want *HelperError", err)
	}
}
