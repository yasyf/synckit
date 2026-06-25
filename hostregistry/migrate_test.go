package hostregistry

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// seedLegacy writes a legacy per-tool state.json with the given self and hosts
// under the test's XDG_CONFIG_HOME.
func seedLegacy(t *testing.T, name, self string, hosts []string) {
	t.Helper()
	cfg := Config{Name: name}
	dir, err := cfg.Dir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{"self": self, "hosts": hosts}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateLegacyMesh(t *testing.T) {
	t.Run("seeds self and hosts from the first populated legacy", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())
		seedLegacy(t, "cookiesync", "yasyf@host", []string{"a@one", "b@two"})

		if err := MigrateLegacyMesh(context.Background(), "cookiesync", "reposync"); err != nil {
			t.Fatalf("MigrateLegacyMesh: %v", err)
		}

		g, err := Mesh.Load()
		if err != nil {
			t.Fatalf("Mesh.Load: %v", err)
		}
		if g.Self != "yasyf@host" {
			t.Fatalf("Self = %q, want yasyf@host", g.Self)
		}
		if !contains(g.Hosts, "a@one") || !contains(g.Hosts, "b@two") {
			t.Fatalf("Hosts = %v, want a@one and b@two", g.Hosts)
		}
	})

	t.Run("idempotent: second call is a no-op", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())
		seedLegacy(t, "cookiesync", "yasyf@host", []string{"a@one"})

		if err := MigrateLegacyMesh(context.Background(), "cookiesync"); err != nil {
			t.Fatalf("first MigrateLegacyMesh: %v", err)
		}
		// Mutate the legacy registry; a re-run must not pick the new value up.
		seedLegacy(t, "cookiesync", "yasyf@drift", []string{"c@drift"})
		if err := MigrateLegacyMesh(context.Background(), "cookiesync"); err != nil {
			t.Fatalf("second MigrateLegacyMesh: %v", err)
		}

		g, err := Mesh.Load()
		if err != nil {
			t.Fatalf("Mesh.Load: %v", err)
		}
		if g.Self != "yasyf@host" {
			t.Fatalf("Self = %q, want yasyf@host (idempotent)", g.Self)
		}
		if contains(g.Hosts, "c@drift") {
			t.Fatalf("Hosts = %v, must not absorb post-migration legacy drift", g.Hosts)
		}
	})

	t.Run("no-op when Mesh already populated", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())
		if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
			g.Self = "yasyf@existing"
			g.UpsertHost("x@existing")
			return nil
		}); err != nil {
			t.Fatalf("seed mesh: %v", err)
		}
		seedLegacy(t, "cookiesync", "yasyf@legacy", []string{"a@one"})

		if err := MigrateLegacyMesh(context.Background(), "cookiesync"); err != nil {
			t.Fatalf("MigrateLegacyMesh: %v", err)
		}

		g, err := Mesh.Load()
		if err != nil {
			t.Fatalf("Mesh.Load: %v", err)
		}
		if g.Self != "yasyf@existing" {
			t.Fatalf("Self = %q, want yasyf@existing (mesh must win)", g.Self)
		}
		if contains(g.Hosts, "a@one") {
			t.Fatalf("Hosts = %v, must not absorb legacy when mesh is populated", g.Hosts)
		}
	})

	t.Run("unions hosts across two legacy files", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())
		seedLegacy(t, "cookiesync", "yasyf@host", []string{"a@one", "shared@dup"})
		seedLegacy(t, "reposync", "yasyf@host", []string{"b@two", "shared@dup"})

		if err := MigrateLegacyMesh(context.Background(), "cookiesync", "reposync"); err != nil {
			t.Fatalf("MigrateLegacyMesh: %v", err)
		}

		g, err := Mesh.Load()
		if err != nil {
			t.Fatalf("Mesh.Load: %v", err)
		}
		for _, want := range []string{"a@one", "b@two", "shared@dup"} {
			if !contains(g.Hosts, want) {
				t.Fatalf("Hosts = %v, want to contain %q", g.Hosts, want)
			}
		}
		// shared@dup appears in both legacy files but must be deduped.
		count := 0
		for _, h := range g.Hosts {
			if h == "shared@dup" {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("shared@dup appears %d times, want 1 (union deduped)", count)
		}
	})

	t.Run("no-op when no legacy is populated", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())

		if err := MigrateLegacyMesh(context.Background(), "cookiesync", "reposync"); err != nil {
			t.Fatalf("MigrateLegacyMesh: %v", err)
		}

		g, err := Mesh.Load()
		if err != nil {
			t.Fatalf("Mesh.Load: %v", err)
		}
		if g.Self != "" {
			t.Fatalf("Self = %q, want empty when no legacy populated", g.Self)
		}
		if len(g.Hosts) != 0 {
			t.Fatalf("Hosts = %v, want empty when no legacy populated", g.Hosts)
		}
	})
}

// TestVerifyBinary pins the binary parameterization: VerifyBinary must probe the
// given binary rather than the Config's Name, so a mesh Config whose Name is not
// an installed tool can still verify an installed consumer binary.
func TestVerifyBinary(t *testing.T) {
	r := NewMockRunner().
		OnSSH("command -v cookiesync", "/opt/homebrew/bin/cookiesync\n", nil).
		OnSSH("cookiesync --version", "cookiesync 9.9.9\n", nil)

	res := Mesh.VerifyBinary(context.Background(), r, "yasyf@host", "cookiesync")
	if !res.Reachable || !res.Bootstrapped {
		t.Fatalf("VerifyBinary: %+v, want reachable+bootstrapped", res)
	}
	if res.Version != "cookiesync 9.9.9" {
		t.Fatalf("Version = %q, want cookiesync 9.9.9", res.Version)
	}
	cmds := r.SSHCmds("yasyf@host")
	want := []string{"command -v cookiesync", "cookiesync --version"}
	if len(cmds) != len(want) {
		t.Fatalf("ssh cmds = %v, want %v", cmds, want)
	}
	for i := range want {
		if cmds[i] != want[i] {
			t.Fatalf("ssh cmd[%d] = %q, want %q", i, cmds[i], want[i])
		}
	}
}
