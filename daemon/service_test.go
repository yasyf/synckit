package daemon

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/service"
)

// fakeLauncher records every bootstrap and bootout it is asked to perform without
// touching launchd, so a test can assert the agent set and the legacy bootouts.
type fakeLauncher struct {
	bootstrapped []string // plist paths
	bootedOut    []string // labels
}

func (f *fakeLauncher) Bootstrap(_ context.Context, plistPath string) error {
	f.bootstrapped = append(f.bootstrapped, plistPath)
	return nil
}

func (f *fakeLauncher) Bootout(_ context.Context, label string) error {
	f.bootedOut = append(f.bootedOut, label)
	return nil
}

func TestToolConfigAgents(t *testing.T) {
	manifests := []manifest.Manifest{
		{
			Name: "cookiesync", Binary: "cookiesync",
			Watch:   manifest.WatchSpec{Backend: "fsnotify", Debounce: codec.Duration(0)},
			Service: manifest.ServiceSpec{Transport: "socket", ServeArgs: []string{"rpc-serve"}, Sock: "~/.config/cookiesync/rpc.sock"},
			Helper:  &manifest.HelperSpec{Command: "helper-serve", SessionType: "Aqua", Label: "helper"},
		},
		{
			Name: "reposync", Binary: "reposync",
			Watch:   manifest.WatchSpec{Backend: "watchman", Debounce: codec.Duration(0)},
			Service: manifest.ServiceSpec{Transport: "stdio", ServeArgs: []string{"rpc-serve"}},
			// no Helper block -> no helper agent
		},
	}

	cfg := toolConfig(manifests)
	if cfg.BinaryName != "synckitd" {
		t.Errorf("BinaryName = %q, want synckitd", cfg.BinaryName)
	}
	if cfg.LabelPrefix != "com.github.yasyf.synckit" {
		t.Errorf("LabelPrefix = %q, want com.github.yasyf.synckit", cfg.LabelPrefix)
	}

	byLabel := map[string]struct{}{}
	for _, a := range cfg.Agents {
		byLabel[a.Label] = struct{}{}
	}
	for _, want := range []string{"reconcile", "serve", "helper.cookiesync"} {
		if _, ok := byLabel[want]; !ok {
			t.Errorf("missing agent %q (have %v)", want, agentLabels(cfg.Agents))
		}
	}
	if _, ok := byLabel["helper.reposync"]; ok {
		t.Errorf("reposync has no Helper block but a helper.reposync agent was built")
	}

	reconcile := findAgent(t, cfg.Agents, "reconcile")
	if reconcile.Command != "reconcile" {
		t.Errorf("reconcile command = %q, want reconcile", reconcile.Command)
	}
	if reconcile.ExtraKeys["StartInterval"] != 900 {
		t.Errorf("reconcile StartInterval = %v, want 900", reconcile.ExtraKeys["StartInterval"])
	}

	serve := findAgent(t, cfg.Agents, "serve")
	if serve.ExtraKeys["KeepAlive"] != true {
		t.Errorf("serve KeepAlive = %v, want true", serve.ExtraKeys["KeepAlive"])
	}

	helper := findAgent(t, cfg.Agents, "helper.cookiesync")
	if helper.Command != "helper-serve" {
		t.Errorf("helper command = %q, want helper-serve", helper.Command)
	}
	if helper.Binary != "cookiesync" {
		t.Errorf("helper Binary = %q, want cookiesync (the consumer binary)", helper.Binary)
	}
	if helper.ExtraKeys["LimitLoadToSessionType"] != "Aqua" {
		t.Errorf("helper session type = %v, want Aqua", helper.ExtraKeys["LimitLoadToSessionType"])
	}
}

func TestInstallBuildsAgentsAndBootsOutLegacy(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("install requires macOS launchd")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfgDir := filepath.Join(home, ".config")
	t.Setenv("XDG_CONFIG_HOME", cfgDir)

	// Put a stub cookiesync binary on PATH so the helper agent's plist resolves it.
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "cookiesync"), []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // test stub must be executable
		t.Fatalf("write stub cookiesync: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	// Register a manifest with a Helper block under the manifests dir.
	dir, err := ensureManifestsDir()
	if err != nil {
		t.Fatalf("ensure manifests dir: %v", err)
	}
	mf := `{"name":"cookiesync","binary":"cookiesync","watch":{"backend":"fsnotify","debounce":"1s"},` +
		`"service":{"transport":"socket","serve_args":["rpc-serve"],"sock":"~/.config/cookiesync/rpc.sock"},` +
		`"helper":{"command":"helper-serve","session_type":"Aqua","label":"helper"}}`
	if err := os.WriteFile(filepath.Join(dir, "cookiesync.json"), []byte(mf), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	fake := &fakeLauncher{}
	if err := install(context.Background(), fake); err != nil {
		t.Fatalf("install: %v", err)
	}

	// Three agents installed: reconcile, serve, helper.cookiesync. Each is booted
	// out before bootstrap, so it appears in bootedOut too.
	if len(fake.bootstrapped) != 3 {
		t.Errorf("bootstrapped %d plists, want 3: %v", len(fake.bootstrapped), fake.bootstrapped)
	}
	wantBootout := map[string]bool{
		"com.github.yasyf.synckit.reconcile":         true,
		"com.github.yasyf.synckit.serve":             true,
		"com.github.yasyf.synckit.helper.cookiesync": true,
		"com.github.yasyf.reposync.reconcile":        true,
		"com.github.yasyf.reposync.watch":            true,
		"com.github.yasyf.cookiesync.reconcile":      true,
		"com.github.yasyf.cookiesync.watch":          true,
	}
	for want := range wantBootout {
		if !contains(fake.bootedOut, want) {
			t.Errorf("legacy/agent bootout missing %q (have %v)", want, fake.bootedOut)
		}
	}
}

func agentLabels(agents []service.AgentSpec) []string {
	out := make([]string, 0, len(agents))
	for _, a := range agents {
		out = append(out, a.Label)
	}
	sort.Strings(out)
	return out
}

func findAgent(t *testing.T, agents []service.AgentSpec, label string) service.AgentSpec {
	t.Helper()
	for _, a := range agents {
		if a.Label == label {
			return a
		}
	}
	t.Fatalf("agent %q not found in %v", label, agentLabels(agents))
	return service.AgentSpec{}
}

func contains(xs []string, x string) bool {
	for _, e := range xs {
		if e == x {
			return true
		}
	}
	return false
}
