package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	dkservice "github.com/yasyf/daemonkit/service"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/manifest"
)

type fakeServiceController struct {
	desired     [][]dkservice.Agent
	closed      int
	closeCtxErr error
	closeErr    error
}

func (f *fakeServiceController) Converge(_ context.Context, agents []dkservice.Agent) error {
	f.desired = append(f.desired, agents)
	return nil
}

func (f *fakeServiceController) Close(ctx context.Context) error {
	f.closed++
	f.closeCtxErr = ctx.Err()
	return f.closeErr
}

func useHelperStaging(t *testing.T, stage func(string, string) (string, error)) {
	t.Helper()
	previous := stageHelperProgram
	stageHelperProgram = stage
	t.Cleanup(func() { stageHelperProgram = previous })
}

func useServiceController(t *testing.T, controller serviceController) {
	t.Helper()
	previous := openServiceController
	openServiceController = func(context.Context) (serviceController, error) { return controller, nil }
	t.Cleanup(func() { openServiceController = previous })
}

func useHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DAEMONKIT_HOME", home)
	return home
}

func TestServiceAgentsUseFixedProgramsAndTypedPolicy(t *testing.T) {
	const build = "v1.2.3"
	home := useHome(t)
	binDir := t.TempDir()
	helperPath := filepath.Join(binDir, "cookiesync")
	if err := os.WriteFile(helperPath, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	agents, err := serviceAgents([]manifest.Manifest{{
		Name: "cookiesync", Binary: "cookiesync", Watch: manifest.WatchSpec{Debounce: codec.Duration(0)},
		Service: manifest.ServiceSpec{
			Kind: "resident", Socket: "~/.config/cookiesync/rpc.sock",
			SchemaFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Helper: &manifest.HelperSpec{Command: "helper-serve", SessionType: manifest.SessionTypeAqua},
	}}, build)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 3 {
		t.Fatalf("agents = %#v", agents)
	}
	daemonDir, err := filepath.EvalSymlinks(filepath.Join(home, ".daemonkit", "bin"))
	if err != nil {
		t.Fatal(err)
	}
	daemonPath := filepath.Join(daemonDir, daemonBinary)
	reconcile := findAgent(t, agents, labelPrefix+".reconcile")
	if reconcile.Program != daemonPath || reconcile.RestartPolicy != dkservice.NoRestart || reconcile.StartInterval != reconcileInterval || reconcile.ProcessType != dkservice.ProcessTypeBackground {
		t.Fatalf("reconcile policy = %#v", reconcile)
	}
	serve := findAgent(t, agents, labelPrefix+".serve")
	if serve.Program != daemonPath || serve.RestartPolicy != dkservice.RestartAlways || serve.ProcessType != 0 {
		t.Fatalf("serve policy = %#v", serve)
	}
	stagedHelper := filepath.Join(daemonDir, labelPrefix+".helper.cookiesync")
	helper := findAgent(t, agents, labelPrefix+".helper.cookiesync")
	if helper.Program != stagedHelper || helper.RestartPolicy != dkservice.RestartAlways || helper.ProcessType != 0 {
		t.Fatalf("helper policy = %#v", helper)
	}
	if _, err := os.Stat(helperPath); err != nil {
		t.Fatalf("helper source removed: %v", err)
	}
}

func TestServiceAgentsNeverSessionLimitOrThrottleHelpers(t *testing.T) {
	sessions := []manifest.SessionType{
		"",
		manifest.SessionTypeAqua,
		manifest.SessionTypeBackground,
		manifest.SessionTypeLoginWindow,
		manifest.SessionTypeStandardIO,
		manifest.SessionTypeSystem,
	}
	for _, session := range sessions {
		t.Run(string(session), func(t *testing.T) {
			useHome(t)
			binDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(binDir, "reposync"), []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
				t.Fatal(err)
			}
			t.Setenv("PATH", binDir)
			agents, err := serviceAgents([]manifest.Manifest{{
				Name: "reposync", Binary: "reposync", Watch: manifest.WatchSpec{Debounce: codec.Duration(0)},
				Service: manifest.ServiceSpec{
					Kind: "resident", Socket: "~/.config/reposync/rpc.sock",
					SchemaFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
				Helper: &manifest.HelperSpec{Command: "helper-serve", SessionType: session},
			}}, "v1.2.3")
			if err != nil {
				t.Fatal(err)
			}
			helper := findAgent(t, agents, labelPrefix+".helper.reposync")
			if helper.LimitLoadToSessionType != 0 || helper.ProcessType != 0 {
				t.Fatalf("helper policy = %#v", helper)
			}
			plist, err := helper.Plist()
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"LimitLoadToSessionType", "ProcessType"} {
				if strings.Contains(string(plist), key) {
					t.Fatalf("helper plist carries %s\n%s", key, plist)
				}
			}
		})
	}
}

func TestServiceAgentsStageOnlyUnbundledHelperPrograms(t *testing.T) {
	tests := []struct {
		name   string
		source func(t *testing.T, binDir string) string
		staged bool
	}{
		{
			name: "plain executable",
			source: func(t *testing.T, binDir string) string {
				target := filepath.Join(t.TempDir(), "reposync")
				if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(binDir, "reposync")); err != nil {
					t.Fatal(err)
				}
				return target
			},
			staged: true,
		},
		{
			name: "bundled executable",
			source: func(t *testing.T, binDir string) string {
				macos := filepath.Join(t.TempDir(), "Reposync.app", "Contents", "MacOS")
				if err := os.MkdirAll(macos, 0o750); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(macos, "reposync")
				if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(binDir, "reposync")); err != nil {
					t.Fatal(err)
				}
				return target
			},
			staged: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useHome(t)
			binDir := t.TempDir()
			source, err := filepath.EvalSymlinks(tt.source(t, binDir))
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", binDir)
			stagedPath := filepath.Join(t.TempDir(), "staged-reposync")
			var calls []string
			useHelperStaging(t, func(label, got string) (string, error) {
				if got != source {
					t.Fatalf("staged source = %q, want %q", got, source)
				}
				calls = append(calls, label)
				return stagedPath, nil
			})
			agents, err := serviceAgents([]manifest.Manifest{{
				Name: "reposync", Binary: "reposync", Watch: manifest.WatchSpec{Debounce: codec.Duration(0)},
				Service: manifest.ServiceSpec{
					Kind: "resident", Socket: "~/.config/reposync/rpc.sock",
					SchemaFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
				Helper: &manifest.HelperSpec{Command: "helper-serve"},
			}}, "v1.2.3")
			if err != nil {
				t.Fatal(err)
			}
			helper := findAgent(t, agents, labelPrefix+".helper.reposync")
			wantProgram, wantCalls := source, []string(nil)
			if tt.staged {
				wantProgram, wantCalls = stagedPath, []string{labelPrefix + ".helper.reposync"}
			}
			if helper.Program != wantProgram {
				t.Fatalf("helper program = %q, want %q", helper.Program, wantProgram)
			}
			if !slices.Equal(calls, wantCalls) {
				t.Fatalf("staging calls = %#v, want %#v", calls, wantCalls)
			}
		})
	}
}

func TestBundledExecutableMatchesInstalledHelperShapes(t *testing.T) {
	tests := []struct {
		program string
		want    bool
	}{
		{program: "/opt/homebrew/Caskroom/reposync/0.27.4/reposync", want: false},
		{program: "/opt/homebrew/Caskroom/cookiesync/0.27.2/CookieSync.app/Contents/MacOS/cookiesync", want: true},
		{program: "/opt/homebrew/bin/reposync", want: false},
		{program: "/Applications/CookieSync.app/Contents/MacOS/cookiesync", want: true},
		{program: "/opt/homebrew/Caskroom/x/1.0/App.app/Contents/Helpers/tool", want: false},
		{program: "/opt/homebrew/Caskroom/x/1.0/plain/Contents/MacOS/tool", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.program, func(t *testing.T) {
			if got := bundledExecutable(tt.program); got != tt.want {
				t.Fatalf("bundledExecutable(%q) = %t, want %t", tt.program, got, tt.want)
			}
		})
	}
}

func TestServiceAgentsFailWhenHelperStagingFails(t *testing.T) {
	useHome(t)
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "reposync"), []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	stageErr := errors.New("stage failed")
	useHelperStaging(t, func(string, string) (string, error) { return "", stageErr })
	_, err := serviceAgents([]manifest.Manifest{{
		Name: "reposync", Binary: "reposync", Watch: manifest.WatchSpec{Debounce: codec.Duration(0)},
		Service: manifest.ServiceSpec{
			Kind: "resident", Socket: "~/.config/reposync/rpc.sock",
			SchemaFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Helper: &manifest.HelperSpec{Command: "helper-serve"},
	}}, "v1.2.3")
	if !errors.Is(err, stageErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestInstallAndUninstallConvergeExactDesiredSet(t *testing.T) {
	useHome(t)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	binDir := t.TempDir()
	helperPath := filepath.Join(binDir, "cookiesync")
	if err := os.WriteFile(helperPath, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"name":"cookiesync","binary":"cookiesync","watch":{"debounce":"1s"},` +
		`"service":{"kind":"resident","socket":"/tmp/cookiesync.sock",` +
		`"schema_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},` +
		`"helper":{"command":"helper-serve","session_type":"Aqua"}}`
	if err := os.WriteFile(filepath.Join(directory, "cookiesync.json"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	controller := &fakeServiceController{}
	useServiceController(t, controller)
	if err := install(t.Context(), "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if len(controller.desired) != 1 || len(controller.desired[0]) != 3 {
		t.Fatalf("install desired = %#v", controller.desired)
	}
	if err := uninstall(t.Context(), "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if len(controller.desired) != 2 || controller.desired[1] != nil || controller.closed != 2 {
		t.Fatalf("controller = %#v", controller)
	}
}

func TestServiceControllerCloseJoinsOperationError(t *testing.T) {
	runErr := errors.New("converge failed")
	closeErr := errors.New("close failed")
	controller := &fakeServiceController{closeErr: closeErr}
	useServiceController(t, controller)
	err := withServiceController(t.Context(), func(serviceController) error { return runErr })
	if !errors.Is(err, runErr) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestServiceControllerPathsAreStableAndDistinct(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	config, err := serviceControllerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(config.StatePath) || !filepath.IsAbs(config.ProcessPath) || config.StatePath == config.ProcessPath || config.WorkerLimit != serviceWorkerLimit {
		t.Fatalf("controller config = %#v", config)
	}
}

func findAgent(t *testing.T, agents []dkservice.Agent, label string) dkservice.Agent {
	t.Helper()
	for _, agent := range agents {
		if agent.Label == label {
			return agent
		}
	}
	t.Fatalf("missing agent %q", label)
	return dkservice.Agent{}
}
