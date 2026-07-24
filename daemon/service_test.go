package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func useServiceController(t *testing.T, controller serviceController) {
	t.Helper()
	previous := openServiceController
	openServiceController = func(context.Context) (serviceController, error) { return controller, nil }
	t.Cleanup(func() { openServiceController = previous })
}

func TestServiceAgentsUseFixedProgramsAndTypedPolicy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	binDir := t.TempDir()
	helperPath := filepath.Join(binDir, "cookiesync")
	if err := os.WriteFile(helperPath, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	helperPath, err := filepath.EvalSymlinks(helperPath)
	if err != nil {
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
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 3 {
		t.Fatalf("agents = %#v", agents)
	}
	reconcile := findAgent(t, agents, labelPrefix+".reconcile")
	if reconcile.RestartPolicy != dkservice.NoRestart || reconcile.StartInterval != reconcileInterval || reconcile.ProcessType != dkservice.ProcessTypeBackground {
		t.Fatalf("reconcile policy = %#v", reconcile)
	}
	serve := findAgent(t, agents, labelPrefix+".serve")
	if serve.RestartPolicy != dkservice.RestartAlways {
		t.Fatalf("serve policy = %#v", serve)
	}
	helper := findAgent(t, agents, labelPrefix+".helper.cookiesync")
	if helper.Program != helperPath || helper.RestartPolicy != dkservice.RestartAlways || helper.LimitLoadToSessionType != dkservice.SessionTypeAqua {
		t.Fatalf("helper policy = %#v", helper)
	}
}

func TestInstallAndUninstallConvergeExactDesiredSet(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
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
