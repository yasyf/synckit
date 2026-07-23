package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	dkservice "github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
)

type fakeServiceController struct {
	desired     [][]dkservice.Agent
	closed      int
	closeCtxErr error
	runErr      error
	closeErr    error
	status      dkservice.Status
	stopSpecs   []dkservice.StopControlSpec
	events      []string
	stopErr     error
}

func (f *fakeServiceController) Converge(_ context.Context, agents []dkservice.Agent) error {
	f.events = append(f.events, "converge")
	f.desired = append(f.desired, append([]dkservice.Agent(nil), agents...))
	return f.runErr
}

func (f *fakeServiceController) Status(_ context.Context, label string) (dkservice.Status, error) {
	status := f.status
	status.Label = label
	return status, nil
}

func (f *fakeServiceController) StopRuntime(_ context.Context, spec dkservice.StopControlSpec) (wire.StopResult, error) {
	f.events = append(f.events, "stop")
	f.stopSpecs = append(f.stopSpecs, spec)
	return wire.StopResult{Stopped: f.stopErr == nil, ProcessGeneration: spec.TargetProcessGeneration}, f.stopErr
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

func useRuntimeHealth(t *testing.T, health rpc.RuntimeHealth) {
	t.Helper()
	previous := observeRuntimeHealth
	observeRuntimeHealth = func(context.Context, string) (rpc.RuntimeHealth, error) { return health, nil }
	t.Cleanup(func() { observeRuntimeHealth = previous })
}

func useStopExecutable(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, daemonBinary)
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write stop executable: %v", err)
	}
	t.Setenv("PATH", directory)
	return executable
}

func TestServiceAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	binDir := t.TempDir()
	helperDir := t.TempDir()
	helperPath := filepath.Join(helperDir, "cookiesync")
	if err := os.WriteFile(helperPath, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write helper: %v", err)
	}
	if err := os.Symlink(helperPath, filepath.Join(binDir, "cookiesync")); err != nil {
		t.Fatalf("link helper: %v", err)
	}
	helperPath, err := filepath.EvalSymlinks(helperPath)
	if err != nil {
		t.Fatalf("canonicalize helper: %v", err)
	}
	t.Setenv("PATH", binDir)
	manifests := []manifest.Manifest{
		{
			Name: "cookiesync", Binary: "cookiesync",
			Watch:   manifest.WatchSpec{Debounce: codec.Duration(0)},
			Service: manifest.ServiceSpec{Transport: "socket", ServeArgs: []string{"rpc-serve"}, Sock: "~/.config/cookiesync/rpc.sock"},
			Helper:  &manifest.HelperSpec{Command: "helper-serve", SessionType: manifest.SessionTypeAqua},
		},
		{
			Name: "reposync", Binary: "reposync",
			Watch:   manifest.WatchSpec{Debounce: codec.Duration(0)},
			Service: manifest.ServiceSpec{Transport: "stdio", ServeArgs: []string{"rpc-serve"}},
		},
	}

	agents, err := serviceAgents(manifests)
	if err != nil {
		t.Fatalf("serviceAgents: %v", err)
	}
	if got, want := agentLabels(agents), []string{
		"com.github.yasyf.synckit.helper.cookiesync",
		"com.github.yasyf.synckit.reconcile",
		"com.github.yasyf.synckit.serve",
	}; !equalStrings(got, want) {
		t.Fatalf("labels = %v, want %v", got, want)
	}

	reconcile := findAgent(t, agents, labelPrefix+".reconcile")
	executable, err := dkservice.CanonicalExecutable()
	if err != nil {
		t.Fatalf("CanonicalExecutable: %v", err)
	}
	if reconcile.Program != executable || !equalStrings(reconcile.Args, []string{"reconcile"}) {
		t.Errorf("reconcile command = %q %v", reconcile.Program, reconcile.Args)
	}
	if reconcile.RestartPolicy != dkservice.NoRestart || reconcile.StartInterval != reconcileInterval {
		t.Errorf("reconcile restart/interval = %v/%v", reconcile.RestartPolicy, reconcile.StartInterval)
	}
	if reconcile.ProcessType != dkservice.ProcessTypeBackground {
		t.Errorf("reconcile ProcessType = %v, want Background", reconcile.ProcessType)
	}

	serve := findAgent(t, agents, labelPrefix+".serve")
	if serve.RestartPolicy != dkservice.RestartAlways || serve.ProcessType != 0 {
		t.Errorf("serve restart/process type = %v/%v", serve.RestartPolicy, serve.ProcessType)
	}

	helper := findAgent(t, agents, labelPrefix+".helper.cookiesync")
	if helper.Program != helperPath || !equalStrings(helper.Args, []string{"helper-serve"}) {
		t.Errorf("helper command = %q %v", helper.Program, helper.Args)
	}
	if helper.RestartPolicy != dkservice.RestartAlways || helper.LimitLoadToSessionType != dkservice.SessionTypeAqua {
		t.Errorf("helper restart/session = %v/%v", helper.RestartPolicy, helper.LimitLoadToSessionType)
	}
	for _, agent := range agents {
		wantLog := filepath.Join(home, "Library", "Logs", "synckit", agent.Label+".log")
		if agent.LogPath != wantLog {
			t.Errorf("%s log = %q, want %q", agent.Label, agent.LogPath, wantLog)
		}
		if agent.Env["PATH"] != daemonPATH {
			t.Errorf("%s PATH = %q", agent.Label, agent.Env["PATH"])
		}
	}
}

func TestServiceAgentsRejectUnknownSession(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	binDir := t.TempDir()
	path := filepath.Join(binDir, "consumer")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write helper: %v", err)
	}
	t.Setenv("PATH", binDir)
	_, err := serviceAgents([]manifest.Manifest{{
		Name: "consumer", Binary: "consumer", Helper: &manifest.HelperSpec{Command: "helper", SessionType: manifest.SessionType("Console")},
	}})
	if err == nil {
		t.Fatal("serviceAgents accepted unknown session type")
	}
}

func TestExecutableAliasPreservesStableSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "synckitd")
	if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write daemon: %v", err)
	}
	alias := filepath.Join(dir, daemonBinary)
	if err := os.Symlink(target, alias); err != nil {
		t.Fatalf("link daemon: %v", err)
	}
	t.Setenv("PATH", dir)
	got, err := executableAlias(daemonBinary)
	if err != nil {
		t.Fatalf("executableAlias: %v", err)
	}
	if got != alias {
		t.Fatalf("role alias = %q, want %q", got, alias)
	}
}

func TestInstallConvergesExactDesiredSet(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	binDir := t.TempDir()
	helperPath := filepath.Join(binDir, "cookiesync")
	if err := os.WriteFile(helperPath, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write helper: %v", err)
	}
	t.Setenv("PATH", binDir)
	dir, err := ensureManifestsDir()
	if err != nil {
		t.Fatalf("ensure manifests dir: %v", err)
	}
	payload := `{"name":"cookiesync","binary":"cookiesync","watch":{"debounce":"1s"},` +
		`"service":{"transport":"socket","serve_args":["rpc-serve"],"sock":"/tmp/cookiesync.sock"},` +
		`"helper":{"command":"helper-serve","session_type":"Aqua"}}`
	if err := os.WriteFile(filepath.Join(dir, "cookiesync.json"), []byte(payload), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	controller := &fakeServiceController{}
	useServiceController(t, controller)
	if err := install(context.Background(), "v1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}
	if controller.closed != 1 {
		t.Fatalf("close calls = %d, want 1", controller.closed)
	}
	if len(controller.desired) != 1 {
		t.Fatalf("convergence calls = %d, want 1", len(controller.desired))
	}
	executable, err := dkservice.CanonicalExecutable()
	if err != nil {
		t.Fatalf("CanonicalExecutable: %v", err)
	}
	for _, agent := range controller.desired[0] {
		if agent.Label == labelPrefix+".reconcile" || agent.Label == labelPrefix+".serve" {
			if agent.Program != executable {
				t.Fatalf("%s program = %q, want canonical %q", agent.Label, agent.Program, executable)
			}
		}
	}
	if got, want := agentLabels(controller.desired[0]), []string{
		labelPrefix + ".helper.cookiesync", labelPrefix + ".reconcile", labelPrefix + ".serve",
	}; !equalStrings(got, want) {
		t.Fatalf("desired labels = %v, want %v", got, want)
	}
}

func TestInstallStopsAndSettlesPriorRuntimeBeforeConverge(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	stopExecutable := useStopExecutable(t)
	controller := &fakeServiceController{status: dkservice.Status{Loaded: true}}
	useServiceController(t, controller)
	useRuntimeHealth(t, rpc.RuntimeHealth{
		RuntimeBuild: "v1.2.2", RuntimeProtocol: int(rpc.Version),
		ProcessGeneration: "old-generation", PID: os.Getpid(), State: "healthy", Ready: true,
	})
	if err := install(context.Background(), "v1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !slices.Equal(controller.events, []string{"stop", "converge"}) {
		t.Fatalf("events = %v, want stop then converge", controller.events)
	}
	if len(controller.stopSpecs) != 1 {
		t.Fatalf("stop specs = %d, want 1", len(controller.stopSpecs))
	}
	spec := controller.stopSpecs[0]
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		t.Fatal(err)
	}
	if spec.Executable != stopExecutable || spec.Role != labelPrefix+".stop" || spec.RuntimeBuild != "v1.2.3" ||
		spec.RuntimeProtocol != int(rpc.Version) || spec.TargetProcessGeneration != "old-generation" ||
		spec.Intent != wire.StopIntentUpgrade || !slices.Equal(spec.Args, []string{"stop-control", sock}) {
		t.Fatalf("stop spec = %+v", spec)
	}
}

func TestInstallDoesNotConvergeWhenPriorRuntimeCannotSettle(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	useStopExecutable(t)
	stopErr := errors.New("target did not settle")
	controller := &fakeServiceController{status: dkservice.Status{Loaded: true}, stopErr: stopErr}
	useServiceController(t, controller)
	useRuntimeHealth(t, rpc.RuntimeHealth{
		RuntimeBuild: "v1.2.2", RuntimeProtocol: int(rpc.Version),
		ProcessGeneration: "old-generation", PID: os.Getpid(), State: "healthy", Ready: true,
	})
	err := install(context.Background(), "v1.2.3")
	if !errors.Is(err, stopErr) || !slices.Equal(controller.events, []string{"stop"}) {
		t.Fatalf("install = %v events=%v, want stop failure before converge", err, controller.events)
	}
}

func TestInstallRestartsSameBuildBeforeConverge(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	useStopExecutable(t)
	controller := &fakeServiceController{status: dkservice.Status{Loaded: true}}
	useServiceController(t, controller)
	useRuntimeHealth(t, rpc.RuntimeHealth{
		RuntimeBuild: "v1.2.3", RuntimeProtocol: int(rpc.Version),
		ProcessGeneration: "current-generation", PID: os.Getpid(), State: "healthy", Ready: true,
	})
	if err := install(context.Background(), "v1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !slices.Equal(controller.events, []string{"stop", "converge"}) ||
		len(controller.stopSpecs) != 1 || controller.stopSpecs[0].Intent != wire.StopIntentRestart {
		t.Fatalf("events=%v stop=%+v, want restart before converge", controller.events, controller.stopSpecs)
	}
}

func TestUninstallConvergesStoredSetToEmpty(t *testing.T) {
	controller := &fakeServiceController{}
	useServiceController(t, controller)
	if err := uninstall(context.Background(), "v1.2.3"); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if controller.closed != 1 || len(controller.desired) != 1 || controller.desired[0] != nil {
		t.Fatalf("controller state: closed=%d desired=%v", controller.closed, controller.desired)
	}
}

func TestUninstallStopsAndSettlesRuntimeBeforeRemovingServices(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	useStopExecutable(t)
	controller := &fakeServiceController{status: dkservice.Status{Loaded: true}}
	useServiceController(t, controller)
	useRuntimeHealth(t, rpc.RuntimeHealth{
		RuntimeBuild: "v1.2.3", RuntimeProtocol: int(rpc.Version),
		ProcessGeneration: "current-generation", PID: os.Getpid(), State: "healthy", Ready: true,
	})
	if err := uninstall(context.Background(), "v1.2.3"); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if !slices.Equal(controller.events, []string{"stop", "converge"}) || len(controller.stopSpecs) != 1 ||
		controller.stopSpecs[0].Intent != wire.StopIntentUninstall || controller.desired[0] != nil {
		t.Fatalf("events=%v stop=%+v desired=%v", controller.events, controller.stopSpecs, controller.desired)
	}
}

func TestServiceControllerCloseErrorJoinsOperationError(t *testing.T) {
	runErr := errors.New("converge failed")
	closeErr := errors.New("close failed")
	controller := &fakeServiceController{closeErr: closeErr}
	useServiceController(t, controller)
	err := withServiceController(context.Background(), func(serviceController) error { return runErr })
	if !errors.Is(err, runErr) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v, want both operation and close errors", err)
	}
}

func TestServiceControllerCloseOutlivesCallerCancellation(t *testing.T) {
	controller := &fakeServiceController{}
	useServiceController(t, controller)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := withServiceController(ctx, func(serviceController) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if controller.closed != 1 || controller.closeCtxErr != nil {
		t.Fatalf("close state: calls=%d context error=%v", controller.closed, controller.closeCtxErr)
	}
}

func TestServiceControllerPathsAreStableAndDistinct(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	config, err := serviceControllerConfig()
	if err != nil {
		t.Fatalf("serviceControllerConfig: %v", err)
	}
	if !filepath.IsAbs(config.StatePath) || !filepath.IsAbs(config.ProcessPath) || config.StatePath == config.ProcessPath {
		t.Fatalf("controller paths = %q / %q", config.StatePath, config.ProcessPath)
	}
	if config.WorkerLimit != serviceWorkerLimit {
		t.Fatalf("worker limit = %d, want %d", config.WorkerLimit, serviceWorkerLimit)
	}
}

func agentLabels(agents []dkservice.Agent) []string {
	out := make([]string, 0, len(agents))
	for _, agent := range agents {
		out = append(out, agent.Label)
	}
	sort.Strings(out)
	return out
}

func findAgent(t *testing.T, agents []dkservice.Agent, label string) dkservice.Agent {
	t.Helper()
	for _, agent := range agents {
		if agent.Label == label {
			return agent
		}
	}
	t.Fatalf("agent %q not found in %v", label, agentLabels(agents))
	return dkservice.Agent{}
}

func equalStrings(a, b []string) bool {
	return slices.Equal(a, b)
}
