package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/launchd"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/manifest"
)

// fakeLaunchd records every launchd mutation install and uninstall drive, in
// the order they were driven, so the sequence itself can be asserted.
type fakeLaunchd struct {
	events    []string
	applied   []string
	removed   []string
	removeErr map[string]error
}

func (f *fakeLaunchd) apply(_ context.Context, agent launchd.Agent) error {
	f.events = append(f.events, "apply:"+agent.Label)
	f.applied = append(f.applied, agent.Label)
	return nil
}

func (f *fakeLaunchd) remove(_ context.Context, label string) error {
	if err := f.removeErr[label]; err != nil {
		return err
	}
	f.events = append(f.events, "remove:"+label)
	f.removed = append(f.removed, label)
	return nil
}

func (f *fakeLaunchd) ensure(context.Context) error {
	f.events = append(f.events, "ensure:"+serveAgentLabel)
	return nil
}

func (f *fakeLaunchd) settle(context.Context) error {
	f.events = append(f.events, "settle:"+serveAgentLabel)
	return nil
}

func (f *fakeLaunchd) reset() {
	f.events, f.applied, f.removed = nil, nil, nil
}

func useLaunchd(t *testing.T) *fakeLaunchd {
	t.Helper()
	fake := &fakeLaunchd{removeErr: map[string]error{}}
	apply, remove := applyAgent, removeMarkedAgent
	ensure, settle := ensureServeAgent, settleServeAgent
	applyAgent, removeMarkedAgent = fake.apply, fake.remove
	ensureServeAgent, settleServeAgent = fake.ensure, fake.settle
	t.Cleanup(func() {
		applyAgent, removeMarkedAgent = apply, remove
		ensureServeAgent, settleServeAgent = ensure, settle
	})
	return fake
}

func useStaging(t *testing.T, stage func(label, source string) (string, error)) {
	t.Helper()
	previous := stageProgram
	stageProgram = stage
	t.Cleanup(func() { stageProgram = previous })
}

// useResidentStaging publishes each staged program as a real executable under a
// symlink-resolved directory: launchd.NewPlan validates every program against
// the live filesystem and refuses a symlinked component, which t.TempDir's
// /var/folders root is. It returns the source each label was staged from.
func useResidentStaging(t *testing.T) map[string]string {
	t.Helper()
	dir := resolvedTempDir(t)
	sources := map[string]string{}
	useStaging(t, func(label, source string) (string, error) {
		path := filepath.Join(dir, label)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
			return "", err
		}
		sources[label] = source
		return path, nil
	})
	return sources
}

func useHome(t *testing.T) string {
	t.Helper()
	home := resolvedTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("DAEMONKIT_HOME", home)
	return home
}

func useMesh(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(resolvedTempDir(t), "config")
	t.Setenv("XDG_CONFIG_HOME", dir)
	return dir
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func usePathBinaries(t *testing.T, names ...string) string {
	t.Helper()
	dir := resolvedTempDir(t)
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // executable test stub
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	return dir
}

func writeManifest(t *testing.T, name string) {
	t.Helper()
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name+".json"), []byte(manifestPayload(name)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// manifestPayload is the exact manifest JSON a consumer named name registers.
func manifestPayload(name string) string {
	return fmt.Sprintf(`{"name":%q,"binary":%q,"watch":{"debounce":"1s"},`+
		`"service":{"kind":"resident",`+
		`"schema_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},`+
		`"helper":{"command":"helper-serve","session_type":"Aqua"}}`, name, name)
}

func removeManifest(t *testing.T, name string) {
	t.Helper()
	directory, err := manifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, name+".json")); err != nil {
		t.Fatal(err)
	}
}

// corruptManifest replaces name's manifest with a file that will not decode,
// which Discover skips rather than failing on: the consumer stays registered
// but stale.
func corruptManifest(t *testing.T, name string) {
	t.Helper()
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name+".json"), []byte(`{"name":`), 0o600); err != nil {
		t.Fatal(err)
	}
}

// duplicateManifest registers a second file claiming name's service, which
// Discover refuses outright: two files naming one consumer is ambiguous at the
// directory level, unlike a single unloadable file, which it skips.
func duplicateManifest(t *testing.T, name string) {
	t.Helper()
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name+"-copy.json"), []byte(manifestPayload(name)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func helperManifest(name string, session manifest.SessionType) manifest.Manifest {
	return manifest.Manifest{
		Name: name, Binary: name, Watch: manifest.WatchSpec{Debounce: codec.Duration(0)},
		Service: manifest.ServiceSpec{
			Kind:              "resident",
			SchemaFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Helper: &manifest.HelperSpec{Command: "helper-serve", SessionType: session},
	}
}

// TestServeLabelIsTheOneEnsureAdopts pins the label against its literal v0.20
// spelling rather than against the constants that compose it: Ensure converges
// exactly the label it is handed, so a renamed prefix or suffix would leave the
// installed agent loaded and unowned instead of adopting it.
func TestServeLabelIsTheOneEnsureAdopts(t *testing.T) {
	const installed = "com.github.yasyf.synckit.serve"
	if serveAgentLabel != installed {
		t.Fatalf("serve label = %q, want %q", serveAgentLabel, installed)
	}
	if string(clientSpec().Label) != installed {
		t.Fatalf("serve spec label = %q, want %q", clientSpec().Label, installed)
	}
}

func TestServiceAgentsUseStagedProgramsAndTypedPolicy(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	sources := useResidentStaging(t)

	agents, err := serviceAgents([]manifest.Manifest{helperManifest("cookiesync", manifest.SessionTypeAqua)})
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 2 {
		t.Fatalf("agents = %#v", agents)
	}
	self, err := filepath.EvalSymlinks(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	reconcile := findAgent(t, agents, reconcileAgentLabel)
	if reconcile.RestartPolicy != launchd.NoRestart || reconcile.StartInterval != reconcileInterval ||
		reconcile.ProcessType != launchd.ProcessTypeBackground {
		t.Fatalf("reconcile policy = %#v", reconcile)
	}
	if !slices.Equal(reconcile.Args, []string{"reconcile"}) || sources[reconcileAgentLabel] != self {
		t.Fatalf("reconcile staged from %q, want %q", sources[reconcileAgentLabel], self)
	}
	helper := findAgent(t, agents, labelPrefix+".helper.cookiesync")
	if helper.RestartPolicy != launchd.RestartAlways || helper.ProcessType != 0 || helper.StartInterval != 0 {
		t.Fatalf("helper policy = %#v", helper)
	}
	if !slices.Equal(helper.Args, []string{"helper-serve"}) {
		t.Fatalf("helper args = %#v", helper.Args)
	}
	for _, agent := range agents {
		if agent.Env["PATH"] != daemonPATH {
			t.Fatalf("agent %q PATH = %q", agent.Label, agent.Env["PATH"])
		}
		if !filepath.IsAbs(agent.LogPath) || filepath.Base(agent.LogPath) != agent.Label+".log" {
			t.Fatalf("agent %q log = %q", agent.Label, agent.LogPath)
		}
	}
}

func TestServiceAgentsOmitTheDaemonkitOwnedServeLabel(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)

	agents, err := serviceAgents([]manifest.Manifest{helperManifest("cookiesync", "")})
	if err != nil {
		t.Fatal(err)
	}
	for _, agent := range agents {
		if agent.Label == serveAgentLabel {
			t.Fatalf("serviceAgents built the daemonkit-owned serve agent: %#v", agent)
		}
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
			useMesh(t)
			usePathBinaries(t, "reposync")
			useResidentStaging(t)

			agents, err := serviceAgents([]manifest.Manifest{helperManifest("reposync", session)})
			if err != nil {
				t.Fatal(err)
			}
			helper := findAgent(t, agents, labelPrefix+".helper.reposync")
			if helper.ProcessType != 0 {
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
				target := filepath.Join(resolvedTempDir(t), "reposync")
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
				macos := filepath.Join(resolvedTempDir(t), "Reposync.app", "Contents", "MacOS")
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
			useMesh(t)
			binDir := usePathBinaries(t)
			source, err := filepath.EvalSymlinks(tt.source(t, binDir))
			if err != nil {
				t.Fatal(err)
			}
			stagedPath := filepath.Join(resolvedTempDir(t), "staged-reposync")
			var calls []string
			useStaging(t, func(label, got string) (string, error) {
				if label == reconcileAgentLabel {
					return stagedPath, nil
				}
				if got != source {
					t.Fatalf("staged source = %q, want %q", got, source)
				}
				calls = append(calls, label)
				return stagedPath, nil
			})
			agents, err := serviceAgents([]manifest.Manifest{helperManifest("reposync", "")})
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
	useMesh(t)
	usePathBinaries(t, "reposync")
	stageErr := errors.New("stage failed")
	staged := filepath.Join(resolvedTempDir(t), "synckitd")
	useStaging(t, func(label, _ string) (string, error) {
		if label == reconcileAgentLabel {
			return staged, nil
		}
		return "", stageErr
	})
	if _, err := serviceAgents([]manifest.Manifest{helperManifest("reposync", "")}); !errors.Is(err, stageErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestStageProgramPublishesUnderTheMeshAndReusesIdenticalBytes(t *testing.T) {
	useHome(t)
	mesh := useMesh(t)
	source := filepath.Join(resolvedTempDir(t), "reposync")
	if err := os.WriteFile(source, []byte("#!/bin/sh\necho one\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	label := labelPrefix + ".helper.reposync"

	program, err := stageProgram(label, source)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := filepath.EvalSymlinks(filepath.Join(mesh, "synckit", stagedProgramDirName))
	if err != nil {
		t.Fatal(err)
	}
	if program != filepath.Join(dir, label) {
		t.Fatalf("staged program = %q, want %q", program, filepath.Join(dir, label))
	}
	info, err := os.Lstat(program)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("staged program mode = %v", info.Mode())
	}

	first := info.ModTime()
	if _, err := stageProgram(label, source); err != nil {
		t.Fatal(err)
	}
	restaged, err := os.Lstat(program)
	if err != nil {
		t.Fatal(err)
	}
	if !restaged.ModTime().Equal(first) {
		t.Fatal("identical bytes restaged the program launchd already execs")
	}

	if err := os.WriteFile(source, []byte("#!/bin/sh\necho two\n"), 0o755); err != nil { //nolint:gosec // executable test stub
		t.Fatal(err)
	}
	if _, err := stageProgram(label, source); err != nil {
		t.Fatal(err)
	}
	staged, err := os.ReadFile(program) //nolint:gosec // exact staged program path
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != "#!/bin/sh\necho two\n" {
		t.Fatalf("staged bytes = %q", staged)
	}
}

func TestInstallEnsuresServeBeforeApplyingEveryPlannedAgent(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")

	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	helper := labelPrefix + ".helper.cookiesync"
	want := []string{"ensure:" + serveAgentLabel, "apply:" + helper, "apply:" + reconcileAgentLabel}
	if !slices.Equal(fake.events, want) {
		t.Fatalf("events = %#v, want %#v", fake.events, want)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(recorded, []string{helper, reconcileAgentLabel}) {
		t.Fatalf("record = %#v", recorded)
	}
}

func TestInstallSweepsAgentsWhoseManifestVanished(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync", "reposync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	writeManifest(t, "reposync")

	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	orphan := labelPrefix + ".helper.reposync"
	if !slices.Contains(fake.applied, orphan) {
		t.Fatalf("first install applied = %#v", fake.applied)
	}
	removeManifest(t, "reposync")
	fake.reset()

	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.removed, []string{orphan}) {
		t.Fatalf("swept = %#v, want %#v", fake.removed, []string{orphan})
	}
	kept := []string{labelPrefix + ".helper.cookiesync", reconcileAgentLabel}
	if !slices.Equal(fake.applied, kept) {
		t.Fatalf("applied = %#v, want %#v", fake.applied, kept)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(recorded, kept) {
		t.Fatalf("record = %#v, want %#v", recorded, kept)
	}
}

func TestUninstallRemovesServeBeforeSettlingAndClearsTheRecord(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	fake.reset()

	if err := uninstall(t.Context()); err != nil {
		t.Fatal(err)
	}
	helper := labelPrefix + ".helper.cookiesync"
	want := []string{
		"remove:" + helper,
		"remove:" + reconcileAgentLabel,
		"remove:" + serveAgentLabel,
		"settle:" + serveAgentLabel,
	}
	if !slices.Equal(fake.events, want) {
		t.Fatalf("events = %#v, want %#v", fake.events, want)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if recorded != nil {
		t.Fatalf("record survived uninstall: %#v", recorded)
	}
}

func TestUninstallKeepsTheRecordWhenARemovalFails(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	fake.reset()
	stuck := errors.New("launchd refused")
	fake.removeErr[reconcileAgentLabel] = stuck

	if err := uninstall(t.Context()); !errors.Is(err, stuck) {
		t.Fatalf("error = %v", err)
	}
	if !slices.Contains(fake.events, "settle:"+serveAgentLabel) {
		t.Fatalf("events = %#v", fake.events)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(recorded, reconcileAgentLabel) {
		t.Fatalf("record = %#v", recorded)
	}
}

// TestInstallSweepsOnlyGenuinelyAbsentManifests pins both arms of the stray
// sweep against one directory: reposync's manifest is replaced with a file that
// will not decode, so its consumer is stale rather than gone and its agent must
// survive untouched; cookiesync's manifest is deleted outright, so its agent is
// the only stray. Post-upgrade every manifest on an existing machine is
// unloadable, which would otherwise make the first install tear down every
// helper.
func TestInstallSweepsOnlyGenuinelyAbsentManifests(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync", "reposync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	writeManifest(t, "reposync")
	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	stale, absent := labelPrefix+".helper.reposync", labelPrefix+".helper.cookiesync"
	if !slices.Contains(fake.applied, stale) || !slices.Contains(fake.applied, absent) {
		t.Fatalf("first install applied = %#v", fake.applied)
	}
	corruptManifest(t, "reposync")
	removeManifest(t, "cookiesync")
	fake.reset()

	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.removed, []string{absent}) {
		t.Fatalf("swept = %#v, want only the absent manifest's agent %q", fake.removed, absent)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(recorded, stale) {
		t.Fatalf("record = %#v, want the retained agent still named so a later run can sweep it", recorded)
	}
	if slices.Contains(recorded, absent) {
		t.Fatalf("record = %#v, want the swept agent gone", recorded)
	}

	removeManifest(t, "reposync")
	fake.reset()

	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.removed, []string{stale}) {
		t.Fatalf("swept = %#v, want %q swept once its consumer truly unregistered", fake.removed, stale)
	}
}

// TestUninstallSweepsEveryRecordedAgentDespiteAFailedDiscovery proves a
// manifests directory that will not load cannot wedge uninstall with agents
// still loaded: the label sources are independent, so the record still names
// what to remove, the serve daemon still settles, and the discovery failure
// surfaces beside them.
func TestUninstallSweepsEveryRecordedAgentDespiteAFailedDiscovery(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	fake.reset()
	duplicateManifest(t, "cookiesync")

	err := uninstall(t.Context())
	if err == nil {
		t.Fatal("uninstall succeeded despite a manifests directory that will not load")
	}
	helper := labelPrefix + ".helper.cookiesync"
	want := []string{
		"remove:" + helper,
		"remove:" + reconcileAgentLabel,
		"remove:" + serveAgentLabel,
		"settle:" + serveAgentLabel,
	}
	if !slices.Equal(fake.events, want) {
		t.Fatalf("events = %#v, want %#v", fake.events, want)
	}
	recorded, readErr := readInstalledAgents()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !slices.Contains(recorded, helper) {
		t.Fatalf("record = %#v, want the helper kept for the next run", recorded)
	}
}

// TestUninstallDeletesTheStagedProgramsItRegistered proves the durable record is
// what makes the staged copies removable: each label it names loses its
// <mesh>/bin copy once launchd has let that agent go.
func TestUninstallDeletesTheStagedProgramsItRegistered(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	useResidentStaging(t)
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context()); err != nil {
		t.Fatal(err)
	}
	dir, err := stagedProgramDir()
	if err != nil {
		t.Fatal(err)
	}
	staged := []string{
		filepath.Join(dir, labelPrefix+".helper.cookiesync"),
		filepath.Join(dir, reconcileAgentLabel),
	}
	for _, path := range staged {
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fake.reset()

	if err := uninstall(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, path := range staged {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("staged program %q survived uninstall: %v", path, err)
		}
	}
}

func TestUninstallLabelsUnionTheRecordAndTheManifests(t *testing.T) {
	helper := func(name string) string { return labelPrefix + ".helper." + name }
	tests := []struct {
		name      string
		recorded  []string
		manifests []string
		want      []string
	}{
		{
			name: "nothing recorded, nothing registered",
			want: []string{reconcileAgentLabel},
		},
		{
			name:      "manifest registered since the record was written",
			recorded:  []string{reconcileAgentLabel},
			manifests: []string{"cookiesync"},
			want:      []string{helper("cookiesync"), reconcileAgentLabel},
		},
		{
			name:     "manifest vanished since the record was written",
			recorded: []string{helper("reposync"), reconcileAgentLabel},
			want:     []string{helper("reposync"), reconcileAgentLabel},
		},
		{
			name:      "record and manifests disagree in both directions",
			recorded:  []string{helper("reposync"), reconcileAgentLabel},
			manifests: []string{"cookiesync"},
			want:      []string{helper("cookiesync"), helper("reposync"), reconcileAgentLabel},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useHome(t)
			useMesh(t)
			if _, err := ensureManifestsDir(); err != nil {
				t.Fatal(err)
			}
			for _, name := range tt.manifests {
				writeManifest(t, name)
			}
			if tt.recorded != nil {
				if err := writeInstalledAgents(tt.recorded); err != nil {
					t.Fatal(err)
				}
			}
			labels, err := uninstallLabels()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(labels, tt.want) {
				t.Fatalf("labels = %#v, want %#v", labels, tt.want)
			}
		})
	}
}

// TestRemoveAgentRemovesAMarkerlessLegacyPlist covers the whole legacy waiver:
// only a markerless plist takes it, the bootout's own verdict decides whether
// the file may go, and a plist marked since the refusal belongs to Remove.
func TestRemoveAgentRemovesAMarkerlessLegacyPlist(t *testing.T) {
	notOwned := fmt.Errorf("%w: %q", launchd.ErrNotOwned, "plist")
	refused := errors.New("launchd refused")
	const (
		markerless = "<plist/>"
		marked     = "<plist><key>" + launchd.OwnerEnvKey + "</key></plist>"
	)
	tests := []struct {
		name       string
		label      string
		markedErr  error
		body       string
		bootout    string
		bootoutNo  int
		wantErr    error
		wantReason string
		wantGone   bool
		wantCalls  int
	}{
		{name: "marked plist", label: reconcileAgentLabel, body: markerless},
		{
			name: "legacy plist", label: reconcileAgentLabel, markedErr: notOwned, body: markerless,
			wantGone: true, wantCalls: 1,
		},
		{
			name: "launchd does not know the label", label: reconcileAgentLabel, markedErr: notOwned,
			body: markerless, bootout: "Boot-out failed: 3: No such process", bootoutNo: 3,
			wantGone: true, wantCalls: 1,
		},
		{
			name: "bootout refusal keeps the plist", label: reconcileAgentLabel, markedErr: notOwned,
			body: markerless, bootout: "Boot-out failed: 1: Operation not permitted", bootoutNo: 1,
			wantReason: "Operation not permitted", wantCalls: 1,
		},
		{
			name: "a plist marked since the refusal is not legacy", label: reconcileAgentLabel,
			markedErr: notOwned, body: marked, wantErr: launchd.ErrMarked,
		},
		{name: "refusal surfaces", label: reconcileAgentLabel, markedErr: refused, body: markerless, wantErr: refused},
		{
			name:      "foreign label never takes the legacy path",
			label:     "com.example.other",
			markedErr: notOwned,
			body:      markerless,
			wantErr:   launchd.ErrNotOwned,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useHome(t)
			var ran [][]string
			previousCtl, previousRemove := launchctl, removeMarkedAgent
			launchctl = func(_ context.Context, path string, args ...string) (string, int, error) {
				ran = append(ran, append([]string{path}, args...))
				return tt.bootout, tt.bootoutNo, nil
			}
			removeMarkedAgent = func(context.Context, string) error { return tt.markedErr }
			t.Cleanup(func() { launchctl, removeMarkedAgent = previousCtl, previousRemove })

			plist, err := launchd.Agent{Label: tt.label}.PlistPath()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(plist), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(plist, []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}

			err = removeAgent(t.Context(), tt.label)
			switch {
			case tt.wantErr == nil && tt.wantReason == "" && err != nil:
				t.Fatalf("error = %v", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			case tt.wantReason != "" && (err == nil || !strings.Contains(err.Error(), tt.wantReason)):
				t.Fatalf("error = %v, want launchd's own %q refusal", err, tt.wantReason)
			}
			_, statErr := os.Lstat(plist)
			if gone := errors.Is(statErr, os.ErrNotExist); gone != tt.wantGone {
				t.Fatalf("plist gone = %t, want %t", gone, tt.wantGone)
			}
			if len(ran) != tt.wantCalls {
				t.Fatalf("launchctl calls = %#v, want %d", ran, tt.wantCalls)
			}
		})
	}
}

func TestInstalledAgentsRecordRefusesForeignLabels(t *testing.T) {
	tests := []struct {
		name   string
		record installedAgents
		ok     bool
	}{
		{
			name:   "synckit labels",
			record: installedAgents{Identity: installedAgentsIdentity, Labels: []string{reconcileAgentLabel}},
			ok:     true,
		},
		{
			name:   "empty set",
			record: installedAgents{Identity: installedAgentsIdentity},
			ok:     true,
		},
		{
			name:   "wrong identity",
			record: installedAgents{Identity: "other", Labels: []string{reconcileAgentLabel}},
		},
		{
			name:   "foreign label",
			record: installedAgents{Identity: installedAgentsIdentity, Labels: []string{"com.example.other"}},
		},
		{
			name:   "prefix is not a component boundary",
			record: installedAgents{Identity: installedAgentsIdentity, Labels: []string{labelPrefix + "-evil.serve"}},
		},
		{
			name:   "label launchd would refuse",
			record: installedAgents{Identity: installedAgentsIdentity, Labels: []string{labelPrefix + "/../evil"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.record.Validate(); (err == nil) != tt.ok {
				t.Fatalf("Validate() = %v, want ok = %t", err, tt.ok)
			}
		})
	}
}

func mustExecutable(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return self
}

func findAgent(t *testing.T, agents []launchd.Agent, label string) launchd.Agent {
	t.Helper()
	for _, agent := range agents {
		if agent.Label == label {
			return agent
		}
	}
	t.Fatalf("missing agent %q", label)
	return launchd.Agent{}
}
