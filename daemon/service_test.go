package daemon

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
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

func (f *fakeLaunchd) ensure(context.Context, string) error {
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
	apply, remove := applyAgent, removeAgent
	ensure, settle := ensureServeAgent, settleServeAgent
	applyAgent, removeAgent = fake.apply, fake.remove
	ensureServeAgent, settleServeAgent = fake.ensure, fake.settle
	t.Cleanup(func() {
		applyAgent, removeAgent = apply, remove
		ensureServeAgent, settleServeAgent = ensure, settle
	})
	return fake
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
	for _, name := range append(names, daemonBinary) {
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
	path, err := hostregistry.RegisteredPath(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
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

func TestReconcileAgentRunsFromThePATHAliasOnASchedule(t *testing.T) {
	useHome(t)
	useMesh(t)
	bin := usePathBinaries(t)

	reconcile, err := reconcileAgent()
	if err != nil {
		t.Fatal(err)
	}
	if reconcile.RestartPolicy != launchd.NoRestart || reconcile.StartInterval != reconcileInterval ||
		reconcile.ProcessType != launchd.ProcessTypeBackground {
		t.Fatalf("reconcile policy = %#v", reconcile)
	}
	if reconcile.Program != filepath.Join(bin, daemonBinary) || !slices.Equal(reconcile.Args, []string{"reconcile"}) {
		t.Fatalf("reconcile runs %q %v, want the %s alias", reconcile.Program, reconcile.Args, daemonBinary)
	}
	if reconcile.Env["PATH"] != daemonPATH {
		t.Fatalf("reconcile PATH = %q", reconcile.Env["PATH"])
	}
	if !filepath.IsAbs(reconcile.LogPath) || filepath.Base(reconcile.LogPath) != reconcile.Label+".log" {
		t.Fatalf("reconcile log = %q", reconcile.LogPath)
	}
}

// TestInstalledAgentPlistsNameTheirSubcommand reads ProgramArguments out of the
// plist bytes launchd parses, for every agent synckitd install writes. synckitd
// prints help when run bare, so the program alone crash-loops the job — what
// v0.37.0 shipped for serve, past assertions that read Args off the struct.
func TestInstalledAgentPlistsNameTheirSubcommand(t *testing.T) {
	home := useHome(t)
	useMesh(t)
	bin := usePathBinaries(t)

	spec, err := stableServeSpec("test")
	if err != nil {
		t.Fatal(err)
	}
	reconcile, err := reconcileAgent()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		plist []byte
		want  []string
	}{
		{
			name:  "serve",
			plist: ensuredServePlist(t, spec, home),
			want:  []string{filepath.Join(home, ".daemonkit", "bin", serveAgentLabel), "serve"},
		},
		{
			name:  "reconcile",
			plist: agentPlist(t, reconcile),
			want:  []string{filepath.Join(bin, daemonBinary), "reconcile"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := plistStrings(t, tt.plist, "ProgramArguments")
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ProgramArguments = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func ensuredServePlist(t *testing.T, spec daemonkit.Daemon, home string) []byte {
	t.Helper()
	label := string(spec.Label)
	return agentPlist(t, launchd.Agent{
		Label:         label,
		Program:       filepath.Join(home, ".daemonkit", "bin", label),
		Args:          spec.Args,
		LogPath:       paths.Agent(label).LogPath(),
		RestartPolicy: launchd.RestartAlways,
		ExitTimeOut:   serveShutdown,
	})
}

func agentPlist(t *testing.T, agent launchd.Agent) []byte {
	t.Helper()
	plist, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	return plist
}

// plistStrings decodes the string array a rendered plist records under key.
func plistStrings(t *testing.T, plist []byte, key string) []string {
	t.Helper()
	decoder := xml.NewDecoder(bytes.NewReader(plist))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			t.Fatalf("plist has no %q key\n%s", key, plist)
		}
		if err != nil {
			t.Fatalf("decode plist: %v", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var name string
		if err := decoder.DecodeElement(&name, &start); err != nil {
			t.Fatalf("decode plist key: %v", err)
		}
		if name != key {
			continue
		}
		var array struct {
			Values []string `xml:"string"`
		}
		if err := decoder.Decode(&array); err != nil {
			t.Fatalf("decode plist %q: %v", key, err)
		}
		return array.Values
	}
}

func TestInstallEnsuresServeBeforeApplyingTheReconcileAgent(t *testing.T) {
	useHome(t)
	useMesh(t)
	usePathBinaries(t, "cookiesync")
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")

	if err := install(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	helper := labelPrefix + ".helper.cookiesync"
	want := []string{"ensure:" + serveAgentLabel, "apply:" + reconcileAgentLabel}
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
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	writeManifest(t, "reposync")

	if err := install(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	orphan := labelPrefix + ".helper.reposync"
	first, err := readInstalledAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(first, orphan) {
		t.Fatalf("first install recorded = %#v", first)
	}
	removeManifest(t, "reposync")
	fake.reset()

	if err := install(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.removed, []string{orphan}) {
		t.Fatalf("swept = %#v, want %#v", fake.removed, []string{orphan})
	}
	kept := []string{labelPrefix + ".helper.cookiesync", reconcileAgentLabel}
	if !slices.Equal(fake.applied, []string{reconcileAgentLabel}) {
		t.Fatalf("applied = %#v, want only the reconcile agent", fake.applied)
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
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context(), "test"); err != nil {
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
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context(), "test"); err != nil {
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
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	writeManifest(t, "reposync")
	if err := install(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	stale, absent := labelPrefix+".helper.reposync", labelPrefix+".helper.cookiesync"
	if first, err := readInstalledAgents(); err != nil || !slices.Contains(first, stale) || !slices.Contains(first, absent) {
		t.Fatalf("first install recorded = %#v, %v", first, err)
	}
	corruptManifest(t, "reposync")
	removeManifest(t, "cookiesync")
	fake.reset()

	if err := install(t.Context(), "test"); err != nil {
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

	if err := install(t.Context(), "test"); err != nil {
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
	fake := useLaunchd(t)
	writeManifest(t, "cookiesync")
	if err := install(t.Context(), "test"); err != nil {
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
