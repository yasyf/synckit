package service

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const fakeExe = "/opt/homebrew/Cellar/synckit/1.2.3/bin/synckit"

// testConfig is a two-agent tool config exercising every ExtraKeys value type: int
// (StartInterval), bool (LowPriorityIO/RunAtLoad/KeepAlive), and string
// (ProcessType on the periodic tick and an Aqua session type on the Standard
// watch agent).
func testConfig() ToolConfig {
	return ToolConfig{
		BinaryName:  "synckit",
		LabelPrefix: "com.example.synckit",
		DaemonPATH:  DefaultDaemonPATH,
		LogName: func(label string) string {
			return filepath.Join("Library", "Logs", label+".log")
		},
		Agents: []AgentSpec{
			{Label: "tick", Command: "reconcile", ExtraKeys: map[string]any{
				"StartInterval":    900,
				"ThrottleInterval": 900,
				"RunAtLoad":        true,
				"ProcessType":      "Background",
				"Nice":             10,
				"LowPriorityIO":    true,
			}},
			{Label: "watch", Command: "watch", ExtraKeys: map[string]any{
				"KeepAlive":              true,
				"RunAtLoad":              true,
				"ThrottleInterval":       10,
				"LimitLoadToSessionType": "Aqua",
			}},
		},
	}
}

// parsePlist parses a launchd plist's top-level <dict> into a flat map. Nested
// <dict>/<array> values become a map / slice so callers can assert on PATH and
// ProgramArguments. It fails the test on malformed XML so it doubles as a
// well-formedness check.
func parsePlist(t *testing.T, xmlStr string) map[string]any {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(xmlStr))
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			t.Fatalf("plist has no top-level <dict>")
		}
		if err != nil {
			t.Fatalf("plist is not well-formed XML: %v", err)
		}
		if start, ok := tok.(xml.StartElement); ok && start.Name.Local == "dict" {
			return parseDict(t, dec)
		}
	}
}

func parseDict(t *testing.T, dec *xml.Decoder) map[string]any {
	t.Helper()
	out := map[string]any{}
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("plist dict parse: %v", err)
		}
		switch el := tok.(type) {
		case xml.StartElement:
			if el.Name.Local != "key" {
				t.Fatalf("expected <key>, got <%s>", el.Name.Local)
			}
			key := readChardata(t, dec)
			out[key] = parseValue(t, dec)
		case xml.EndElement:
			if el.Name.Local == "dict" {
				return out
			}
		}
	}
}

func parseValue(t *testing.T, dec *xml.Decoder) any {
	t.Helper()
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("plist value parse: %v", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "string":
			return readChardata(t, dec)
		case "integer":
			n, err := strconv.Atoi(strings.TrimSpace(readChardata(t, dec)))
			if err != nil {
				t.Fatalf("plist integer parse: %v", err)
			}
			return n
		case "true":
			return true
		case "false":
			return false
		case "dict":
			return parseDict(t, dec)
		case "array":
			return parseArray(t, dec)
		default:
			t.Fatalf("unexpected plist value <%s>", start.Name.Local)
		}
	}
}

func parseArray(t *testing.T, dec *xml.Decoder) []any {
	t.Helper()
	var out []any
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("plist array parse: %v", err)
		}
		switch el := tok.(type) {
		case xml.StartElement:
			if el.Name.Local == "string" {
				out = append(out, readChardata(t, dec))
			}
		case xml.EndElement:
			if el.Name.Local == "array" {
				return out
			}
		}
	}
}

func readChardata(t *testing.T, dec *xml.Decoder) string {
	t.Helper()
	var sb strings.Builder
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("plist chardata parse: %v", err)
		}
		switch el := tok.(type) {
		case xml.CharData:
			sb.Write(el)
		case xml.EndElement:
			return sb.String()
		}
	}
}

func agentByLabel(cfg ToolConfig, suffix string) AgentSpec {
	for _, a := range cfg.Agents {
		if a.Label == suffix {
			return a
		}
	}
	panic("no agent " + suffix)
}

func TestRenderPlistCommonKeys(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()
	agent := agentByLabel(cfg, "tick")

	out, err := renderPlist(cfg, fakeExe, agent)
	if err != nil {
		t.Fatalf("renderPlist: %v", err)
	}
	got := parsePlist(t, out)

	if got["Label"] != "com.example.synckit.tick" {
		t.Errorf("Label = %v", got["Label"])
	}
	wantArgs := []any{fakeExe, "reconcile"}
	if args, _ := got["ProgramArguments"].([]any); len(args) != 2 || args[0] != wantArgs[0] || args[1] != wantArgs[1] {
		t.Errorf("ProgramArguments = %v, want %v", got["ProgramArguments"], wantArgs)
	}
	env, ok := got["EnvironmentVariables"].(map[string]any)
	if !ok || env["PATH"] != DefaultDaemonPATH {
		t.Errorf("EnvironmentVariables.PATH = %v, want %q", got["EnvironmentVariables"], DefaultDaemonPATH)
	}
	wantLog := filepath.Join(home, "Library", "Logs", "com.example.synckit.tick.log")
	if got["StandardOutPath"] != wantLog || got["StandardErrorPath"] != wantLog {
		t.Errorf("log paths = out:%v err:%v, want %q", got["StandardOutPath"], got["StandardErrorPath"], wantLog)
	}
	if strings.Contains(out, "~/Library") {
		t.Errorf("plist contains unexpanded ~ path")
	}
}

// TestRenderPlistBinaryOverride proves an AgentSpec.Binary overrides the own-exe as
// the program / first ProgramArguments entry, resolved on PATH (symlink-preserved,
// not EvalSymlinks'd), while empty Binary leaves the own-exe in place.
func TestRenderPlistBinaryOverride(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "helper-bin")
	//nolint:gosec // G306: test writes an executable shell-script fixture that must be runnable for PATH resolution.
	if err := os.WriteFile(binPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	cfg := testConfig()

	tests := []struct {
		name        string
		agent       AgentSpec
		wantProgram string
	}{
		{
			name:        "binary override resolves on PATH",
			agent:       AgentSpec{Label: "helper", Binary: "helper-bin", Command: "helper-serve"},
			wantProgram: binPath,
		},
		{
			name:        "empty binary keeps own exe",
			agent:       AgentSpec{Label: "tick", Command: "reconcile"},
			wantProgram: fakeExe,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parsePlist(t, mustRender(t, cfg, tt.agent))
			args, ok := got["ProgramArguments"].([]any)
			if !ok || len(args) != 2 {
				t.Fatalf("ProgramArguments = %v, want 2 strings", got["ProgramArguments"])
			}
			if args[0] != tt.wantProgram {
				t.Errorf("program = %v, want %q", args[0], tt.wantProgram)
			}
			if args[1] != tt.agent.Command {
				t.Errorf("command = %v, want %q", args[1], tt.agent.Command)
			}
		})
	}
}

// TestRenderPlistBinaryUnresolvable proves a Binary not on PATH fails render rather
// than silently falling back to the own-exe.
func TestRenderPlistBinaryUnresolvable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	cfg := testConfig()
	agent := AgentSpec{Label: "helper", Binary: "nope-not-here", Command: "x"}

	if _, err := renderPlist(cfg, fakeExe, agent); err == nil {
		t.Fatal("expected error for unresolvable Binary")
	}
}

func TestRenderPlistExtraKeysByType(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()

	tick := parsePlist(t, mustRender(t, cfg, agentByLabel(cfg, "tick")))
	for key, want := range map[string]any{
		"StartInterval":    900,
		"ThrottleInterval": 900,
		"Nice":             10,
		"RunAtLoad":        true,
		"LowPriorityIO":    true,
		"ProcessType":      "Background",
	} {
		if tick[key] != want {
			t.Errorf("tick[%q] = %v (%T), want %v (%T)", key, tick[key], tick[key], want, want)
		}
	}
	// Cross-agent watch keys must be absent from the tick plist.
	for _, absent := range []string{"KeepAlive", "LimitLoadToSessionType"} {
		if _, ok := tick[absent]; ok {
			t.Errorf("tick plist unexpectedly has %q", absent)
		}
	}

	watch := parsePlist(t, mustRender(t, cfg, agentByLabel(cfg, "watch")))
	for key, want := range map[string]any{
		"KeepAlive":              true,
		"ThrottleInterval":       10,
		"LimitLoadToSessionType": "Aqua",
	} {
		if watch[key] != want {
			t.Errorf("watch[%q] = %v (%T), want %v (%T)", key, watch[key], watch[key], want, want)
		}
	}
	// Cross-agent tick keys must be absent from the watch plist.
	for _, absent := range []string{"StartInterval", "Nice", "LowPriorityIO", "ProcessType"} {
		if _, ok := watch[absent]; ok {
			t.Errorf("watch plist unexpectedly has %q", absent)
		}
	}
}

// TestRenderPlistDeterministic renders the same agent many times; sorted ExtraKeys
// make every render byte-identical despite random Go map iteration order.
func TestRenderPlistDeterministic(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()
	agent := agentByLabel(cfg, "tick")

	first := mustRender(t, cfg, agent)
	for range 50 {
		if got := mustRender(t, cfg, agent); got != first {
			t.Fatalf("render not deterministic:\n--- first ---\n%s\n--- got ---\n%s", first, got)
		}
	}
	// Keys appear in sorted order between EnvironmentVariables and StandardOutPath.
	want := []string{"LowPriorityIO", "Nice", "ProcessType", "RunAtLoad", "StartInterval", "ThrottleInterval"}
	if idx := keyOrder(first, want); !idx {
		t.Errorf("ExtraKeys not in sorted order in:\n%s", first)
	}
}

func TestRenderPlistUnsupportedValueType(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()
	cfg.Agents = []AgentSpec{{Label: "bad", Command: "x", ExtraKeys: map[string]any{"Floaty": 1.5}}}

	if _, err := renderPlist(cfg, fakeExe, cfg.Agents[0]); err == nil {
		t.Fatal("expected error for float64 ExtraKeys value")
	}
}

func mustRender(t *testing.T, cfg ToolConfig, agent AgentSpec) string {
	t.Helper()
	out, err := renderPlist(cfg, fakeExe, agent)
	if err != nil {
		t.Fatalf("renderPlist: %v", err)
	}
	return out
}

// keyOrder reports whether the <key>NAME</key> tags for want appear in the given
// order in xmlStr.
func keyOrder(xmlStr string, want []string) bool {
	pos := -1
	for _, k := range want {
		i := strings.Index(xmlStr, "<key>"+k+"</key>")
		if i <= pos {
			return false
		}
		pos = i
	}
	return true
}

// fakeLauncher records the plist paths passed to Bootstrap and the labels passed to
// Bootout, in call order. bootstrapErrs and bootoutErrs are per-call error queues:
// each call pops the next error (an empty queue means success), so a test can script
// a bootstrap that fails a few times then succeeds. events is one interleaved log of
// bootstrap/bootout/sleep in true call order (the sleep entries come from the sleep
// method a retry test wires into the manager), and delays captures the backoff
// durations, so a test pins ordering, not just counts.
type fakeLauncher struct {
	bootstrapped  []string // plist paths
	bootedOut     []string // launchd labels
	bootstrapErrs []error
	bootoutErrs   []error
	events        []string
	delays        []time.Duration
}

func (f *fakeLauncher) Bootstrap(_ context.Context, plistPath string) error {
	f.bootstrapped = append(f.bootstrapped, plistPath)
	f.events = append(f.events, "bootstrap")
	return popErr(&f.bootstrapErrs)
}

func (f *fakeLauncher) Bootout(_ context.Context, label string) error {
	f.bootedOut = append(f.bootedOut, label)
	f.events = append(f.events, "bootout")
	return popErr(&f.bootoutErrs)
}

func (f *fakeLauncher) sleep(_ context.Context, d time.Duration) error {
	f.delays = append(f.delays, d)
	f.events = append(f.events, "sleep "+d.String())
	return nil
}

func popErr(q *[]error) error {
	if len(*q) == 0 {
		return nil
	}
	err := (*q)[0]
	*q = (*q)[1:]
	return err
}

func skipNonDarwin(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skipf("Install/Uninstall are macOS-only; skipping on %s", runtime.GOOS)
	}
}

func plistPath(home, label string) string {
	return filepath.Join(home, launchAgentsRelpath, label+".plist")
}

func TestInstallAllAgents(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()

	f := &fakeLauncher{}
	if err := NewLaunchdManager(f).Install(context.Background(), cfg, false); err != nil {
		t.Fatalf("Install: %v", err)
	}

	tickLabel := "com.example.synckit.tick"
	watchLabel := "com.example.synckit.watch"
	tickPath := plistPath(home, tickLabel)
	watchPath := plistPath(home, watchLabel)

	tickData, err := os.ReadFile(tickPath) //nolint:gosec // G304: test reads a plist from a test-controlled temp home.
	if err != nil {
		t.Fatalf("read tick plist: %v", err)
	}
	if got := parsePlist(t, string(tickData)); got["Label"] != tickLabel || got["StartInterval"] != 900 {
		t.Errorf("tick plist on disk wrong: %v", got)
	}
	watchData, err := os.ReadFile(watchPath) //nolint:gosec // G304: test reads a plist from a test-controlled temp home.
	if err != nil {
		t.Fatalf("read watch plist: %v", err)
	}
	if got := parsePlist(t, string(watchData)); got["Label"] != watchLabel || got["KeepAlive"] != true {
		t.Errorf("watch plist on disk wrong: %v", got)
	}

	if !equalStrings(f.bootstrapped, []string{tickPath, watchPath}) {
		t.Errorf("Bootstrap calls = %v, want %v", f.bootstrapped, []string{tickPath, watchPath})
	}
	// Each install boots out before bootstrap so reload picks up changes.
	if !equalStrings(f.bootedOut, []string{tickLabel, watchLabel}) {
		t.Errorf("Bootout calls = %v, want %v", f.bootedOut, []string{tickLabel, watchLabel})
	}
}

// TestInstallBootoutBeforeBootstrap pins the per-agent ordering: for each agent the
// bootout of its label precedes the bootstrap of its plist.
func TestInstallBootoutBeforeBootstrap(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()

	var calls []string
	rec := &orderLauncher{record: func(s string) { calls = append(calls, s) }}
	if err := NewLaunchdManager(rec).Install(context.Background(), cfg, false); err != nil {
		t.Fatalf("Install: %v", err)
	}
	want := []string{
		"bootout com.example.synckit.tick",
		"bootstrap " + plistPath(home, "com.example.synckit.tick"),
		"bootout com.example.synckit.watch",
		"bootstrap " + plistPath(home, "com.example.synckit.watch"),
	}
	if !equalStrings(calls, want) {
		t.Errorf("call order = %v, want %v", calls, want)
	}
}

type orderLauncher struct {
	record func(string)
}

func (o *orderLauncher) Bootstrap(_ context.Context, plistPath string) error {
	o.record("bootstrap " + plistPath)
	return nil
}

func (o *orderLauncher) Bootout(_ context.Context, label string) error {
	o.record("bootout " + label)
	return nil
}

func TestInstallTickOnly(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()

	f := &fakeLauncher{}
	if err := NewLaunchdManager(f).Install(context.Background(), cfg, true); err != nil {
		t.Fatalf("Install: %v", err)
	}

	tickPath := plistPath(home, "com.example.synckit.tick")
	watchPath := plistPath(home, "com.example.synckit.watch")
	if _, err := os.Stat(tickPath); err != nil {
		t.Errorf("tick plist should exist: %v", err)
	}
	if _, err := os.Stat(watchPath); !os.IsNotExist(err) {
		t.Errorf("watch plist should be absent, got err=%v", err)
	}
	if !equalStrings(f.bootstrapped, []string{tickPath}) {
		t.Errorf("Bootstrap calls = %v, want %v", f.bootstrapped, []string{tickPath})
	}
}

func TestInstallPlistMode(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)

	if err := NewLaunchdManager(&fakeLauncher{}).Install(context.Background(), testConfig(), true); err != nil {
		t.Fatalf("Install: %v", err)
	}
	info, err := os.Stat(plistPath(home, "com.example.synckit.tick"))
	if err != nil {
		t.Fatalf("stat tick plist: %v", err)
	}
	if info.Mode().Perm() != plistFileMode {
		t.Errorf("tick plist mode = %o, want %o", info.Mode().Perm(), plistFileMode)
	}
}

// TestInstallPreflightAbortsBeforeWrite proves a failing per-agent preflight aborts
// install before the agent's plist is written or loaded, and only fires for the
// agent it targets.
func TestInstallPreflightAbortsBeforeWrite(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()
	sentinel := errors.New("watch dependency missing")
	var checked []string
	cfg.PreflightCheck = func(_ context.Context, agent AgentSpec) error {
		checked = append(checked, agent.Label)
		if agent.Label == "watch" {
			return sentinel
		}
		return nil
	}

	f := &fakeLauncher{}
	err := NewLaunchdManager(f).Install(context.Background(), cfg, false)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Install error = %v, want %v", err, sentinel)
	}
	if !equalStrings(checked, []string{"tick", "watch"}) {
		t.Errorf("preflight ran for %v, want [tick watch]", checked)
	}
	// Tick loaded; watch plist never written and never bootstrapped.
	if _, err := os.Stat(plistPath(home, "com.example.synckit.watch")); !os.IsNotExist(err) {
		t.Errorf("watch plist should not exist, got err=%v", err)
	}
	if !equalStrings(f.bootstrapped, []string{plistPath(home, "com.example.synckit.tick")}) {
		t.Errorf("Bootstrap calls = %v, want only the tick", f.bootstrapped)
	}
}

func TestUninstall(t *testing.T) {
	skipNonDarwin(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := testConfig()

	m := NewLaunchdManager(&fakeLauncher{})
	if err := m.Install(context.Background(), cfg, false); err != nil {
		t.Fatalf("Install: %v", err)
	}

	f := &fakeLauncher{}
	if err := NewLaunchdManager(f).Uninstall(context.Background(), cfg); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}

	tickLabel := "com.example.synckit.tick"
	watchLabel := "com.example.synckit.watch"
	if !equalStrings(f.bootedOut, []string{tickLabel, watchLabel}) {
		t.Errorf("Bootout calls = %v, want %v", f.bootedOut, []string{tickLabel, watchLabel})
	}
	for _, p := range []string{plistPath(home, tickLabel), plistPath(home, watchLabel)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("plist %s should be removed, got err=%v", p, err)
		}
	}
}

func TestUninstallMissingFilesOK(t *testing.T) {
	skipNonDarwin(t)
	t.Setenv("HOME", t.TempDir())

	f := &fakeLauncher{}
	if err := NewLaunchdManager(f).Uninstall(context.Background(), testConfig()); err != nil {
		t.Fatalf("Uninstall with no installed agents should succeed: %v", err)
	}
	if len(f.bootedOut) != 2 {
		t.Errorf("expected 2 Bootout calls, got %d", len(f.bootedOut))
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		//nolint:gosec // G602: guarded above by len(a) != len(b), so b[i] is in range for every i in range a.
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		//nolint:gosec // G602: guarded above by len(a) != len(b), so b[i] is in range for every i in range a.
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// wrapExit wraps a real exit-N error the way the production launcher does
// (launchctl bootstrap %s: %w: %s), so a test proves isInFlux/errors.As reach the
// ExitError through the wrap rather than passing only against a bare exit error.
func wrapExit(t *testing.T, n int) error {
	t.Helper()
	return fmt.Errorf("launchctl bootstrap /p.plist: %w: %s", exitErr(t, n), "Bootstrap failed: 5: Input/output error")
}

// TestInstallBootstrapRetryThenSucceed proves a bootstrap that hits launchd's EIO
// (exit 5) twice then succeeds installs cleanly, and pins the exact interleaving of
// bootstrap, backoff sleep, and re-bootout — a count-only check would pass a broken
// order.
func TestInstallBootstrapRetryThenSucceed(t *testing.T) {
	skipNonDarwin(t)
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()

	f := &fakeLauncher{bootstrapErrs: []error{wrapExit(t, 5), wrapExit(t, 5)}}
	m := NewLaunchdManager(f)
	m.sleep = f.sleep

	if err := m.Install(context.Background(), cfg, true); err != nil {
		t.Fatalf("Install: %v", err)
	}
	// The leading bootout is installAgent's reload bootout; the rest is the retry loop.
	wantEvents := []string{
		"bootout",
		"bootstrap", "sleep 200ms", "bootout",
		"bootstrap", "sleep 400ms", "bootout",
		"bootstrap",
	}
	if !equalStrings(f.events, wantEvents) {
		t.Errorf("event order = %v, want %v", f.events, wantEvents)
	}
}

// TestInstallBootstrapRetryExhaustion proves a bootstrap that hits EIO on every one
// of bootstrapAttempts tries fails install, sleeps the full doubling schedule, boots
// out once per retry (no extra bootout after the final failed bootstrap), and
// surfaces the exact final queued error — identity-pinned and reachable as an
// *exec.ExitError (code 5) through the production-style wrap.
func TestInstallBootstrapRetryExhaustion(t *testing.T) {
	skipNonDarwin(t)
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()

	errs := make([]error, bootstrapAttempts)
	for i := range errs {
		errs[i] = wrapExit(t, 5)
	}
	f := &fakeLauncher{bootstrapErrs: errs}
	m := NewLaunchdManager(f)
	m.sleep = f.sleep

	err := m.Install(context.Background(), cfg, true)
	if err == nil {
		t.Fatal("Install should fail after exhausting bootstrap retries")
	}
	if len(f.bootstrapped) != bootstrapAttempts {
		t.Errorf("Bootstrap calls = %d, want %d", len(f.bootstrapped), bootstrapAttempts)
	}
	// 1 initial bootout + 5 retry bootouts; none after the sixth (final) bootstrap.
	if len(f.bootedOut) != bootstrapAttempts {
		t.Errorf("Bootout calls = %d, want %d (1 initial + 5 retry)", len(f.bootedOut), bootstrapAttempts)
	}
	wantDelays := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond,
	}
	if !equalDurations(f.delays, wantDelays) {
		t.Errorf("sleeps = %v, want %v", f.delays, wantDelays)
	}
	// The distinct final queued error is the one surfaced, pinned through the wrap chain.
	if !errors.Is(err, errs[bootstrapAttempts-1]) {
		t.Errorf("errors.Is did not reach the final queued error; err = %v", err)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 5 {
		t.Errorf("errors.As = %v, want *exec.ExitError with code 5", err)
	}
	if !strings.Contains(err.Error(), "bootstrap com.example.synckit.tick") {
		t.Errorf("error %q missing labeled bootstrap wrap", err)
	}
}

// TestInstallSessionTypeHint proves the parenthetical hint appears only when a
// persistent EIO exhausts the retries on a session-limited agent, and that a non-EIO
// exit fails at once with no retry, no sleep, and no hint.
func TestInstallSessionTypeHint(t *testing.T) {
	skipNonDarwin(t)

	exhaust := func() []error {
		errs := make([]error, bootstrapAttempts)
		for i := range errs {
			errs[i] = exitErr(t, 5)
		}
		return errs
	}

	for _, tt := range []struct {
		name          string
		agentLabel    string
		bootstrapErrs []error
		wantBootstrap int
		wantBootout   int
		wantSleeps    int
		wantHint      bool
	}{
		{"aqua-limited EIO exhaustion gets hint", "watch", exhaust(), bootstrapAttempts, bootstrapAttempts, bootstrapAttempts - 1, true},
		{"non-limited EIO exhaustion gets no hint", "tick", exhaust(), bootstrapAttempts, bootstrapAttempts, bootstrapAttempts - 1, false},
		{"exit 1 fails at once without retry or hint", "watch", []error{exitErr(t, 1)}, 1, 1, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			cfg := testConfig()
			cfg.Agents = []AgentSpec{agentByLabel(cfg, tt.agentLabel)}
			f := &fakeLauncher{bootstrapErrs: tt.bootstrapErrs}
			m := NewLaunchdManager(f)
			m.sleep = f.sleep

			err := m.Install(context.Background(), cfg, false)
			if err == nil {
				t.Fatal("Install should fail")
			}
			if len(f.bootstrapped) != tt.wantBootstrap {
				t.Errorf("Bootstrap calls = %d, want %d", len(f.bootstrapped), tt.wantBootstrap)
			}
			if len(f.bootedOut) != tt.wantBootout {
				t.Errorf("Bootout calls = %d, want %d", len(f.bootedOut), tt.wantBootout)
			}
			if len(f.delays) != tt.wantSleeps {
				t.Errorf("sleeps = %d, want %d", len(f.delays), tt.wantSleeps)
			}
			gotHint := strings.Contains(err.Error(), "limited to session type Aqua")
			if gotHint != tt.wantHint {
				t.Errorf("hint present = %v, want %v (err: %q)", gotHint, tt.wantHint, err)
			}
		})
	}
}

// TestInstallBootstrapRetryBootoutFails proves that when the retry bootout itself
// fails with EIO, install fails after exactly one bootstrap with the bootout-retry
// wrap and — critically — does NOT misdiagnose it with the session-type hint, even on
// a session-limited agent whose bootout error also carries exit 5.
func TestInstallBootstrapRetryBootoutFails(t *testing.T) {
	skipNonDarwin(t)
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()
	cfg.Agents = []AgentSpec{agentByLabel(cfg, "watch")} // Aqua-limited

	f := &fakeLauncher{
		bootstrapErrs: []error{exitErr(t, 5)},
		bootoutErrs:   []error{nil, exitErr(t, 5)}, // initial bootout ok; retry bootout fails
	}
	m := NewLaunchdManager(f)
	m.sleep = f.sleep

	err := m.Install(context.Background(), cfg, false)
	if err == nil {
		t.Fatal("Install should fail when the retry bootout fails")
	}
	if len(f.bootstrapped) != 1 {
		t.Errorf("Bootstrap calls = %d, want 1", len(f.bootstrapped))
	}
	if len(f.bootedOut) != 2 {
		t.Errorf("Bootout calls = %d, want 2 (initial + retry)", len(f.bootedOut))
	}
	wantDelays := []time.Duration{200 * time.Millisecond}
	if !equalDurations(f.delays, wantDelays) {
		t.Errorf("sleeps = %v, want %v", f.delays, wantDelays)
	}
	if !strings.Contains(err.Error(), "bootout com.example.synckit.watch before retry") {
		t.Errorf("error %q missing bootout-before-retry wrap", err)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 5 {
		t.Errorf("errors.As = %v, want *exec.ExitError with code 5", err)
	}
	if strings.Contains(err.Error(), "limited to session type") {
		t.Errorf("bootout failure wrongly carries the session-type hint: %q", err)
	}
}

// TestInstallBootstrapCtxCancel proves a context canceled during the first backoff
// aborts install through the real sleepCtx after exactly one bootstrap attempt.
func TestInstallBootstrapCtxCancel(t *testing.T) {
	skipNonDarwin(t)
	t.Setenv("HOME", t.TempDir())
	cfg := testConfig()

	f := &fakeLauncher{bootstrapErrs: []error{exitErr(t, 5)}}
	m := NewLaunchdManager(f)
	ctx, cancel := context.WithCancel(context.Background())
	m.sleep = func(c context.Context, d time.Duration) error {
		cancel()
		return sleepCtx(c, d)
	}

	err := m.Install(ctx, cfg, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Install error = %v, want context.Canceled", err)
	}
	if len(f.bootstrapped) != 1 {
		t.Errorf("Bootstrap calls = %d, want 1", len(f.bootstrapped))
	}
}
