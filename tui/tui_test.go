package tui

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/exp/teatest"

	"github.com/yasyf/synckit/hostregistry"
)

func TestTabBarAndSwitch(t *testing.T) {
	opts := hermeticOptions(t)
	tm := teatest.NewTestModel(t, newRootModel(opts), teatest.WithInitialTermSize(100, 30))

	// The tab bar names the consumer's content screen and the appended Hosts
	// screen, and the content screen's body renders — all in one accumulated read.
	waitForContent(t, tm, "Content", "Hosts", "content body")

	// NextTab ("n") activates the Hosts screen, which lazily initializes and shows
	// its discovering state. The spinner keeps re-rendering that line while the
	// scan runs, so it recurs in fresh frames after the tab switch.
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	waitForContent(t, tm, "Discovering hosts")

	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.WaitFinished(t, teatest.WithFinalTimeout(5*time.Second))
}

func TestAddHostInputAndCancel(t *testing.T) {
	opts := hermeticOptions(t)
	tm := teatest.NewTestModel(t, newRootModel(opts), teatest.WithInitialTermSize(100, 30))

	// Switch to the Hosts tab and let it reach its discovering state.
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	waitForContent(t, tm, "Discovering hosts")

	// "+" opens the add-host text input, which surfaces its prompt and placeholder
	// in the same frame.
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'+'}})
	waitForContent(t, tm, "Add host:", "user@node")

	// esc returns to the list — the add-host prompt gives way to the list chrome
	// whose help footer offers "add host" again.
	tm.Send(tea.KeyMsg{Type: tea.KeyEsc})
	waitForContent(t, tm, "add host")

	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.WaitFinished(t, teatest.WithFinalTimeout(5*time.Second))
}

func TestCtrlCQuitsCleanly(t *testing.T) {
	opts := hermeticOptions(t)
	tm := teatest.NewTestModel(t, newRootModel(opts), teatest.WithInitialTermSize(100, 30))

	// Let the first frame render so the program is past Init before quitting.
	waitForContent(t, tm, "Content")

	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.WaitFinished(t, teatest.WithFinalTimeout(5*time.Second))

	// The router blanks its view on quit; the final model retains the quit flag.
	final := tm.FinalModel(t, teatest.WithFinalTimeout(5*time.Second)).(rootModel)
	if !final.quitting {
		t.Fatal("rootModel.quitting = false after ctrl+c, want true")
	}
}

func TestStartAddFocusesInput(t *testing.T) {
	m := newHostsModel(Options{})
	s, _ := m.startAdd("")
	hm := s.(hostsModel)
	if !hm.input.Focused() {
		t.Fatal("startAdd must focus the input so the host target can be typed")
	}
	// A keystroke must reach the (focused) input, not be swallowed.
	s, _ = hm.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	if got := s.(hostsModel).input.Value(); got != "x" {
		t.Fatalf("after typing into the add-host input, value = %q, want %q", got, "x")
	}
}

func TestValidateTarget(t *testing.T) {
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{name: "user@node", in: "yasyf@yasyf-home", ok: true},
		{name: "bare node", in: "yasyf-home", ok: true},
		{name: "dotted node", in: "node.tailnet.ts.net", ok: true},
		{name: "underscore user", in: "ad_min@node", ok: true},
		{name: "empty", in: "", ok: false},
		{name: "whitespace only", in: "   ", ok: false},
		{name: "embedded space", in: "user @node", ok: false},
		{name: "trailing space", in: "node ", ok: false},
		{name: "leading at", in: "@node", ok: false},
		{name: "node starts with hyphen", in: "-node", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTarget(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("validateTarget(%q) = %v, want nil", tc.in, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("validateTarget(%q) = nil, want error", tc.in)
			}
		})
	}
}

func TestClassifyVerify(t *testing.T) {
	cases := []struct {
		name string
		res  hostregistry.VerifyResult
		want verifyState
	}{
		{name: "ready", res: hostregistry.VerifyResult{Reachable: true, Bootstrapped: true}, want: verifyOK},
		{name: "reachable not installed", res: hostregistry.VerifyResult{Reachable: true}, want: verifyWarn},
		{name: "unreachable", res: hostregistry.VerifyResult{Err: errors.New("connection refused")}, want: verifyFail},
		{name: "unreachable but bootstrapped flag ignored", res: hostregistry.VerifyResult{Bootstrapped: true}, want: verifyFail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyVerify(tc.res); got != tc.want {
				t.Fatalf("classifyVerify(%+v) = %v, want %v", tc.res, got, tc.want)
			}
		})
	}
}

func TestMergeHostItems(t *testing.T) {
	cands := []hostregistry.HostCandidate{
		{Node: "alpha", DefaultTarget: "yasyf@alpha", Source: "tailscale", Online: true, Registered: true},
		{Node: "beta", DefaultTarget: "yasyf@beta", Source: "bonjour", Online: false, Registered: false},
	}
	// gamma is registered but undiscovered; beta is already covered by the "beta"
	// candidate's node so registration must not duplicate it.
	registered := []string{"yasyf@gamma", "yasyf@beta"}

	items := mergeHostItems(cands, registered)

	if len(items) != 3 {
		t.Fatalf("got %d items, want 3: %+v", len(items), items)
	}

	// Registered hosts float to the top: alpha (discovered+registered) then gamma
	// (registered-only) ahead of beta (discovered-only). Within each group order is
	// the assembly order, so alpha precedes gamma.
	alpha := items[0]
	if alpha.node != "alpha" || alpha.target != "yasyf@alpha" || alpha.source != "tailscale" {
		t.Fatalf("alpha = %+v, want node=alpha target=yasyf@alpha source=tailscale", alpha)
	}
	if !alpha.online || !alpha.registered {
		t.Fatalf("alpha online/registered = %v/%v, want true/true", alpha.online, alpha.registered)
	}

	gamma := items[1]
	if gamma.node != "gamma" || gamma.target != "yasyf@gamma" {
		t.Fatalf("gamma = %+v, want node=gamma target=yasyf@gamma", gamma)
	}
	if gamma.source != "registered" || gamma.online || !gamma.registered {
		t.Fatalf("gamma = %+v, want source=registered online=false registered=true", gamma)
	}

	beta := items[2]
	if beta.registered {
		t.Fatalf("beta registered = true, want false (candidate carried Registered=false)")
	}
}

// TestMergeHostItemsRegisteredByFQDN proves an FQDN registration folds into its
// discovered short-label row instead of appending a duplicate registered-only row,
// so a mesh that adopted FQDN dialing does not double-list its own peers.
func TestMergeHostItemsRegisteredByFQDN(t *testing.T) {
	cands := []hostregistry.HostCandidate{
		{Node: "beta", DefaultTarget: "yasyf@beta.tailnet.ts.net", Source: "tailscale", Online: true},
	}
	registered := []string{"yasyf@beta.tailnet.ts.net"}

	items := mergeHostItems(cands, registered)

	if len(items) != 1 {
		t.Fatalf("got %d items, want 1 (FQDN registration must not duplicate the beta row): %+v", len(items), items)
	}
	if items[0].node != "beta" {
		t.Fatalf("row node = %q, want beta", items[0].node)
	}
}

// TestMergeHostItemsRegisteredSortFirst feeds a deliberately interleaved candidate
// set (unregistered, registered, unregistered, registered) plus a registered-only
// host and asserts every registered row sorts ahead of every discovered-only one,
// while order within each group stays the stable assembly order.
func TestMergeHostItemsRegisteredSortFirst(t *testing.T) {
	cands := []hostregistry.HostCandidate{
		{Node: "aaa", DefaultTarget: "yasyf@aaa", Source: "bonjour", Registered: false},
		{Node: "bbb", DefaultTarget: "yasyf@bbb", Source: "tailscale", Registered: true},
		{Node: "ccc", DefaultTarget: "yasyf@ccc", Source: "bonjour", Registered: false},
		{Node: "ddd", DefaultTarget: "yasyf@ddd", Source: "tailscale", Registered: true},
	}
	// zzz is registered but undiscovered; it appends last yet must still sort into
	// the registered group.
	registered := []string{"yasyf@bbb", "yasyf@ddd", "yasyf@zzz"}

	items := mergeHostItems(cands, registered)

	gotTargets := make([]string, len(items))
	gotReg := make([]bool, len(items))
	for i, it := range items {
		gotTargets[i] = it.target
		gotReg[i] = it.registered
	}

	// bbb, ddd (discovered+registered, in candidate order), then zzz (registered-
	// only, appended), then aaa, ccc (discovered-only, in candidate order).
	wantTargets := []string{"yasyf@bbb", "yasyf@ddd", "yasyf@zzz", "yasyf@aaa", "yasyf@ccc"}
	wantReg := []bool{true, true, true, false, false}

	if !slices.Equal(gotTargets, wantTargets) {
		t.Fatalf("targets = %v, want %v", gotTargets, wantTargets)
	}
	if !slices.Equal(gotReg, wantReg) {
		t.Fatalf("registered flags = %v, want %v", gotReg, wantReg)
	}

	// Guard the invariant directly: no unregistered row may precede a registered one.
	for i := 1; i < len(items); i++ {
		if !items[i-1].registered && items[i].registered {
			t.Fatalf("unregistered row %q precedes registered row %q", items[i-1].target, items[i].target)
		}
	}
}

// seedMesh points the shared mesh at a fresh temp config dir and persists the
// given hosts as registered peers, so newHostsModel seeds from a real on-disk
// state.json rather than a mocked registry.
func seedMesh(t *testing.T, hosts ...string) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, identity := range hosts {
		fact, err := hostregistry.NewSSHHostFact(identity, "/opt/homebrew/bin/synckitd", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := hostregistry.Mesh.RegisterHost(context.Background(), fact); err != nil {
			t.Fatalf("seed mesh: %v", err)
		}
	}
}

// findItem returns the row with the given target from the model's canonical
// slice, failing the test when it is absent.
func findItem(t *testing.T, m hostsModel, target string) hostItem {
	t.Helper()
	for _, raw := range m.allItems {
		if it := raw.(hostItem); it.target == target {
			return it
		}
	}
	t.Fatalf("target %q not in allItems (%d rows)", target, len(m.allItems))
	return hostItem{}
}

// TestHostsSeedsFromRegisteredMeshBeforeDiscovery proves the stale-while-
// revalidate seed: a model built over a registry with a known host paints that
// host row immediately, never the full-screen "Discovering hosts…" takeover,
// before any discovery result arrives.
func TestHostsSeedsFromRegisteredMeshBeforeDiscovery(t *testing.T) {
	seedMesh(t, "yasyf@alpha")

	m := newHostsModel(Options{Runner: fakeRunner{}})
	if m.loading {
		t.Fatal("seeded model loading = true, want false (a known host must paint without the cold spinner)")
	}

	// Size the screen exactly as the router does on startup; this is what pushes
	// the seeded rows into the list so View renders them.
	s, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	m = s.(hostsModel)

	view := m.View()
	if strings.Contains(view, "Discovering hosts") {
		t.Fatalf("seeded View rendered the cold loading screen:\n%s", view)
	}
	if !strings.Contains(view, "yasyf@alpha") {
		t.Fatalf("seeded View missing the registered host row:\n%s", view)
	}

	// The seeded row sits in …checking until verify resolves it.
	if got := findItem(t, m, "yasyf@alpha").state; got != verifyChecking {
		t.Fatalf("seeded host state = %v, want verifyChecking", got)
	}
}

// TestHostsLoadedPreservesResolvedVerifyState proves the merge half: once a
// seeded host has resolved to a verify state, a later discovery pass that re-
// lists it (plus a freshly-discovered host) must carry that state over instead
// of resetting it to …checking, while the new host appears.
func TestHostsLoadedPreservesResolvedVerifyState(t *testing.T) {
	seedMesh(t, "yasyf@alpha")

	m := newHostsModel(Options{Runner: fakeRunner{}})
	s, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	m = s.(hostsModel)

	// Resolve alpha to ✓ via the real verify message path.
	s, _ = m.Update(hostVerifiedMsg{target: "yasyf@alpha", res: hostregistry.VerifyResult{Reachable: true, Bootstrapped: true, Version: "1.2.3"}})
	m = s.(hostsModel)
	if got := findItem(t, m, "yasyf@alpha").state; got != verifyOK {
		t.Fatalf("after verify, alpha state = %v, want verifyOK", got)
	}

	// A discovery pass re-lists alpha (registered) and surfaces beta (new, never
	// probed). The merge must preserve alpha's resolved state and version.
	s, _ = m.Update(hostsLoadedMsg{items: []hostItem{
		{node: "alpha", target: "yasyf@alpha", source: "tailscale", online: true, registered: true},
		{node: "beta", target: "yasyf@beta", source: "bonjour", online: true, registered: false},
	}})
	m = s.(hostsModel)

	alpha := findItem(t, m, "yasyf@alpha")
	if alpha.state != verifyOK {
		t.Fatalf("after discovery merge, alpha state = %v, want verifyOK (resolved row must not flash back to …checking)", alpha.state)
	}
	if alpha.verify.Version != "1.2.3" {
		t.Fatalf("after discovery merge, alpha version = %q, want %q (verify result must carry over)", alpha.verify.Version, "1.2.3")
	}

	beta := findItem(t, m, "yasyf@beta")
	if beta.state != verifyUnknown {
		t.Fatalf("freshly-discovered beta state = %v, want verifyUnknown (no probe carried over)", beta.state)
	}

	if m.refreshing {
		t.Fatal("refreshing = true after hostsLoadedMsg, want false (the in-flight indicator must clear when a pass returns)")
	}
}
