package tui

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/exp/teatest"
)

// fakeRunner satisfies hostregistry.Runner with canned, network-free replies: Local
// answers the tailscale and id probes host discovery makes; SSH always succeeds
// silently so verify never reaches the network.
type fakeRunner struct{}

const fakeTailscaleJSON = `{
  "Self": {"DNSName": "self.tailnet.ts.net.", "HostName": "self", "Online": true},
  "Peer": {
    "key-alpha": {"DNSName": "alpha.tailnet.ts.net.", "HostName": "alpha", "Online": true,  "OS": "linux"},
    "key-beta":  {"DNSName": "beta.tailnet.ts.net.",  "HostName": "beta",  "Online": false, "OS": "macOS"}
  }
}`

func (fakeRunner) Local(_ context.Context, name string, _ ...string) (string, error) {
	switch name {
	case "tailscale":
		return fakeTailscaleJSON, nil
	case "id":
		return "yasyf\n", nil
	}
	return "", nil
}

func (fakeRunner) SSH(_ context.Context, _, _ string) (string, error) { return "", nil }

// stubScreen is a minimal content tab: it renders a fixed marker so a test can
// confirm the router mounts and routes to a consumer-provided screen.
type stubScreen struct {
	title  string
	marker string
}

func (s stubScreen) Init() tea.Cmd                    { return nil }
func (s stubScreen) Update(tea.Msg) (Screen, tea.Cmd) { return s, nil }
func (s stubScreen) View() string                     { return s.marker }
func (s stubScreen) Title() string                    { return s.title }
func (s stubScreen) Help() []key.Binding              { return nil }
func (s stubScreen) WantsKey(tea.KeyMsg) bool         { return false }

// heightScreen fills exactly the inner height it was last handed via
// WindowSizeMsg and advertises a fixed set of help bindings, so a router test can
// prove the composed view fits the terminal as the help bar grows.
type heightScreen struct {
	title    string
	h        int
	bindings []key.Binding
}

func (s heightScreen) Init() tea.Cmd { return nil }

func (s heightScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	if ws, ok := msg.(tea.WindowSizeMsg); ok {
		s.h = ws.Height
	}
	return s, nil
}

func (s heightScreen) View() string {
	rows := make([]string, s.h)
	for i := range rows {
		rows[i] = "x"
	}
	return strings.Join(rows, "\n")
}

func (s heightScreen) Title() string            { return s.title }
func (s heightScreen) Help() []key.Binding      { return s.bindings }
func (s heightScreen) WantsKey(tea.KeyMsg) bool { return false }

// confirmScreen fills its handed inner height like heightScreen, but flips a
// confirm flag through the WantsKey path when it sees toggleKey and shrinks its
// Help() to the confirmed set while the flag is on. It models a screen whose
// footer height changes on an internal state shift, with no WindowSizeMsg to
// prompt a reflow.
type confirmScreen struct {
	title     string
	h         int
	full      []key.Binding
	confirmed []key.Binding
	toggleKey string
	confirm   bool
}

func (s confirmScreen) Init() tea.Cmd { return nil }

func (s confirmScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		s.h = msg.Height
	case tea.KeyMsg:
		if msg.String() == s.toggleKey {
			s.confirm = !s.confirm
		}
	}
	return s, nil
}

func (s confirmScreen) View() string {
	rows := make([]string, s.h)
	for i := range rows {
		rows[i] = "x"
	}
	return strings.Join(rows, "\n")
}

func (s confirmScreen) Title() string { return s.title }

func (s confirmScreen) Help() []key.Binding {
	if s.confirm {
		return s.confirmed
	}
	return s.full
}

func (s confirmScreen) WantsKey(msg tea.KeyMsg) bool { return msg.String() == s.toggleKey }

// fixedBindings builds n distinct help bindings so a heightScreen advertises a
// known expanded-help height.
func fixedBindings(n int) []key.Binding {
	out := make([]key.Binding, n)
	for i := range out {
		k := string(rune('a' + i))
		out[i] = key.NewBinding(key.WithKeys(k), key.WithHelp(k, k+" action"))
	}
	return out
}

// hermeticOptions points the shared mesh at a fresh temp config dir so detectSelf
// and host discovery never touch the real registry, and wires in the fake runner
// behind a single stub content screen.
func hermeticOptions(t *testing.T) Options {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	return Options{
		Brand:   "synckit",
		Screens: []Screen{stubScreen{title: "Content", marker: "content body"}},
		Runner:  fakeRunner{},
	}
}

// waitForContent fails the test unless every substr appears in the model output.
// WaitFor drains the shared output buffer as it reads, so content that renders
// only once must be asserted in a single call: chaining one WaitFor per
// substring would make later calls block on frames that already scrolled past.
func waitForContent(t *testing.T, tm *teatest.TestModel, substrs ...string) {
	t.Helper()
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
		for _, s := range substrs {
			if !bytes.Contains(b, []byte(s)) {
				return false
			}
		}
		return true
	}, teatest.WithDuration(5*time.Second), teatest.WithCheckInterval(20*time.Millisecond))
}
