package tui

import (
	"bytes"
	"context"
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
