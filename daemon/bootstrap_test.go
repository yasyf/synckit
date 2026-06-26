package daemon

import (
	"context"
	"strings"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

func TestAddHostStepSequence(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "", nil).   // synckitd not installed
		OnSSH("command -v cookiesync", "", nil). // consumer not installed
		OnSSH("brew install yasyf/tap/synckitd", "", nil).
		OnSSH("brew install yasyf/tap/cookiesync", "", nil).
		DefaultSSH("", nil)

	manifests := []manifest.Manifest{{
		Name:   "cookiesync",
		Binary: "cookiesync",
		Brew:   "yasyf/tap/cookiesync",
		Watch:  manifest.WatchSpec{Backend: "fsnotify"},
	}}

	var steps []string
	err := AddHost(context.Background(), mock, manifests, "peer@node", "me@self", false, func(s string) {
		steps = append(steps, s)
	})
	if err != nil {
		t.Fatalf("AddHost: %v", err)
	}

	// The mesh records the peer and self.
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		t.Fatalf("load mesh: %v", err)
	}
	if reg.Self != "me@self" {
		t.Errorf("mesh self = %q, want me@self", reg.Self)
	}
	if len(reg.Hosts) != 1 || reg.Hosts[0] != "peer@node" {
		t.Errorf("mesh hosts = %v, want [peer@node]", reg.Hosts)
	}

	// The remote bootstrap ran in order: ensure daemon, ensure consumer,
	// inverse-register, reconcile, install.
	cmds := mock.SSHCmds("peer@node")
	wantContains := []string{
		"command -v synckitd",
		"brew install yasyf/tap/synckitd",
		"command -v cookiesync",
		"brew install yasyf/tap/cookiesync",
		"synckitd host add me@self --no-recurse",
		"synckitd reconcile",
		"synckitd install",
	}
	assertOrderedContains(t, cmds, wantContains)
}

func TestAddHostNoRecurse(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	mock := hostregistry.NewMockRunner().DefaultSSH("", nil)

	err := AddHost(context.Background(), mock, nil, "peer@node", "me@self", true, nil)
	if err != nil {
		t.Fatalf("AddHost no-recurse: %v", err)
	}
	if cmds := mock.SSHCmdsAll(); len(cmds) != 0 {
		t.Errorf("no-recurse ran ssh %v, want none", cmds)
	}
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		t.Fatalf("load mesh: %v", err)
	}
	if reg.Self != "me@self" || len(reg.Hosts) != 1 {
		t.Errorf("mesh = %+v, want self=me@self hosts=[peer@node]", reg)
	}
}

func TestAddHostConsumerAlreadyInstalled(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd", nil).
		OnSSH("command -v cookiesync", "/opt/homebrew/bin/cookiesync", nil).
		DefaultSSH("", nil)

	manifests := []manifest.Manifest{{
		Name: "cookiesync", Binary: "cookiesync", Brew: "yasyf/tap/cookiesync",
		Watch: manifest.WatchSpec{Backend: "fsnotify"},
	}}

	if err := AddHost(context.Background(), mock, manifests, "peer@node", "me@self", false, nil); err != nil {
		t.Fatalf("AddHost: %v", err)
	}
	for _, c := range mock.SSHCmdsAll() {
		if strings.Contains(c, "brew install") {
			t.Errorf("brew install ran for an already-installed peer: %q", c)
		}
	}
}

// assertOrderedContains checks that, scanning got once front-to-back, each want
// substring is found in order (later wants may not precede earlier matches).
func assertOrderedContains(t *testing.T, got []string, want []string) {
	t.Helper()
	i := 0
	for _, g := range got {
		if i < len(want) && strings.Contains(g, want[i]) {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("ssh cmds %v\ndid not contain in order: %v (matched %d/%d)", got, want, i, len(want))
	}
}
