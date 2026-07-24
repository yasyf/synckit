package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

// stubLocalNodes replaces the live bonjour browse with a fixed node list, so AddHost
// tests neither wait on a 3s mDNS browse nor pick up real LAN hosts.
func stubLocalNodes(t *testing.T, nodes ...string) {
	t.Helper()
	prev := localNodeDiscovery
	localNodeDiscovery = func(context.Context, string) ([]string, []hostregistry.SkipNote) {
		return nodes, nil
	}
	t.Cleanup(func() { localNodeDiscovery = prev })
}

func TestAddHostStepSequence(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	stubLocalNodes(t)

	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd", nil).
		OnSSH("command -v cookiesync", "", nil). // consumer not installed
		OnSSH("brew install yasyf/tap/cookiesync", "", nil).
		DefaultSSH("", nil)

	manifests := []manifest.Manifest{{
		Name:   "cookiesync",
		Binary: "cookiesync",
		Brew:   "yasyf/tap/cookiesync",
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
		"command -v cookiesync",
		"brew install yasyf/tap/cookiesync",
		"'/opt/homebrew/bin/synckitd' host add 'me@self' --no-recurse",
		"'/opt/homebrew/bin/synckitd' reconcile",
		"'/opt/homebrew/bin/synckitd' install",
	}
	assertOrderedContains(t, cmds, wantContains)
}

func TestAddHostNoRecurse(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	stubLocalNodes(t)
	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd", nil).
		DefaultSSH("", nil)

	err := AddHost(context.Background(), mock, nil, "peer@node", "me@self", true, nil)
	if err != nil {
		t.Fatalf("AddHost no-recurse: %v", err)
	}
	if cmds := mock.SSHCmdsAll(); len(cmds) != 1 || cmds[0] != "command -v synckitd" {
		t.Errorf("no-recurse ran ssh %v, want only exact path discovery", cmds)
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
	stubLocalNodes(t)
	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd", nil).
		OnSSH("command -v cookiesync", "/opt/homebrew/bin/cookiesync", nil).
		DefaultSSH("", nil)

	manifests := []manifest.Manifest{{
		Name: "cookiesync", Binary: "cookiesync", Brew: "yasyf/tap/cookiesync",
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

// TestAddHostRecordsBonjourLocalAddr proves that when bonjour sees the peer's node on
// the LAN, host add records its .local address as a dial candidate tried before the
// tailnet FQDN.
func TestAddHostRecordsBonjourLocalAddr(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	stubLocalNodes(t, "node")

	mock := hostregistry.NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd", nil).
		DefaultSSH("", nil)
	if err := AddHost(context.Background(), mock, nil, "peer@node.tail.ts.net", "me@self", true, nil); err != nil {
		t.Fatalf("AddHost: %v", err)
	}

	got, err := hostregistry.DialAddrs("peer@node.tail.ts.net")
	if err != nil {
		t.Fatalf("DialAddrs: %v", err)
	}
	want := []string{"peer@node.local", "peer@node.tail.ts.net"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("DialAddrs = %v, want %v (LAN .local first, tailnet last)", got, want)
	}
}

// TestRemoteBrewInstallSurfacesNoFormulaFromStdout proves remoteBrewInstall reads brew's
// missing-formula message off captured stdout — not only the error text — and turns it
// into the actionable "publish a goreleaser release" hint.
func TestRemoteBrewInstallSurfacesNoFormulaFromStdout(t *testing.T) {
	mock := hostregistry.NewMockRunner().
		OnSSH("brew install yasyf/tap/synckitd",
			`Error: No available formula with the name "yasyf/tap/synckitd".`,
			errors.New("ssh me@peer: exit status 1")).
		DefaultSSH("", nil)

	err := remoteBrewInstall(context.Background(), mock, "peer@node", "yasyf/tap/synckitd")
	if err == nil {
		t.Fatal("remoteBrewInstall succeeded, want the missing-formula error")
	}
	if !strings.Contains(err.Error(), "publish a goreleaser release") {
		t.Fatalf("error = %v, want the actionable no-formula hint derived from captured stdout", err)
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
