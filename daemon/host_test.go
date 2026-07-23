package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
)

// TestHostAddrAddRecordsDialAddr proves `host addr add` records an alternate dial
// address that DialAddrs then returns ahead of the target's own FQDN.
func TestHostAddrAddRecordsDialAddr(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}

	cmd := newHostCmd()
	cmd.SetArgs([]string{"addr", "add", "me@node.tail.ts.net", "me@node.local"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("host addr add: %v", err)
	}

	got, err := hostregistry.DialAddrs("me@node.tail.ts.net")
	if err != nil {
		t.Fatalf("DialAddrs: %v", err)
	}
	want := []string{"me@node.local", "me@node.tail.ts.net"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("DialAddrs = %v, want %v", got, want)
	}
}

func TestHostLsJSONShape(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := hostregistry.Mesh.Update(context.Background(), func(g *hostregistry.Registry) error {
		g.Self = "me@self"
		g.UpsertHost("a@one")
		g.UpsertHost("b@two")
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}

	cmd := newHostLsCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("host ls --json: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json output %q: %v", out.String(), err)
	}
	if _, ok := decoded["self"]; !ok {
		t.Errorf("json output missing %q key: %v", "self", decoded)
	}
	if _, ok := decoded["hosts"]; !ok {
		t.Errorf("json output missing %q key: %v", "hosts", decoded)
	}
	if got := decoded["self"]; got != "me@self" {
		t.Errorf("self = %v, want me@self", got)
	}
	hosts, ok := decoded["hosts"].([]any)
	if !ok || len(hosts) != 2 || hosts[0] != "a@one" || hosts[1] != "b@two" {
		t.Errorf("hosts = %v, want [a@one b@two]", decoded["hosts"])
	}
	// Exactly the two consumer-shimmed keys, no extras.
	if len(decoded) != 2 {
		t.Errorf("json keys = %v, want exactly {self, hosts}", decoded)
	}
}
