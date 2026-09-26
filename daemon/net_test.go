package daemon

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

var netObservedAt = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

type fakeNetMonitor struct {
	states  []netpolicy.State
	calls   int
	changed chan struct{}
}

func (m *fakeNetMonitor) Current() (netpolicy.State, <-chan struct{}) {
	state := m.states[min(m.calls, len(m.states)-1)]
	m.calls++
	return state, m.changed
}

func (*fakeNetMonitor) Close() error { return nil }

func closedChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func useNetMesh(t *testing.T, local netpolicy.State, peers map[string]syncservice.Transport) {
	t.Helper()
	shortDaemonHome(t)
	useMesh(t)
	ctx := context.Background()
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hostregistry.Mesh.Update(ctx, func(g *hostregistry.Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"a@one", "b@two"} {
		fact, err := hostregistry.NewSSHHostFact(identity, "/opt/homebrew/bin/synckitd", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := hostregistry.Mesh.RegisterHost(ctx, fact); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest(t, "cc-sync")

	prevMonitor, prevDial := newNetMonitor, dialTransport
	newNetMonitor = func(string) (netpolicy.Monitor, error) {
		return &fakeNetMonitor{states: []netpolicy.State{local}, changed: make(chan struct{})}, nil
	}
	dialTransport = func(_ processScope, m manifest.Manifest, peer, self string) syncservice.Transport {
		transport, ok := peers[peer]
		if !ok || m.Name != "cc-sync" || self != "me@self" {
			t.Errorf("dialTransport(%s, peer %s, self %s)", m.Name, peer, self)
			return synctransport.Failed(errString("unexpected dial"))
		}
		return transport
	}
	t.Cleanup(func() { newNetMonitor, dialTransport = prevMonitor, prevDial })
}

func netStatusPeer(state netpolicy.State) syncservice.Transport {
	d := rpc.NewDispatcher()
	d.Register(artifact.MethodNetStatus, func(context.Context, map[string]any) (any, error) {
		return artifact.NetStatusResult{State: state}, nil
	})
	return directSyncTransport{dispatcher: d}
}

func TestNetStatusCommand(t *testing.T) {
	local := netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: netObservedAt}
	cellular := netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, Expensive: true, ObservedAt: netObservedAt}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "json",
			args: []string{"status", "--json"},
			want: `{
  "local": {
    "status": "connected",
    "expensive": false,
    "constrained": false,
    "cellular": false,
    "manual_metered": false,
    "observed_at": "2026-09-26T12:00:00Z"
  },
  "peers": [
    {
      "host": "a@one",
      "service": "cc-sync",
      "state": {
        "status": "connected",
        "expensive": true,
        "constrained": false,
        "cellular": true,
        "manual_metered": false,
        "observed_at": "2026-09-26T12:00:00Z"
      },
      "verdict": {
        "allowed": false,
        "reason": "remote: cellular"
      }
    },
    {
      "host": "b@two",
      "error": "cc-sync: ssh: connection refused"
    }
  ]
}
`,
		},
		{
			name: "text",
			args: []string{"status"},
			want: "local: connected\n" +
				"peer a@one: connected, cellular, expensive via cc-sync: paused (remote: cellular)\n" +
				"peer b@two: unreachable: cc-sync: ssh: connection refused\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useNetMesh(t, local, map[string]syncservice.Transport{
				"a@one": netStatusPeer(cellular),
				"b@two": synctransport.Failed(errString("ssh: connection refused")),
			})
			cmd := newNetCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)
			if err := cmd.ExecuteContext(t.Context()); err != nil {
				t.Fatalf("net %v: %v", tt.args, err)
			}
			if out.String() != tt.want {
				t.Fatalf("net %v printed\n%s\nwant\n%s", tt.args, out.String(), tt.want)
			}
		})
	}
}

func TestNetStatusAllowsUnrestrictedPeer(t *testing.T) {
	local := netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: netObservedAt}
	useNetMesh(t, local, map[string]syncservice.Transport{
		"a@one": netStatusPeer(local),
		"b@two": netStatusPeer(netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}),
	})
	cmd := newNetCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"status"})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := "local: connected\n" +
		"peer a@one: connected via cc-sync: allowed\n" +
		"peer b@two: connected, manual-metered via cc-sync: paused (remote: manual metered)\n"
	if out.String() != want {
		t.Fatalf("net status printed %q, want %q", out.String(), want)
	}
}

func TestFirstObservation(t *testing.T) {
	unobserved := netpolicy.State{Status: netpolicy.StatusUnknown}
	observed := netpolicy.State{Status: netpolicy.StatusConnected, Constrained: true, ObservedAt: netObservedAt}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		states  []netpolicy.State
		changed chan struct{}
		want    netpolicy.State
		calls   int
	}{
		{"already observed", context.Background(), []netpolicy.State{observed}, make(chan struct{}), observed, 1},
		{"waits for the first path update", context.Background(), []netpolicy.State{unobserved, observed}, closedChannel(), observed, 2},
		{"canceled before any update", canceled, []netpolicy.State{unobserved}, make(chan struct{}), unobserved, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := &fakeNetMonitor{states: tt.states, changed: tt.changed}
			if got := firstObservation(tt.ctx, monitor); got != tt.want || monitor.calls != tt.calls {
				t.Fatalf("firstObservation() = %+v after %d samples, want %+v after %d", got, monitor.calls, tt.want, tt.calls)
			}
		})
	}
}

func TestNetMeteredCommand(t *testing.T) {
	useMesh(t)
	path, err := netpolicy.ManualPath()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		arg     string
		want    netpolicy.Manual
		printed string
	}{
		{"on", netpolicy.Manual{Metered: true}, "metered: on\n"},
		{"off", netpolicy.Manual{Metered: false}, "metered: off\n"},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			cmd := newNetCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetArgs([]string{"metered", tt.arg})
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			got, err := netpolicy.LoadManual(path)
			if err != nil || got != tt.want || out.String() != tt.printed {
				t.Fatalf("metered %s: saved %+v, %v; printed %q", tt.arg, got, err, out.String())
			}
		})
	}
	for _, args := range [][]string{{"metered"}, {"metered", "maybe"}, {"metered", "on", "off"}} {
		cmd := newNetCmd()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatalf("net %v succeeded", args)
		}
	}
	if got, err := netpolicy.LoadManual(path); err != nil || got.Metered {
		t.Fatalf("refused metered args changed the setting to %+v, %v", got, err)
	}
}

func TestRootWiresNetCommands(t *testing.T) {
	root := newRoot("test")
	for _, path := range [][]string{{"net", "status"}, {"net", "metered"}} {
		cmd, _, err := root.Find(path)
		if err != nil || cmd.Name() != path[1] || cmd.Parent().Name() != "net" {
			t.Fatalf("root.Find(%v) = %v, %v", path, cmd, err)
		}
	}
	status, _, err := root.Find([]string{"net", "status"})
	if err != nil || status.Flags().Lookup("json") == nil {
		t.Fatalf("net status lacks --json: %v", err)
	}
}
