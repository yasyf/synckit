package daemon

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/paths"

	"github.com/yasyf/synckit/hostregistry"
)

// TestStatusPrintsTheLabelDerivedSockets pins the one place an operator reads a
// socket path: no manifest declares one any more, so status must derive both
// the daemon's own and each resident helper's from their labels.
func TestStatusPrintsTheLabelDerivedSockets(t *testing.T) {
	shortDaemonHome(t)
	useMesh(t)
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureManifestsDir(); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, "cookiesync")

	command := newStatusCmd("test")
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetErr(&out)
	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("status: %v", err)
	}

	daemonSocket, err := paths.Socket(serveLabel)
	if err != nil {
		t.Fatal(err)
	}
	helperSocket, err := residentSocket("cookiesync")
	if err != nil {
		t.Fatal(err)
	}
	printed := out.String()
	for _, want := range []string{
		"label: " + serveLabel + "\n",
		"socket: " + daemonSocket + "\n",
		"  socket: " + helperSocket + "\n",
	} {
		if !strings.Contains(printed, want) {
			t.Fatalf("status printed %q, want a line %q", printed, want)
		}
	}
}

func TestDaemonLineFlagsADaemonOnAnotherBuild(t *testing.T) {
	for _, tc := range []struct {
		name, installed, running, want string
	}{
		{
			name: "current", installed: "0.44.0", running: "v0.44.0",
			want: "daemon: running v0.44.0",
		},
		{
			name: "stale", installed: "0.44.0", running: "0.41.2",
			want: "daemon: running 0.41.2, but the installed synckitd is 0.44.0; the next reconcile tick restages it, or run synckitd install",
		},
		{
			name: "unversioned", installed: "0.44.0", running: "",
			want: "daemon: running an unversioned build, but the installed synckitd is 0.44.0; the next reconcile tick restages it, or run synckitd install",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := daemonLine(tc.installed, tc.running); got != tc.want {
				t.Fatalf("daemonLine(%q, %q) = %q, want %q", tc.installed, tc.running, got, tc.want)
			}
		})
	}
}
