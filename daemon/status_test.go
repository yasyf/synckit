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

	command := newStatusCmd()
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
