package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

// stubConsumer writes an executable shell script to dir that, for the agreed
// action contract, prints a fixed `list --json` payload and appends every other
// invocation's argv to a record file. It returns the script path and the record
// path so a test can assert which actions were shelled.
func stubConsumer(t *testing.T, dir string) (binary, recordPath string) {
	t.Helper()
	recordPath = filepath.Join(dir, "record.log")
	binary = filepath.Join(dir, "consumer")
	script := `#!/bin/sh
case "$1" in
  list)
    echo '[{"id":"site-a","watch_dirs":["` + dir + `"],"fingerprint":"fp-a"},{"id":"site-b","watch_dirs":["` + dir + `"],"fingerprint":"fp-b"}]'
    ;;
  *)
    echo "$@" >> "` + recordPath + `"
    ;;
esac
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil { //nolint:gosec // test stub script must be executable
		t.Fatalf("write stub consumer: %v", err)
	}
	return binary, recordPath
}

func readRecord(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // G304: test reads back its own record file under t.TempDir.
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read record: %v", err)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func testManifest(binary string) manifest.Manifest {
	return manifest.Manifest{
		Name:   "stub",
		Binary: binary,
		Watch: manifest.WatchSpec{
			Backend:  "fsnotify",
			Debounce: codec.Duration(10 * time.Millisecond),
			ListCmd:  "list --json",
		},
		Actions: manifest.ActionSpec{
			Reconcile: "reconcile",
			Sync:      "sync --origin {{.Peer}}",
			Fetch:     "fetch",
			Apply:     "apply",
		},
	}
}

func TestSupervisorListItems(t *testing.T) {
	dir := t.TempDir()
	binary, _ := stubConsumer(t, dir)
	sup := newSupervisor(hostregistry.NewExecRunner())

	items, err := sup.listItems(context.Background(), testManifest(binary))
	if err != nil {
		t.Fatalf("listItems: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}
	if items[0].ID != "site-a" || items[0].Fingerprint != "fp-a" {
		t.Errorf("item[0] = %+v, want id=site-a fp=fp-a", items[0])
	}
	if items[1].ID != "site-b" || items[1].Fingerprint != "fp-b" {
		t.Errorf("item[1] = %+v, want id=site-b fp=fp-b", items[1])
	}
}

func TestManifestResolver(t *testing.T) {
	dir := t.TempDir()
	binary, _ := stubConsumer(t, dir)
	r := manifestResolver{runner: hostregistry.NewExecRunner(), m: testManifest(binary)}

	tests := []struct {
		name string
		id   string
		want string
	}{
		{"known a", "site-a", "fp-a"},
		{"known b", "site-b", "fp-b"},
		{"missing", "site-z", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Resolve(context.Background(), tt.id)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got != tt.want {
				t.Errorf("Resolve(%q) = %q, want %q", tt.id, got, tt.want)
			}
		})
	}
}

func TestManifestNotifierLocal(t *testing.T) {
	dir := t.TempDir()
	binary, record := stubConsumer(t, dir)
	n := manifestNotifier{runner: hostregistry.NewExecRunner(), m: testManifest(binary), self: "me@self"}

	if err := n.Notify(context.Background(), "me@self", "site-a"); err != nil {
		t.Fatalf("Notify local: %v", err)
	}
	got := readRecord(t, record)
	if len(got) != 1 {
		t.Fatalf("recorded %v, want 1 invocation", got)
	}
	// Self notify renders an empty Origin: "sync --origin {{.Peer}}" with Peer="".
	if got[0] != "sync --origin" {
		t.Errorf("local sync argv = %q, want %q", got[0], "sync --origin")
	}
}

func TestEngineEventShellsSync(t *testing.T) {
	dir := t.TempDir()
	binary, record := stubConsumer(t, dir)
	m := testManifest(binary)
	runner := hostregistry.NewExecRunner()

	eng := buildEngine(runner, m, &hostregistry.Registry{Self: "me@self"})

	ctx := context.Background()
	eng.OnEvent(ctx, "site-a")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(readRecord(t, record)) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := readRecord(t, record)
	if len(got) != 1 || got[0] != "sync --origin" {
		t.Fatalf("event shelled %v, want one local sync %q", got, "sync --origin")
	}
}

func TestManifestNotifierPeer(t *testing.T) {
	dir := t.TempDir()
	binary, _ := stubConsumer(t, dir)
	mock := hostregistry.NewMockRunner().DefaultSSH("", nil)
	n := manifestNotifier{runner: mock, m: testManifest(binary), self: "me@self"}

	if err := n.Notify(context.Background(), "peer@node", "site-a"); err != nil {
		t.Fatalf("Notify peer: %v", err)
	}
	cmds := mock.SSHCmds("peer@node")
	if len(cmds) != 1 {
		t.Fatalf("ssh cmds = %v, want 1", cmds)
	}
	// Peer notify renders Origin=self and shell-quotes each argv field.
	want := binary + " 'sync' '--origin' 'peer@node'"
	if cmds[0] != want {
		t.Errorf("ssh sync cmd = %q, want %q", cmds[0], want)
	}
}
