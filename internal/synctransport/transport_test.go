package synctransport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

func TestSpawnedCommandSealsTheChildEnvironment(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := spawnedCommand("/usr/bin/ssh", []string{"-T", "peer"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cmd.Env, []string{"HOME=" + home}) {
		t.Fatalf("env = %q, want only HOME", cmd.Env)
	}
	if cmd.Dir != filepath.Dir("/usr/bin/ssh") || !cmd.Session {
		t.Fatalf("dir = %q session = %t", cmd.Dir, cmd.Session)
	}
	if cmd.Exec != daemonkit.ServingSameUser() {
		t.Fatal("spawned command states no same-user exec posture")
	}
}

func TestSpawnedCommandRejectsEmptyArgv(t *testing.T) {
	if _, err := spawnedCommand("", nil); err == nil {
		t.Fatal("spawnedCommand accepted an empty executable")
	}
}

func TestStderrErrorMarksTruncation(t *testing.T) {
	process := &spawnedProcess{stderr: daemonkit.NewCapture(4)}
	if err := process.stderrError(); err != nil {
		t.Fatalf("empty stderr = %v, want nil", err)
	}
	if _, err := process.stderr.Write([]byte("boom boom")); err != nil {
		t.Fatal(err)
	}
	err := process.stderrError()
	if err == nil || !strings.Contains(err.Error(), "(truncated)") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("stderr error = %v", err)
	}
}

// TestSpawnedTransportRoundTripsOverTheHandoff proves both halves of the sealed
// local lane against a real child: the parent spawns on ChannelHandoff and
// attaches Child.Business, the child claims fd 3 through ServeSpawned, and the
// two adopt the one Contract the spawn conveyed.
func TestSpawnedTransportRoundTripsOverTheHandoff(t *testing.T) {
	stub := buildStub(t, "spawnstub")
	owned := testOwned(t)
	transport := NewSpawned(owned, stub, "stub")
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	response, err := transport.Do(ctx, &rpc.Request{Method: "echo", Params: map[string]any{"say": "pong"}})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if !response.OK {
		t.Fatalf("response = %+v", response)
	}
	var said string
	if err := json.Unmarshal(response.Result, &said); err != nil {
		t.Fatal(err)
	}
	if said != "pong" {
		t.Fatalf("result = %q, want %q", said, "pong")
	}
}

// TestRemoteTransportFailsOverWhenTheLaneNeverOpens covers the failure failover
// exists for: the first address is unreachable, so no lane opens and no
// daemonkit call error exists to classify — the advance must still happen.
func TestRemoteTransportFailsOverWhenTheLaneNeverOpens(t *testing.T) {
	fact, err := hostregistry.NewSSHHostFact("sync@primary.example", "/usr/local/bin/synckitd", []string{"secondary.example"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fact.Addresses) != 2 {
		t.Fatalf("addresses = %q, want two dial addresses", fact.Addresses)
	}
	spawner := &refusingSpawner{err: errors.New("ssh never spawned")}
	transport := NewRemote(spawner, fact, writeKnownHosts(t), "stub")
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for attempt := range 2 {
		if _, err := transport.Do(ctx, &rpc.Request{Method: "status"}); err == nil {
			t.Fatalf("attempt %d succeeded against a spawner that never spawns", attempt)
		}
	}
	if !slices.Equal(spawner.dialed, fact.Addresses) {
		t.Fatalf("dialed %q, want %q — an unreachable first address must not be retried", spawner.dialed, fact.Addresses)
	}
}

// TestRemoteDialClosesTheConnWhenTheHelloNeverVerifies proves a failed handshake
// releases the stdio conn Child.Conn hands over for good: an empty known_hosts
// makes every ssh session die at host-key verification, and repeating it must
// not accumulate parent-side descriptors.
func TestRemoteDialClosesTheConnWhenTheHelloNeverVerifies(t *testing.T) {
	if _, err := os.Stat("/usr/bin/ssh"); err != nil {
		t.Skip("ssh not installed")
	}
	fact, err := hostregistry.NewSSHHostFact("sync@127.0.0.1", "/usr/local/bin/synckitd", nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := NewRemote(testOwned(t), fact, writeKnownHosts(t), "stub")
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	request := &rpc.Request{Method: "status"}
	if _, err := transport.Do(ctx, request); err == nil {
		t.Fatal("Do succeeded against an unverifiable host key")
	}
	// A leaked conn's descriptors close on the os.File finalizer, so a GC pass
	// would report a leak as clean. Nothing collects while the count is taken.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	before := openDescriptors(t)
	const attempts = 6
	for attempt := range attempts {
		if _, err := transport.Do(ctx, request); err == nil {
			t.Fatalf("attempt %d succeeded against an unverifiable host key", attempt)
		}
	}
	if after := openDescriptors(t); after > before {
		t.Fatalf("%d failed handshakes left %d extra descriptors open", attempts, after-before)
	}
}

// refusingSpawner records the address each dial reached for and starts nothing.
type refusingSpawner struct {
	err    error
	dialed []string
}

func (s *refusingSpawner) Spawn(
	_ context.Context, cmd daemonkit.Cmd, _ daemonkit.Channel, _ io.Writer,
) (*daemonkit.Child, error) {
	s.dialed = append(s.dialed, cmd.Args[len(cmd.Args)-2])
	return nil, s.err
}

// writeKnownHosts returns an empty mode-0600 known_hosts under a symlink-free
// directory, the two things the ssh contract validates before it builds argv.
func writeKnownHosts(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// openDescriptors counts this process's open descriptors. It reads names only:
// /dev/fd lists the directory's own descriptor, which is already closed by the
// time a stat of the entry would run.
func openDescriptors(t *testing.T) int {
	t.Helper()
	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	return len(names)
}

func testOwned(t *testing.T) *daemonkit.Owned {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owned, err := daemonkit.OwnProcesses(ctx, filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatalf("own processes: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := owned.Close(closeCtx); err != nil {
			t.Errorf("close process scope: %v", err)
		}
	})
	return owned
}

func buildStub(t *testing.T, pkg string) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), pkg)
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/"+pkg) //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	return bin
}
