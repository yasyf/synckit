package hostregistry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func testTaskPool(t *testing.T) *supervise.Pool {
	t.Helper()
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")},
		Generation: t.Name(),
	}
	pool, err := supervise.NewPool(4, reaper)
	if err != nil {
		t.Fatalf("new process pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		pool.Cancel()
		if err := pool.Wait(context.Background()); err != nil {
			t.Errorf("wait for process pool: %v", err)
		}
	})
	return pool
}

// fakeSSH writes an executable ssh stand-in that logs the address it was invoked with
// (the argv before the wrapped remote command) to logPath, then exits by token: 255
// for a "connfail" address (ssh's own connection failure), 3 for a "remotefail"
// address (the remote command's own failure), 0 otherwise. It lets the failover tests
// script per-address outcomes without a real ssh or network.
func fakeSSH(t *testing.T, logPath string) string {
	t.Helper()
	script := "#!/bin/sh\n" +
		"a2=\"\"; a1=\"\"\n" +
		"for a in \"$@\"; do a2=\"$a1\"; a1=\"$a\"; done\n" +
		"printf '%s\\n' \"$a2\" >> \"" + logPath + "\"\n" +
		"case \"$a2\" in\n" +
		"  *connfail*) echo \"ssh: connect: Connection refused\" >&2; exit 255 ;;\n" +
		"  *remotefail*) echo \"remote boom\" >&2; exit 3 ;;\n" +
		"  *) printf 'ran-on:%s\\n' \"$a2\" ;;\n" +
		"esac\n"
	path := filepath.Join(t.TempDir(), "fake-ssh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	return path
}

func swapSSHBin(t *testing.T, bin string) {
	t.Helper()
	prev := sshBin
	sshBin = bin
	t.Cleanup(func() { sshBin = prev })
}

func attempts(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath) //nolint:gosec // G304: test reads a file it wrote in its own temp dir.
	if err != nil {
		t.Fatalf("read attempts log: %v", err)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// TestExecSSHTriesAddrsInOrderOn255 proves an ssh-level connection failure (exit 255)
// advances to the next dial address in order, and the first address that answers wins.
func TestExecSSHTriesAddrsInOrderOn255(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "attempts.log")
	swapSSHBin(t, fakeSSH(t, logPath))

	out, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@connfail-lan", "me@good-tailnet"}, "echo hi", nil)
	if err != nil {
		t.Fatalf("execSSHAddrs: %v", err)
	}
	if !strings.Contains(out, "good-tailnet") {
		t.Fatalf("stdout = %q, want it to name the address that answered", out)
	}
	tried := attempts(t, logPath)
	want := []string{"me@connfail-lan", "me@good-tailnet"}
	if strings.Join(tried, ",") != strings.Join(want, ",") {
		t.Fatalf("addresses tried = %v, want %v (255 fails over in order)", tried, want)
	}
}

// slowConnfailSSH writes a fake ssh whose connfail addresses sleep 300ms before exiting
// 255, so the dial-budget tests accumulate real elapsed time per dead address.
func slowConnfailSSH(t *testing.T, logPath string) string {
	t.Helper()
	script := "#!/bin/sh\n" +
		"a2=\"\"; a1=\"\"\n" +
		"for a in \"$@\"; do a2=\"$a1\"; a1=\"$a\"; done\n" +
		"printf '%s\\n' \"$a2\" >> \"" + logPath + "\"\n" +
		"case \"$a2\" in\n" +
		"  *connfail*) sleep 0.3; echo \"ssh: connect: Operation timed out\" >&2; exit 255 ;;\n" +
		"  *) printf 'ran-on:%s\\n' \"$a2\" ;;\n" +
		"esac\n"
	path := filepath.Join(t.TempDir(), "fake-ssh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	return path
}

// shrinkDialBudget points sshDialBudget at d so the budget tests run fast, restoring it
// on cleanup.
func shrinkDialBudget(t *testing.T, d time.Duration) {
	t.Helper()
	prev := sshDialBudget
	sshDialBudget = d
	t.Cleanup(func() { sshDialBudget = prev })
}

// TestExecSSHDialBudgetBoundsDeadAddressFailover proves the overall dial bound: with the
// budget spent after the first dead address (300ms attempt vs 250ms budget), the four
// remaining alternates are skipped and only the final canonical address is dialed.
func TestExecSSHDialBudgetBoundsDeadAddressFailover(t *testing.T) {
	shrinkDialBudget(t, 250*time.Millisecond)
	logPath := filepath.Join(t.TempDir(), "attempts.log")
	swapSSHBin(t, slowConnfailSSH(t, logPath))

	addrs := []string{"me@connfail-1", "me@connfail-2", "me@connfail-3", "me@connfail-4", "me@connfail-5", "me@connfail-final"}
	_, err := execSSHAddrs(context.Background(), testTaskPool(t), addrs, "echo hi", nil)
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the dial failure surfaced")
	}
	if !isConnFailure(err) {
		t.Fatalf("error = %v, want the final attempt's 255 surfaced", err)
	}
	tried := attempts(t, logPath)
	want := []string{"me@connfail-1", "me@connfail-final"}
	if strings.Join(tried, ",") != strings.Join(want, ",") {
		t.Fatalf("addresses tried = %v, want %v (spent budget skips to the final canonical address)", tried, want)
	}
}

// TestExecSSHDialBudgetStillDialsCanonicalTarget proves a spent budget skips only the
// recorded alternates, never the final canonical address: a host reachable at its tailnet
// FQDN still succeeds after dead LAN addresses exhaust the budget.
func TestExecSSHDialBudgetStillDialsCanonicalTarget(t *testing.T) {
	shrinkDialBudget(t, 250*time.Millisecond)
	logPath := filepath.Join(t.TempDir(), "attempts.log")
	swapSSHBin(t, slowConnfailSSH(t, logPath))

	out, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@connfail-1", "me@connfail-2", "me@connfail-3", "me@good-tailnet"}, "echo hi", nil)
	if err != nil {
		t.Fatalf("execSSHAddrs: %v (a spent budget must still dial the canonical target)", err)
	}
	if !strings.Contains(out, "good-tailnet") {
		t.Fatalf("stdout = %q, want the canonical address to have answered", out)
	}
	tried := attempts(t, logPath)
	want := []string{"me@connfail-1", "me@good-tailnet"}
	if strings.Join(tried, ",") != strings.Join(want, ",") {
		t.Fatalf("addresses tried = %v, want %v (spent budget jumps to the canonical address)", tried, want)
	}
}

// TestExecSSHRemoteFailureNeverFailsOver proves a remote command's own non-255 exit
// fails immediately and is never re-run against the next address.
func TestExecSSHRemoteFailureNeverFailsOver(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "attempts.log")
	swapSSHBin(t, fakeSSH(t, logPath))

	_, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@remotefail-lan", "me@good-tailnet"}, "echo hi", nil)
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the remote command's failure surfaced")
	}
	if !strings.Contains(err.Error(), "remote boom") {
		t.Fatalf("error = %v, want it to carry the remote stderr", err)
	}
	tried := attempts(t, logPath)
	if len(tried) != 1 || tried[0] != "me@remotefail-lan" {
		t.Fatalf("addresses tried = %v, want only [me@remotefail-lan] (non-255 never fails over)", tried)
	}
}

// TestExecSSHKillsBackgroundedDescendantOnCtxCancel proves daemonkit settles a
// pipe-holding descendant before a canceled task returns.
func TestExecSSHKillsBackgroundedDescendantOnCtxCancel(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	script := "#!/bin/sh\n" +
		"sleep 30 &\n" +
		"echo $! > " + pidFile + "\n" +
		"exit 255\n"
	bin := filepath.Join(t.TempDir(), "fake-ssh")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	swapSSHBin(t, bin)
	t.Cleanup(func() {
		if pid := readDescendantPID(pidFile); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := testTaskPool(t)
	done := make(chan error, 1)
	go func() {
		_, err := execSSHAddrs(ctx, pool, []string{"me@peer"}, "echo hi", nil)
		done <- err
	}()
	pid := waitDescendantPID(t, pidFile)
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("execSSHAddrs did not settle after cancellation")
	}
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the ctx-cancelled ssh failure")
	}

	for deadline := time.Now().Add(2 * time.Second); ; {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d still alive; ctx cancel must kill the whole process group", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestExecSSHCtxCancelWithExitZeroLeaderReturnsCtxErr reproduces the masked bug: a fake
// ssh leader backgrounds a pipe-holding descendant then exits 0, so os/exec's own Cancel
// never fires and Wait would return nil once our watcher reaps the descendant. The watcher
// must surface cancellation as a ctx.Err()-wrapped error (never a success), leave the whole
// process group dead, and — since a ctx error is not exit 255 — never fail over to the next
// address.
func TestExecSSHCtxCancelWithExitZeroLeaderReturnsCtxErr(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "descendant.pid")
	logPath := filepath.Join(dir, "attempts.log")
	script := "#!/bin/sh\n" +
		"a2=\"\"; a1=\"\"\n" +
		"for a in \"$@\"; do a2=\"$a1\"; a1=\"$a\"; done\n" +
		"printf '%s\\n' \"$a2\" >> \"" + logPath + "\"\n" +
		"sleep 30 &\n" +
		"echo $! > " + pidFile + "\n" +
		"exit 0\n"
	bin := filepath.Join(dir, "fake-ssh")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	swapSSHBin(t, bin)
	t.Cleanup(func() {
		if pid := readDescendantPID(pidFile); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := testTaskPool(t)
	done := make(chan error, 1)
	go func() {
		_, err := execSSHAddrs(ctx, pool, []string{"me@peer-a", "me@peer-b"}, "echo hi", nil)
		done <- err
	}()
	pid := waitDescendantPID(t, pidFile)
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("execSSHAddrs did not settle after cancellation")
	}
	if err == nil {
		t.Fatal("execSSHAddrs returned nil, want the cancelled op surfaced as an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want it to wrap context.Canceled", err)
	}
	if tried := attempts(t, logPath); len(tried) != 1 || tried[0] != "me@peer-a" {
		t.Fatalf("addresses tried = %v, want only [me@peer-a] (a ctx error must not fail over)", tried)
	}

	for deadline := time.Now().Add(2 * time.Second); ; {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d still alive; cancellation must kill the whole process group", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readDescendantPID reads the pid the fake ssh recorded, or 0 when the file is absent or
// not yet written.
func readDescendantPID(path string) int {
	data, err := os.ReadFile(path) //nolint:gosec // G304: test reads a file its own stub wrote in a temp dir.
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

func waitDescendantPID(t *testing.T, path string) int {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if pid := readDescendantPID(path); pid > 0 {
			return pid
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fake ssh never recorded its descendant pid")
	return 0
}

// TestExecSSHReapsSurvivingDescendantWhenLeaderExitsFirst proves a completed
// daemonkit task settles descendants before applying exit-255 failover.
func TestExecSSHReapsSurvivingDescendantWhenLeaderExitsFirst(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "descendant.pid")
	logPath := filepath.Join(dir, "attempts.log")
	script := "#!/bin/sh\n" +
		"a2=\"\"; a1=\"\"\n" +
		"for a in \"$@\"; do a2=\"$a1\"; a1=\"$a\"; done\n" +
		"printf '%s\\n' \"$a2\" >> \"" + logPath + "\"\n" +
		"case \"$a2\" in\n" +
		"  *connfail*) sleep 30 & echo $! > \"" + pidFile + "\"; echo 'ssh: connect: Connection refused' >&2; exit 255 ;;\n" +
		"  *) printf 'ran-on:%s\\n' \"$a2\" ;;\n" +
		"esac\n"
	bin := filepath.Join(dir, "fake-ssh")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	swapSSHBin(t, bin)
	t.Cleanup(func() {
		if pid := readDescendantPID(pidFile); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	out, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@connfail-lan", "me@good-tailnet"}, "echo hi", nil)
	if err != nil {
		t.Fatalf("execSSHAddrs: %v (a 255 leader must fail over to the next address)", err)
	}
	if !strings.Contains(out, "good-tailnet") {
		t.Fatalf("stdout = %q, want the second address to have answered", out)
	}
	tried := attempts(t, logPath)
	want := []string{"me@connfail-lan", "me@good-tailnet"}
	if strings.Join(tried, ",") != strings.Join(want, ",") {
		t.Fatalf("addresses tried = %v, want %v (255 fails over in order)", tried, want)
	}

	pid := readDescendantPID(pidFile)
	if pid <= 0 {
		t.Fatal("fake ssh never recorded its descendant pid")
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d still alive; the post-Wait group kill must reap it", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestExecSSHErrorTypedWithStderr proves ExecSSH surfaces a *SSHError with the captured
// stderr on both a terminal remote-exit failure and a ctx-kill failure, and that the error
// still unwraps to the exit cause failover keys off / the context error.
func TestExecSSHErrorTypedWithStderr(t *testing.T) {
	t.Run("remote exit", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "attempts.log")
		swapSSHBin(t, fakeSSH(t, logPath))

		_, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@remotefail-lan"}, "echo hi", nil)
		if err == nil {
			t.Fatal("execSSHAddrs succeeded, want the remote failure surfaced")
		}
		var sshErr *SSHError
		if !errors.As(err, &sshErr) {
			t.Fatalf("error %v (%T) is not a *SSHError", err, err)
		}
		if sshErr.Addr != "me@remotefail-lan" {
			t.Fatalf("SSHError.Addr = %q, want me@remotefail-lan", sshErr.Addr)
		}
		if !strings.Contains(sshErr.Stderr, "remote boom") {
			t.Fatalf("SSHError.Stderr = %q, want it to carry the remote stderr", sshErr.Stderr)
		}
		var ee *supervise.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("error %v does not unwrap to *supervise.ExitError; isConnFailure would misjudge failover", err)
		}
	})

	t.Run("ctx kill", func(t *testing.T) {
		dir := t.TempDir()
		bin := filepath.Join(dir, "fake-ssh")
		readyFile := filepath.Join(dir, "ready.pid")
		script := "#!/bin/sh\n" +
			"echo 'stderr diag before kill' >&2\n" +
			"echo $$ > \"" + readyFile + "\"\n" +
			"exec sleep 30\n"
		if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
			t.Fatalf("write fake ssh: %v", err)
		}
		swapSSHBin(t, bin)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pool := testTaskPool(t)
		done := make(chan error, 1)
		go func() {
			_, err := execSSHAddrs(ctx, pool, []string{"me@peer"}, "echo hi", nil)
			done <- err
		}()
		_ = waitDescendantPID(t, readyFile)
		cancel()
		var err error
		select {
		case err = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("execSSHAddrs did not settle after cancellation")
		}
		if err == nil {
			t.Fatal("execSSHAddrs succeeded, want the ctx-cancelled failure surfaced")
		}
		var sshErr *SSHError
		if !errors.As(err, &sshErr) {
			t.Fatalf("error %v (%T) is not a *SSHError", err, err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v does not unwrap to context.Canceled", err)
		}
		if !strings.Contains(sshErr.Stderr, "stderr diag before kill") {
			t.Fatalf("SSHError.Stderr = %q, want the captured stderr populated on the ctx-kill path", sshErr.Stderr)
		}
	})
}

func TestDialAddrsDefaultsToTarget(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	initializeTestState(t, Mesh)
	registerTestHost(t, Mesh, "me@node.tail.ts.net")
	got, err := DialAddrs("me@node.tail.ts.net")
	if err != nil {
		t.Fatalf("DialAddrs: %v", err)
	}
	if len(got) != 1 || got[0] != "me@node.tail.ts.net" {
		t.Fatalf("DialAddrs = %v, want just [me@node.tail.ts.net]", got)
	}
}

func TestDialAddrsLANFirstTailnetLast(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	initializeTestState(t, Mesh)
	target := "me@node.tail.ts.net"
	registerTestHost(t, Mesh, target, "me@node.local")
	got, err := DialAddrs(target)
	if err != nil {
		t.Fatalf("DialAddrs: %v", err)
	}
	want := []string{"me@node.local", target}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("DialAddrs = %v, want %v (LAN first, tailnet last)", got, want)
	}
}

func TestRegisterHostPersistsOneCanonicalFact(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	initializeTestState(t, Mesh)
	target := "peer@node.tail.ts.net"

	if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}
	registerTestHost(t, Mesh, target, "peer@node.local", "peer@node.local")
	reg, err := Mesh.Load()
	if err != nil {
		t.Fatal(err)
	}
	if reg.Self != "me@self" || len(reg.Hosts) != 1 || reg.Hosts[0] != target {
		t.Fatalf("registry changed across RegisterHost: %+v", reg)
	}

	byTarget, err := Mesh.LoadAddrs()
	if err != nil {
		t.Fatalf("LoadAddrs: %v", err)
	}
	if got := byTarget[target]; len(got) != 2 || got[0] != "peer@node.local" || got[1] != target {
		t.Fatalf("addrs[%q] = %v, want [peer@node.local %s]", target, got, target)
	}
}

// TestExecSSHReturnsStdoutOnTerminalFailure proves a remote command that writes to
// stdout then exits non-zero (a terminal, non-255 failure) hands its captured stdout
// back alongside the error — the contract remoteBrewInstall relies on to read brew's
// "no available formula" message off stdout.
func TestExecSSHReturnsStdoutOnTerminalFailure(t *testing.T) {
	script := "#!/bin/sh\nprintf 'Error: No available formula with the name\\n'\nexit 1\n"
	path := filepath.Join(t.TempDir(), "fake-ssh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatalf("write fake ssh: %v", err)
	}
	swapSSHBin(t, path)

	out, err := execSSHAddrs(context.Background(), testTaskPool(t), []string{"me@peer"}, "brew install x", nil)
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the terminal failure surfaced")
	}
	if !strings.Contains(out, "No available formula") {
		t.Fatalf("stdout = %q, want the captured stdout returned alongside the error", out)
	}
}

func TestUpdatePreservesHostFacts(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	initializeTestState(t, Mesh)
	target := "peer@node.tail.ts.net"

	registerTestHost(t, Mesh, target, "peer@node.local")

	if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatalf("identity-only Update: %v", err)
	}
	byTarget, err := Mesh.LoadAddrs()
	if err != nil {
		t.Fatalf("LoadAddrs: %v", err)
	}
	if got := byTarget[target]; len(got) != 2 || got[0] != "peer@node.local" || got[1] != target {
		t.Fatalf("addrs[%q] = %v, want [peer@node.local %s] preserved through Update", target, got, target)
	}
}

func registerTestHost(t *testing.T, config Config, identity string, addresses ...string) {
	t.Helper()
	fact, err := NewSSHHostFact(identity, "/opt/homebrew/bin/synckitd", addresses)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.RegisterHost(context.Background(), fact); err != nil {
		t.Fatal(err)
	}
}

// TestLocalNodes proves the bonjour node filter drops this host (case-insensitively)
// and dedupes the rest, so only real peers become .local dial candidates.
func TestLocalNodes(t *testing.T) {
	labels, notes := localNodes([]string{"yBook-Pro", "yasyf-home", "yasyf-home", "", "ybook-pro"}, "yBook-Pro")
	if strings.Join(labels, ",") != "yasyf-home" {
		t.Fatalf("labels = %v, want [yasyf-home] (self dropped, dups collapsed)", labels)
	}
	if len(notes) != 1 || notes[0].Reason != "self" {
		t.Fatalf("notes = %+v, want one self note", notes)
	}
}
