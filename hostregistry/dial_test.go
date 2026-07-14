package hostregistry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

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

	out, err := execSSHAddrs(context.Background(), []string{"me@connfail-lan", "me@good-tailnet"}, "echo hi", nil)
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

// TestExecSSHRemoteFailureNeverFailsOver proves a remote command's own non-255 exit
// fails immediately and is never re-run against the next address.
func TestExecSSHRemoteFailureNeverFailsOver(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "attempts.log")
	swapSSHBin(t, fakeSSH(t, logPath))

	_, err := execSSHAddrs(context.Background(), []string{"me@remotefail-lan", "me@good-tailnet"}, "echo hi", nil)
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

// TestExecSSHKillsBackgroundedDescendantOnCtxCancel reproduces the WaitDelay overrun: a
// fake ssh leader backgrounds a long-lived descendant that keeps our stdout/stderr pipes
// open, then exits, so exec.CommandContext's own Cancel never fires (the leader is already
// gone). Without the independent ctx watcher, Wait blocks on the pipe until WaitDelay (5s)
// and the descendant survives; with it, a 100ms ctx SIGKILLs the whole group at once.
func TestExecSSHKillsBackgroundedDescendantOnCtxCancel(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	script := "#!/bin/sh\n" +
		"sh -c 'echo $$ > " + pidFile + "; exec sleep 30' &\n" +
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := execSSHAddrs(ctx, []string{"me@peer"}, "echo hi", nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the ctx-cancelled ssh failure")
	}
	if elapsed > time.Second {
		t.Fatalf("execSSHAddrs took %s, want well under 1s (ctx cancel must SIGKILL the group, not wait out WaitDelay)", elapsed)
	}

	pid := 0
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if pid = readDescendantPID(pidFile); pid > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("fake ssh never recorded its descendant pid")
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
// must surface the deadline as a ctx.Err()-wrapped error (never a success), leave the whole
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
		"sh -c 'echo $$ > " + pidFile + "; exec sleep 30' &\n" +
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := execSSHAddrs(ctx, []string{"me@peer-a", "me@peer-b"}, "echo hi", nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("execSSHAddrs returned nil, want the cancelled op surfaced as an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("execSSHAddrs took %s, want well under 1s (the deadline must SIGKILL the group, not wait out WaitDelay)", elapsed)
	}
	if tried := attempts(t, logPath); len(tried) != 1 || tried[0] != "me@peer-a" {
		t.Fatalf("addresses tried = %v, want only [me@peer-a] (a ctx error must not fail over)", tried)
	}

	pid := 0
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if pid = readDescendantPID(pidFile); pid > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("fake ssh never recorded its descendant pid")
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d still alive; the deadline must kill the whole process group", pid)
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

// shrinkWaitDelay points sshWaitDelay at d so a test that exercises the leader-exits-first
// WaitDelay path runs fast, restoring it on cleanup.
func shrinkWaitDelay(t *testing.T, d time.Duration) {
	t.Helper()
	prev := sshWaitDelay
	sshWaitDelay = d
	t.Cleanup(func() { sshWaitDelay = prev })
}

// TestExecSSHReapsSurvivingDescendantWhenLeaderExitsFirst covers the leak G2 fixes: a 255
// leader backgrounds a descendant that holds our pipes and then exits, so the ctx watcher
// never fires (ctx is live) and WaitDelay alone releases Wait. The unconditional post-Wait
// group kill must reap the descendant, and the 255 must still fail over to the next address.
func TestExecSSHReapsSurvivingDescendantWhenLeaderExitsFirst(t *testing.T) {
	shrinkWaitDelay(t, 150*time.Millisecond)
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "descendant.pid")
	logPath := filepath.Join(dir, "attempts.log")
	script := "#!/bin/sh\n" +
		"a2=\"\"; a1=\"\"\n" +
		"for a in \"$@\"; do a2=\"$a1\"; a1=\"$a\"; done\n" +
		"printf '%s\\n' \"$a2\" >> \"" + logPath + "\"\n" +
		"case \"$a2\" in\n" +
		"  *connfail*) sh -c 'echo $$ > " + pidFile + "; exec sleep 30' & echo 'ssh: connect: Connection refused' >&2; exit 255 ;;\n" +
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

	out, err := execSSHAddrs(context.Background(), []string{"me@connfail-lan", "me@good-tailnet"}, "echo hi", nil)
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

		_, err := execSSHAddrs(context.Background(), []string{"me@remotefail-lan"}, "echo hi", nil)
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
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("error %v does not unwrap to *exec.ExitError; isConnFailure would misjudge failover", err)
		}
	})

	t.Run("ctx kill", func(t *testing.T) {
		bin := filepath.Join(t.TempDir(), "fake-ssh")
		script := "#!/bin/sh\necho 'stderr diag before kill' >&2\nexec sleep 30\n"
		if err := os.WriteFile(bin, []byte(script), 0o755); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
			t.Fatalf("write fake ssh: %v", err)
		}
		swapSSHBin(t, bin)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := execSSHAddrs(ctx, []string{"me@peer"}, "echo hi", nil)
		if err == nil {
			t.Fatal("execSSHAddrs succeeded, want the ctx-cancelled failure surfaced")
		}
		var sshErr *SSHError
		if !errors.As(err, &sshErr) {
			t.Fatalf("error %v (%T) is not a *SSHError", err, err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error %v does not unwrap to context.DeadlineExceeded", err)
		}
		if !strings.Contains(sshErr.Stderr, "stderr diag before kill") {
			t.Fatalf("SSHError.Stderr = %q, want the captured stderr populated on the ctx-kill path", sshErr.Stderr)
		}
	})
}

func TestDialAddrsDefaultsToTarget(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
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
	target := "me@node.tail.ts.net"
	if err := Mesh.AddAddr(context.Background(), target, "me@node.local"); err != nil {
		t.Fatalf("AddAddr: %v", err)
	}
	got, err := DialAddrs(target)
	if err != nil {
		t.Fatalf("DialAddrs: %v", err)
	}
	want := []string{"me@node.local", target}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("DialAddrs = %v, want %v (LAN first, tailnet last)", got, want)
	}
}

// TestAddAddrPreservesForeignKeys proves the new addrs writer honors the shared-state
// FK contract: appending an address leaves self/hosts and a consumer's domain keys
// byte-for-byte intact, and a repeat address is a no-op.
func TestAddAddrPreservesForeignKeys(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	target := "peer@node.tail.ts.net"

	if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
		g.Self = "me@self"
		g.UpsertHost(target)
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}
	if err := Mesh.UpdateRaw(context.Background(), func(raw map[string]json.RawMessage) error {
		raw["repos"] = json.RawMessage(`[{"relpath":"cc-review","local_only":false}]`)
		return nil
	}); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	before := readState(t)
	if err := Mesh.AddAddr(context.Background(), target, "peer@node.local"); err != nil {
		t.Fatalf("AddAddr: %v", err)
	}
	after := readState(t)
	assertKeysByteEqual(t, "addr write", before, after, []string{"self", "hosts", "repos"})

	byTarget, err := Mesh.LoadAddrs()
	if err != nil {
		t.Fatalf("LoadAddrs: %v", err)
	}
	if got := byTarget[target]; len(got) != 1 || got[0] != "peer@node.local" {
		t.Fatalf("addrs[%q] = %v, want [peer@node.local]", target, got)
	}

	if err := Mesh.AddAddr(context.Background(), target, "peer@node.local"); err != nil {
		t.Fatalf("AddAddr repeat: %v", err)
	}
	byTarget, err = Mesh.LoadAddrs()
	if err != nil {
		t.Fatalf("LoadAddrs: %v", err)
	}
	if got := byTarget[target]; len(got) != 1 {
		t.Fatalf("repeat AddAddr recorded a duplicate: %v", got)
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

	out, err := execSSHAddrs(context.Background(), []string{"me@peer"}, "brew install x", nil)
	if err == nil {
		t.Fatal("execSSHAddrs succeeded, want the terminal failure surfaced")
	}
	if !strings.Contains(out, "No available formula") {
		t.Fatalf("stdout = %q, want the captured stdout returned alongside the error", out)
	}
}

// TestUpdatePreservesAddrsKey proves the reverse FK direction: once AddAddr has written
// the "addrs" key, a plain Mesh.Update by an addrs-unaware writer (it touches only
// self/hosts) leaves the addrs key byte-for-byte intact.
func TestUpdatePreservesAddrsKey(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	target := "peer@node.tail.ts.net"

	if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
		g.Self = "me@self"
		g.UpsertHost(target)
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}
	if err := Mesh.AddAddr(context.Background(), target, "peer@node.local"); err != nil {
		t.Fatalf("AddAddr: %v", err)
	}

	before := readState(t)
	if _, err := Mesh.Update(context.Background(), func(g *Registry) error {
		g.UpsertHost("other@host")
		return nil
	}); err != nil {
		t.Fatalf("addrs-unaware Update: %v", err)
	}
	after := readState(t)
	assertKeysByteEqual(t, "addrs-unaware update", before, after, []string{"addrs"})
	assertKeysChanged(t, "addrs-unaware update", before, after, []string{"hosts"})

	byTarget, err := Mesh.LoadAddrs()
	if err != nil {
		t.Fatalf("LoadAddrs: %v", err)
	}
	if got := byTarget[target]; len(got) != 1 || got[0] != "peer@node.local" {
		t.Fatalf("addrs[%q] = %v, want [peer@node.local] preserved through Update", target, got)
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
