package hostregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// sshConnFailureExit is the exit status ssh returns for its own connection failures;
// the remote command's own exit code (0-254) passes through unchanged. Only this code
// advances ExecSSH to the next dial address.
const sshConnFailureExit = 255

// sshBin is the ssh binary ExecSSH shells; a var so tests point it at a fake.
var sshBin = "ssh"

// sshWaitDelay bounds how long Wait blocks on the pipes after the ssh leader exits, so a
// descendant that inherited them cannot wedge Wait forever; a var so tests shrink it.
var sshWaitDelay = 5 * time.Second

// dialOpts are the per-attempt ssh options: BatchMode, a short ConnectTimeout so a
// dead address fails over fast, and keepalives so a wedged peer drops rather than
// hangs.
var dialOpts = []string{
	"-o", "BatchMode=yes",
	"-o", "ConnectTimeout=3",
	"-o", "ServerAliveInterval=5",
	"-o", "ServerAliveCountMax=3",
}

// SSHError is the failure ExecSSH returns for every ssh attempt that does not succeed: a
// remote command's non-zero exit, ssh's own 255 connection failure, or a ctx-cancel /
// post-Wait group kill. Unwrap Err to reach the *exec.ExitError that isConnFailure keys
// failover off; Stderr carries whatever the attempt captured (possibly empty).
type SSHError struct {
	Addr   string
	Stderr string
	Err    error
}

// Error renders the dial address, the underlying error, and the trimmed captured stderr
// when it is non-empty.
func (e *SSHError) Error() string {
	if s := strings.TrimSpace(e.Stderr); s != "" {
		return fmt.Sprintf("ssh %s: %v: %s", e.Addr, e.Err, s)
	}
	return fmt.Sprintf("ssh %s: %v", e.Addr, e.Err)
}

// Unwrap returns the underlying error so errors.Is / errors.As reach the ssh exit cause.
func (e *SSHError) Unwrap() error { return e.Err }

// brewWrap wraps remoteCmd so a non-interactive ssh on macOS sources brew's shellenv
// and finds a brew-installed tool on PATH.
func brewWrap(remoteCmd string) string {
	return fmt.Sprintf(`eval "$(/opt/homebrew/bin/brew shellenv)" && %s`, remoteCmd)
}

// ExecSSH runs remoteCmd on target over ssh, piping stdin when non-nil, and returns
// its stdout. It is the one production ssh path: it dials the addresses DialAddrs
// resolves for target in order — LAN/.local first, the tailnet FQDN last — and SIGKILLs
// the whole process group on ctx cancel so no ssh helper that inherited our stdout pipe
// outlives the deadline. Failover is exit-255-only: ssh returns 255 for its own
// connection failures and passes the remote command's exit code (0-254) through, so
// only an ssh-level failure advances to the next address; a remote command's own
// non-zero exit fails immediately and is never re-run on another address. Because a 255
// failure re-runs remoteCmd on the next address (at-least-once delivery), remoteCmd must
// be idempotent or convergent. Every failing attempt surfaces as a [*SSHError] naming the
// dial address, its captured stderr, and the unwrappable ssh exit cause.
func ExecSSH(ctx context.Context, target, remoteCmd string, stdin []byte) (string, error) {
	addrs, err := DialAddrs(target)
	if err != nil {
		return "", err
	}
	return execSSHAddrs(ctx, addrs, remoteCmd, stdin)
}

// execSSHAddrs dials each address in order, returning the first success and advancing
// only on an ssh-level (exit 255) connection failure. On the terminal (non-failover)
// path it propagates the attempt's [*SSHError] unchanged alongside its captured stdout,
// so a caller like remoteBrewInstall can read brew's "no available formula" message off
// stdout.
func execSSHAddrs(ctx context.Context, addrs []string, remoteCmd string, stdin []byte) (string, error) {
	var lastErr error
	for i, addr := range addrs {
		out, err := execSSHOnce(ctx, addr, remoteCmd, stdin)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if i < len(addrs)-1 && isConnFailure(err) {
			continue
		}
		return out, err
	}
	return "", lastErr
}

// execSSHOnce runs one ssh attempt to addr with the process-group-kill mechanics: on
// ctx cancel it SIGKILLs the whole group so a helper holding our stdout pipe dies too,
// WaitDelay force-closes the pipes as a backstop, and after Wait it unconditionally
// SIGKILLs the group so no descendant outlives the call. Every failure returns a
// [*SSHError].
func execSSHOnce(ctx context.Context, addr, remoteCmd string, stdin []byte) (string, error) {
	args := append(append([]string{}, dialOpts...), addr, brewWrap(remoteCmd))
	cmd := exec.CommandContext(ctx, sshBin, args...) //nolint:gosec // G204: this sync tool's job is to run ssh; addr/remoteCmd come from trusted local state (registered hosts), not untrusted input.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = sshWaitDelay
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", &SSHError{Addr: addr, Stderr: stderr.String(), Err: err}
	}
	// The watcher records ctxKilled before killing the group so the post-Wait join reads it
	// consistently; the unconditional kill then reaps a descendant that outlived the leader.
	ctxKilled := false
	done := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-ctx.Done():
			ctxKilled = true
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // best-effort; ESRCH is done
		case <-done:
		}
	}()
	err := cmd.Wait()
	close(done)
	<-watched
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // best-effort backstop; ESRCH is done
	if ctxKilled {
		return stdout.String(), &SSHError{Addr: addr, Stderr: stderr.String(), Err: ctx.Err()}
	}
	if err != nil {
		return stdout.String(), &SSHError{Addr: addr, Stderr: stderr.String(), Err: err}
	}
	return stdout.String(), nil
}

// isConnFailure reports whether err is ssh's own connection failure (exit 255), the
// only failure ExecSSH fails over on. A remote command's exit code (0-254) or a signal
// kill is not a connection failure.
func isConnFailure(err error) bool {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode() == sshConnFailureExit
	}
	return false
}

// DialAddrs returns the ordered ssh dial candidates for target: the LAN/.local
// addresses recorded for it (in recorded order) first, then target's own FQDN last, so
// a call reaches the peer over the LAN — under sshd's stable TCC identity — before
// falling back to the tailnet. With no recorded addresses it is just [target].
func DialAddrs(target string) ([]string, error) {
	byTarget, err := Mesh.LoadAddrs()
	if err != nil {
		return nil, err
	}
	return orderDialAddrs(target, byTarget[target]), nil
}

// orderDialAddrs puts the recorded LAN addresses first and target last, dropping empty
// or duplicate entries and any recorded copy of target itself.
func orderDialAddrs(target string, lan []string) []string {
	ordered := make([]string, 0, len(lan)+1)
	seen := map[string]struct{}{}
	for _, a := range lan {
		if a == "" || a == target {
			continue
		}
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		ordered = append(ordered, a)
	}
	return append(ordered, target)
}

// LoadAddrs reads the target-to-dial-addresses map from state.json, returning an empty
// map when the "addrs" key is absent.
func (c Config) LoadAddrs() (map[string][]string, error) {
	raw, err := c.readRaw()
	if err != nil {
		return nil, err
	}
	return addrsFromRaw(raw)
}

// addrsFromRaw decodes the "addrs" key out of a raw state map, returning an empty map
// when the key is absent.
func addrsFromRaw(raw map[string]json.RawMessage) (map[string][]string, error) {
	out := map[string][]string{}
	if a, ok := raw["addrs"]; ok {
		if err := json.Unmarshal(a, &out); err != nil {
			return nil, fmt.Errorf("parse addrs: %w", err)
		}
	}
	return out, nil
}

// AddAddr records addr as an alternate dial address for target, appending it under the
// "addrs" key while preserving every other key in state.json byte-for-byte. A repeat
// addr is a no-op.
func (c Config) AddAddr(ctx context.Context, target, addr string) error {
	return c.UpdateRaw(ctx, func(raw map[string]json.RawMessage) error {
		byTarget, err := addrsFromRaw(raw)
		if err != nil {
			return err
		}
		for _, a := range byTarget[target] {
			if a == addr {
				return nil
			}
		}
		byTarget[target] = append(byTarget[target], addr)
		encoded, err := json.Marshal(byTarget)
		if err != nil {
			return fmt.Errorf("encode addrs: %w", err)
		}
		raw["addrs"] = encoded
		return nil
	})
}

// LocalTarget rewrites target's host to its short node's ".local" mDNS name, keeping
// the user: "u@host.tail.ts.net" becomes "u@host.local", "host" becomes "host.local".
func LocalTarget(target string) string {
	node := HostNode(target)
	if i := strings.LastIndex(target, "@"); i >= 0 {
		return target[:i+1] + node + ".local"
	}
	return node + ".local"
}
