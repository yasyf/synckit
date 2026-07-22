package hostregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

// sshConnFailureExit is the exit status ssh returns for its own connection failures;
// the remote command's own exit code (0-254) passes through unchanged. Only this code
// advances ExecSSH to the next dial address.
const sshConnFailureExit = 255

// sshBin is the ssh binary ExecSSH shells; a var so tests point it at a fake.
var sshBin = "ssh"

// sshDialBudget caps total failover time across dial addresses: once spent, the next
// 255 skips straight to the final canonical target, so many dead recorded addresses
// cost ~budget + one ConnectTimeout, not N x ConnectTimeout. It bounds only failover —
// a connected remote command rides ctx, never this budget. A var so tests shrink it.
var sshDialBudget = 10 * time.Second

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
// remote command's non-zero exit, ssh's own 255 connection failure, or cancellation.
// Unwrap Err to reach the typed ExitCode cause that isConnFailure keys failover off;
// Stderr carries whatever the attempt captured (possibly empty).
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
// resolves for target in order — LAN/.local first, the tailnet FQDN last — through a
// daemonkit task owner that settles the complete process session before returning.
// Failover is exit-255-only: ssh returns 255 for its own
// connection failures and passes the remote command's exit code (0-254) through, so
// only an ssh-level failure advances to the next address; a remote command's own
// non-zero exit fails immediately and is never re-run on another address — an address
// that connects and runs the command is the right host (a stale address reaching the
// wrong machine fails host-key verification, a 255), so its exit code is authoritative
// and re-running elsewhere would mask it. Because a 255 failure re-runs remoteCmd on the
// next address (at-least-once delivery), remoteCmd must be idempotent or convergent.
// ExecSSH bounds only the dial phase (per-address ConnectTimeout, sshDialBudget across
// addresses, keepalives that drop a dead peer); a connected command's runtime belongs
// to the caller's ctx deadline — only the caller knows how long its command should take.
// Every failing attempt surfaces as a [*SSHError] naming the dial address, its captured
// stderr, and the unwrappable typed exit cause.
func ExecSSH(ctx context.Context, runner supervise.TaskRunner, target, remoteCmd string, stdin []byte) (string, error) {
	addrs, err := DialAddrs(target)
	if err != nil {
		return "", err
	}
	return execSSHAddrs(ctx, runner, addrs, remoteCmd, stdin)
}

// execSSHAddrs dials each address in order, returning the first success and advancing
// only on an ssh-level (exit 255) connection failure. Failover is bounded by
// sshDialBudget: once spent, the remaining recorded alternates are skipped and only the
// final canonical target is dialed. On the terminal (non-failover) path it propagates
// the attempt's [*SSHError] unchanged alongside its captured stdout, so a caller like
// remoteBrewInstall can read brew's "no available formula" message off stdout.
func execSSHAddrs(
	ctx context.Context,
	runner supervise.TaskRunner,
	addrs []string,
	remoteCmd string,
	stdin []byte,
) (string, error) {
	start := time.Now()
	var lastErr error
	for i := 0; i < len(addrs); i++ {
		out, err := execSSHOnce(ctx, runner, addrs[i], remoteCmd, stdin)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if i < len(addrs)-1 && isConnFailure(err) {
			if time.Since(start) >= sshDialBudget {
				i = len(addrs) - 2 // budget spent: next iteration dials the final canonical target
			}
			continue
		}
		return out, err
	}
	return "", lastErr
}

// execSSHOnce runs one durably owned ssh attempt to addr. Every failure returns a
// [*SSHError].
func execSSHOnce(
	ctx context.Context,
	runner supervise.TaskRunner,
	addr, remoteCmd string,
	stdin []byte,
) (string, error) {
	args := append(append([]string{}, dialOpts...), addr, brewWrap(remoteCmd))
	input, err := taskInput(stdin)
	if err != nil {
		return "", &SSHError{Addr: addr, Err: err}
	}
	var stdout, stderr bytes.Buffer
	err = runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          sshBin,
		Args:          args,
		Stdin:         input,
		Stdout:        &stdout,
		Stderr:        &stderr,
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return stdout.String(), &SSHError{Addr: addr, Stderr: stderr.String(), Err: err}
	}
	return stdout.String(), nil
}

func taskInput(payload []byte) (*os.File, error) {
	if payload == nil {
		return nil, nil
	}
	input, err := os.CreateTemp("", "synckit-task-input-*")
	if err != nil {
		return nil, fmt.Errorf("create task input: %w", err)
	}
	_ = os.Remove(input.Name())
	if _, err := input.Write(payload); err != nil {
		_ = input.Close()
		return nil, fmt.Errorf("write task input: %w", err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		_ = input.Close()
		return nil, fmt.Errorf("rewind task input: %w", err)
	}
	return input, nil
}

// isConnFailure reports whether err is ssh's own connection failure (exit 255), the
// only failure ExecSSH fails over on. A remote command's exit code (0-254) or a signal
// kill is not a connection failure.
func isConnFailure(err error) bool {
	var ee interface{ ExitCode() int }
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
