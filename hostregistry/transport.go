package hostregistry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/synckit/internal/clirunner"
)

// ErrRunnerClosed means a callback-scoped command runner has left its scope.
var ErrRunnerClosed = errors.New("hostregistry: runner scope closed")

// Runner executes commands locally and over SSH; the SSH/exec boundary tests mock.
type Runner interface {
	// Local runs name with args on this machine and returns its stdout.
	Local(ctx context.Context, name string, args ...string) (string, error)
	// SSH runs remoteCmd on target over ssh and returns its stdout.
	SSH(ctx context.Context, target, remoteCmd string) (string, error)
}

// execRunner is the production Runner: Local and SSH execute through one
// daemonkit task owner; SSH also sources brew's shellenv remotely.
type execRunner struct{ runner supervise.TaskRunner }

// NewExecRunner returns the default Runner that executes commands locally and over ssh.
func NewExecRunner(runner supervise.TaskRunner) Runner {
	return execRunner{runner: runner}
}

// WithExecRunner runs callback with the sole crash-recoverable CLI command
// owner. The runner is safe for concurrent use only while callback is active.
func WithExecRunner(ctx context.Context, callback func(Runner) error) error {
	if callback == nil {
		return errors.New("hostregistry: runner callback is required")
	}
	directory, err := Mesh.Dir()
	if err != nil {
		return fmt.Errorf("resolve synckit state directory: %w", err)
	}
	return clirunner.WithPool(ctx, directory, func(pool *supervise.Pool) error {
		runner := &scopedRunner{runner: execRunner{runner: pool}}
		runner.active.Store(true)
		defer runner.active.Store(false)
		return callback(runner)
	})
}

type scopedRunner struct {
	runner execRunner
	active atomic.Bool
}

func (r *scopedRunner) Local(ctx context.Context, name string, args ...string) (string, error) {
	if !r.active.Load() {
		return "", ErrRunnerClosed
	}
	return r.runner.Local(ctx, name, args...)
}

func (r *scopedRunner) SSH(ctx context.Context, target, remoteCmd string) (string, error) {
	if !r.active.Load() {
		return "", ErrRunnerClosed
	}
	return r.runner.SSH(ctx, target, remoteCmd)
}

func (r execRunner) Local(ctx context.Context, name string, args ...string) (string, error) {
	return runCmd(ctx, r.runner, name, args...)
}

func (r execRunner) SSH(ctx context.Context, target, remoteCmd string) (string, error) {
	return ExecSSH(ctx, r.runner, target, remoteCmd, nil)
}

// SSHArgv returns the full ssh argv that runs remoteCmd on target: the dial options
// (BatchMode, a short ConnectTimeout, keepalives), the target, then remoteCmd wrapped
// to source brew's shellenv. argv[0] is "ssh"; argv[1:] are its arguments. It is the
// argv a stdio tunnel spawns; a one-shot command goes through ExecSSH instead.
func SSHArgv(target, remoteCmd string) []string {
	return append(append([]string{sshBin}, dialOpts...), target, brewWrap(remoteCmd))
}

func runCmd(ctx context.Context, runner supervise.TaskRunner, name string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	err := runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          name,
		Args:          args,
		Stdout:        &stdout,
		Stderr:        &stderr,
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// ShellQuote single-quotes s so it survives intact as one argument to a remote
// shell, escaping any embedded single quotes.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
