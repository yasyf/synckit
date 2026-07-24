package hostregistry

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/worker"

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
type execRunner struct{ runner *worker.Pool }

// NewExecRunner returns the default Runner that executes commands locally and over ssh.
func NewExecRunner(runner *worker.Pool) Runner {
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
	return clirunner.WithPool(ctx, directory, func(pool *worker.Pool) error {
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
	return ExecBootstrapSSH(ctx, r.runner, target, remoteCmd, nil)
}

func runCmd(ctx context.Context, runner *worker.Pool, name string, args ...string) (string, error) {
	executable, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", name, err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return "", fmt.Errorf("resolve absolute %s: %w", name, err)
	}
	result, runErr := runner.Run(ctx, worker.CommandRequest{
		Path: filepath.Clean(executable), Args: args, Dir: filepath.Dir(executable), TotalTimeout: 12 * time.Minute,
	})
	if runErr != nil {
		return string(result.Stdout), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), runErr, strings.TrimSpace(string(result.Stderr)))
	}
	return string(result.Stdout), nil
}

// ShellQuote single-quotes s so it survives intact as one argument to a remote
// shell, escaping any embedded single quotes.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
