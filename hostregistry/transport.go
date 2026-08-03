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

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/clirunner"
)

// maxCommandRun ceilings one command's whole life — spawn, streams, settlement — over
// the caller's own deadline, never past it, so a wedged local command or ssh session
// cannot outlive the CLI that started it.
const maxCommandRun = 12 * time.Minute

const maxCommandOutput = 16 << 20

// ErrRunnerClosed means a callback-scoped command runner has left its scope.
var ErrRunnerClosed = errors.New("hostregistry: runner scope closed")

// Commander runs one bounded disposable command under durable process ownership.
// Both *daemonkit.Owned (a CLI scope) and daemonkit.Ctx (a daemon's) satisfy it.
type Commander interface {
	Run(ctx context.Context, cmd daemonkit.Cmd) (daemonkit.RunResult, error)
}

// Runner executes commands locally and over SSH; the SSH/exec boundary tests mock.
type Runner interface {
	// Local runs name with args on this machine and returns its stdout.
	Local(ctx context.Context, name string, args ...string) (string, error)
	// SSH runs remoteCmd on target over ssh and returns its stdout.
	SSH(ctx context.Context, target, remoteCmd string) (string, error)
}

// execRunner is the production Runner: Local and SSH execute through one
// daemonkit process-ownership scope; SSH also sources brew's shellenv remotely.
type execRunner struct{ runner Commander }

// NewExecRunner returns the default Runner that executes commands locally and over ssh.
func NewExecRunner(runner Commander) Runner {
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
	return clirunner.WithOwned(ctx, directory, func(owned *daemonkit.Owned) error {
		runner := &scopedRunner{runner: execRunner{runner: owned}}
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

func runCmd(ctx context.Context, runner Commander, name string, args ...string) (string, error) {
	executable, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", name, err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return "", fmt.Errorf("resolve absolute %s: %w", name, err)
	}
	runCtx, cancel := context.WithTimeout(ctx, maxCommandRun)
	defer cancel()
	result, runErr := runner.Run(runCtx, daemonkit.Cmd{
		Path: filepath.Clean(executable), Args: args, Dir: filepath.Dir(executable),
		Exec: daemonkit.ServingSameUser(), MaxOutput: maxCommandOutput,
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
