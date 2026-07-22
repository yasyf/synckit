package hostregistry

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

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
