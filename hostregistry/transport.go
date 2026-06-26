package hostregistry

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Runner executes commands locally and over SSH; the SSH/exec boundary tests mock.
type Runner interface {
	// Local runs name with args on this machine and returns its stdout.
	Local(ctx context.Context, name string, args ...string) (string, error)
	// SSH runs remoteCmd on target over ssh and returns its stdout.
	SSH(ctx context.Context, target, remoteCmd string) (string, error)
}

// execRunner is the production Runner: Local shells out directly, SSH wraps the
// remote command so it sources brew's shellenv (non-interactive ssh on macOS
// lacks brew, and thus a brew-installed tool, on PATH).
type execRunner struct{}

// NewExecRunner returns the default Runner that executes commands locally and over ssh.
func NewExecRunner() Runner {
	return execRunner{}
}

func (execRunner) Local(ctx context.Context, name string, args ...string) (string, error) {
	return runCmd(ctx, name, args...)
}

func (execRunner) SSH(ctx context.Context, target, remoteCmd string) (string, error) {
	argv := SSHArgv(target, remoteCmd)
	return runCmd(ctx, argv[0], argv[1:]...)
}

// SSHArgv returns the full ssh argv that runs remoteCmd on target: the BatchMode,
// timeout, and keepalive options, the target, then remoteCmd wrapped to source
// brew's shellenv (a non-interactive ssh on macOS lacks brew, and thus any
// brew-installed tool, on PATH). argv[0] is "ssh"; argv[1:] are its arguments.
func SSHArgv(target, remoteCmd string) []string {
	wrapped := fmt.Sprintf(`eval "$(/opt/homebrew/bin/brew shellenv)" && %s`, remoteCmd)
	return []string{"ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3", target, wrapped}
}

func runCmd(ctx context.Context, name string, args ...string) (string, error) {
	//nolint:gosec // G204: this is a CLI sync tool whose job is to run ssh/git; name and args come from trusted local state (registered hosts, repo config), not untrusted input.
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// ShellQuote single-quotes s so it survives intact as one argument to a remote
// shell, escaping any embedded single quotes.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
