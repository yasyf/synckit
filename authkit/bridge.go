// Package authkit bridges Go consumers to the installed, Developer-ID-signed
// authkit helper app — the one process that touches the Secure Enclave and
// summons consent prompts. The bridge fails closed: a missing helper surfaces
// a *HelperError rather than degrading to an unsigned fallback, since an
// ad-hoc helper is SIGKILLed at exec by AMFI and refused the Enclave. Each
// call returns the helper's raw exit code, stdout, and stderr so callers
// branch on the documented 0 (approved) / 1 (denied) / 2 (unavailable) / 3
// (screen-locked) / 4 (caller-rejected-or-usage-error) contract and log the
// helper's stderr diagnostics.
package authkit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
)

// The helper's exit-code contract.
const (
	// CodeApproved is an approved prompt or a successful operation.
	CodeApproved = 0
	// CodeDenied is a cancelled or denied prompt; do not retry.
	CodeDenied = 1
	// CodeUnavailable means only that the device auth mechanism is unavailable
	// (no biometry or passcode) or an item is not found — the sole outcome a
	// consumer may degrade on.
	CodeUnavailable = 2
	// CodeScreenLocked is a Secure-Enclave call the data-protection keybag
	// refused with errSecInteractionNotAllowed (-25308): the screen is locked
	// or no user is present. Distinct from CodeUnavailable so callers route
	// the gate instead of failing; retry after unlock.
	CodeScreenLocked = 3
	// CodeCallerRejected is a non-pinned caller, a malformed invocation, or
	// any misconfiguration: a hard failure no consumer may degrade on or
	// route around.
	CodeCallerRejected = 4
)

// Result is the outcome of one helper subcommand: the raw exit code and the
// bytes the helper wrote to stdout and stderr. On a non-zero exit, Stderr
// carries the helper's diagnostic line (the failing operation and its numeric
// OSStatus) so callers can log and classify the failure.
type Result struct {
	Code   int
	Stdout []byte
	Stderr []byte
}

// Bridge invokes the signed authkit helper. The zero value resolves the
// installed helper via RequireHelper on each call (failing closed if absent);
// set Binary to pin a path.
type Bridge struct {
	// Binary, when set, is the helper executable to run; otherwise the bridge
	// resolves the installed signed helper via RequireHelper.
	Binary string
}

// binary resolves the helper executable, failing closed with a *HelperError
// when none is installed.
func (b Bridge) binary() (string, error) {
	if b.Binary != "" {
		return b.Binary, nil
	}
	return RequireHelper()
}

// Run executes one helper subcommand, feeding stdin (when non-nil) and
// appending extraEnv to the inherited environment. It returns the helper's
// exit code, stdout, and stderr; a non-zero exit is reported in Result.Code,
// not as an error, so callers branch on the 0/1/2/3/4 contract. err is non-nil
// only when the helper cannot be resolved or spawned, or it dies on a signal.
func (b Bridge) Run(ctx context.Context, stdin []byte, extraEnv []string, args ...string) (Result, error) {
	bin, err := b.binary()
	if err != nil {
		return Result{}, err
	}
	//nolint:gosec // G204: bin is the resolved signed helper and args are fixed subcommands, not user-supplied.
	cmd := exec.CommandContext(ctx, bin, args...)
	if extraEnv != nil {
		cmd.Env = append(cmd.Environ(), extraEnv...)
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	switch runErr := cmd.Run(); {
	case runErr == nil:
		return Result{Code: 0, Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, nil
	case isExit(runErr):
		var exitErr *exec.ExitError
		errors.As(runErr, &exitErr)
		return Result{Code: exitErr.ExitCode(), Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, nil
	default:
		return Result{}, fmt.Errorf("run %s %v: %w: %s", bin, args, runErr, bytes.TrimSpace(stderr.Bytes()))
	}
}

// isExit reports whether err is a clean non-zero process exit (ExitCode >= 0).
// A signal kill reports ExitCode() == -1, which is a genuine run failure, not
// a contract exit code, so it is excluded.
func isExit(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() >= 0
}
