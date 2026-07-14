package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

const (
	// notLoadedExit is launchctl bootout's exit code (ESRCH) when the target agent
	// isn't loaded — the only tolerated bootout failure, by code, never by stderr text.
	notLoadedExit = 3
	// inFluxExit is launchd's catch-all EIO exit code (Input/output error). It surfaces
	// when a booted-out registration is still draining, when KeepAlive respawned the
	// job, or when bootstrapping a session-limited agent from the wrong session type.
	inFluxExit = 5
)

// launchctlLauncher is the production Launcher: it shells out to launchctl's modern
// domain API (bootstrap/bootout gui/<uid>), which reports failures via exit code.
type launchctlLauncher struct{}

// NewLauncher returns the default Launcher backed by the launchctl CLI.
func NewLauncher() Launcher {
	return launchctlLauncher{}
}

func guiDomain() string {
	return fmt.Sprintf("gui/%d", os.Getuid())
}

func (launchctlLauncher) Bootstrap(ctx context.Context, plistPath string) error {
	//nolint:gosec // G204: plistPath is the tool's own generated launchd plist path, not user-supplied.
	cmd := exec.CommandContext(ctx, "launchctl", "bootstrap", guiDomain(), plistPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap %s: %w: %s", plistPath, err, bytes.TrimSpace(out))
	}
	return nil
}

func (launchctlLauncher) Bootout(ctx context.Context, label string) error {
	//nolint:gosec // G204: label is one of the tool's own launchd label constants, not user-supplied.
	out, err := exec.CommandContext(ctx, "launchctl", "bootout", guiDomain()+"/"+label).CombinedOutput()
	if err == nil || isNotLoaded(err) {
		return nil
	}
	return fmt.Errorf("launchctl bootout %s: %w: %s", label, err, bytes.TrimSpace(out))
}

// launchctlExitCode returns the process exit code carried by err, or -1 when err is
// nil or not an *exec.ExitError. Classification is by exit code only, never by stderr
// text, since the code is stable across macOS versions while the message is not.
func launchctlExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

// isNotLoaded reports whether err is launchctl bootout's ESRCH exit (the agent
// isn't loaded) — expected on a first install and during reload.
func isNotLoaded(err error) bool {
	return launchctlExitCode(err) == notLoadedExit
}

// isInFlux reports whether err is launchd's catch-all EIO exit (exit 5): a
// booted-out registration still draining, KeepAlive having respawned the job, or a
// session-limited agent bootstrapped from the wrong session type.
func isInFlux(err error) bool {
	return launchctlExitCode(err) == inFluxExit
}
