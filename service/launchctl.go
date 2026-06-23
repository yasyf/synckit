package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// notLoadedExit is launchctl bootout's exit code (ESRCH) when the target agent
// isn't loaded — the only tolerated bootout failure, by code, never by stderr text.
const notLoadedExit = 3

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

// isNotLoaded reports whether err is launchctl bootout's ESRCH exit (the agent
// isn't loaded) — expected on a first install and during reload. The decision is by
// exit code only, never by stderr text, since the code is stable across macOS
// versions while the message is not.
func isNotLoaded(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == notLoadedExit
}
