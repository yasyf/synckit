package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// exitErr runs `sh -c "exit N"` to obtain a genuine *exec.ExitError carrying exit
// code n, so the tolerance check is tested against real process state, not a
// hand-built error.
func exitErr(t *testing.T, n int) error {
	t.Helper()
	//nolint:gosec // G204: n is a test-table int, not user input; the command runs a fixed `sh -c "exit N"`.
	err := exec.CommandContext(context.Background(), "sh", "-c", fmt.Sprintf("exit %d", n)).Run()
	if err == nil {
		t.Fatalf("sh -c 'exit %d' returned nil error", n)
	}
	return err
}

func TestIsNotLoadedTolerance(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want bool
	}{
		{"esrch exit 3 tolerated", notLoadedExit, true},
		{"exit 1 not tolerated", 1, false},
		{"exit 2 not tolerated", 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNotLoaded(exitErr(t, tc.code)); got != tc.want {
				t.Errorf("isNotLoaded(exit %d) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
	if isNotLoaded(os.ErrNotExist) {
		t.Error("isNotLoaded should be false for a non-exec error")
	}
	// Production wraps the exit error; classification must reach through it (errors.As).
	if !isNotLoaded(fmt.Errorf("launchctl bootout: %w: boom", exitErr(t, notLoadedExit))) {
		t.Error("isNotLoaded should reach a wrapped exit-3 error")
	}
}

func TestLaunchctlExitCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"nil is -1", nil, -1},
		{"exit 3", exitErr(t, 3), 3},
		{"exit 5", exitErr(t, 5), 5},
		{"wrapped exit 5", fmt.Errorf("launchctl bootstrap /p: %w: boom", exitErr(t, 5)), 5},
		{"non-exec error is -1", errors.New("boom"), -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := launchctlExitCode(tc.err); got != tc.want {
				t.Errorf("launchctlExitCode(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsInFluxTolerance(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want bool
	}{
		{"eio exit 5 in flux", inFluxExit, true},
		{"exit 3 not in flux", 3, false},
		{"exit 1 not in flux", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isInFlux(exitErr(t, tc.code)); got != tc.want {
				t.Errorf("isInFlux(exit %d) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
	if isInFlux(os.ErrNotExist) {
		t.Error("isInFlux should be false for a non-exec error")
	}
	// Production wraps the exit error; classification must reach through it (errors.As).
	if !isInFlux(fmt.Errorf("launchctl bootstrap /p: %w: boom", exitErr(t, inFluxExit))) {
		t.Error("isInFlux should reach a wrapped exit-5 error")
	}
}

func TestGuiDomainFormat(t *testing.T) {
	if got, want := guiDomain(), fmt.Sprintf("gui/%d", os.Getuid()); got != want {
		t.Errorf("guiDomain() = %q, want %q", got, want)
	}
}

func TestNewLauncherType(t *testing.T) {
	if _, ok := NewLauncher().(launchctlLauncher); !ok {
		t.Errorf("NewLauncher() returned %T, want launchctlLauncher", NewLauncher())
	}
}
