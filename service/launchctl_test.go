package service

import (
	"context"
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
