package daemon

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var (
	restageInstalled = restageUpgrade
	startInstall     = launchInstall
)

func launchInstall(program string) error {
	cmd := exec.Command(program, "install") //nolint:gosec // G204: the installed synckitd resolved from PATH, fixed argv.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// launchd kills a job's whole process group when the job exits, and install
	// boots out the reconcile job that launched it, so install needs its own session.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s install: %w", program, err)
	}
	return cmd.Process.Release()
}

func restageUpgrade() (bool, error) {
	installed, err := canonicalHelperProgram(daemonBinary)
	if errors.Is(err, exec.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	self, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("resolve current executable: %w", err)
	}
	running, err := programDigest(self)
	if err != nil {
		return false, err
	}
	want, err := programDigest(installed)
	if err != nil {
		return false, err
	}
	if running == want {
		return false, nil
	}
	if err := startInstall(installed); err != nil {
		return false, err
	}
	return true, nil
}
