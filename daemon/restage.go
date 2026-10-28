package daemon

import (
	"bytes"
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
	if running != want {
		if err := startInstall(installed); err != nil {
			return false, err
		}
		return true, nil
	}
	drifted, err := helpersDrifted()
	if err != nil || !drifted {
		return false, err
	}
	return false, startInstall(installed)
}

func helpersDrifted() (bool, error) {
	manifests, err := discoverManifests()
	if err != nil {
		return false, err
	}
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		agent, err := helperAgent(m)
		if errors.Is(err, exec.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		path, err := agent.PlistPath()
		if err != nil {
			return false, fmt.Errorf("resolve agent %q plist: %w", agent.Label, err)
		}
		published, err := os.ReadFile(path) //nolint:gosec // exact agent-owned plist path
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("read agent %q plist: %w", agent.Label, err)
		}
		desired, err := agent.Plist()
		if err != nil {
			return false, fmt.Errorf("render agent %q plist: %w", agent.Label, err)
		}
		if !bytes.Equal(published, desired) {
			return true, nil
		}
	}
	return false, nil
}
