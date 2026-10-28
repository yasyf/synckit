package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/version"

	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/manifest"
)

const (
	versionProbeTimeout  = 10 * time.Second
	helperReplaceTimeout = 30 * time.Second
)

var (
	restageInstalled = restageUpgrade
	replaceHelpers   = replaceOutdatedHelpers
	startInstall     = launchInstall
	installedVersion = probeVersion
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

func probeVersion(ctx context.Context, program string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, versionProbeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, program, "--version").Output() //nolint:gosec // G204: an installed binary resolved from PATH, fixed argv.
	if err != nil {
		return "", fmt.Errorf("probe %s --version: %w", program, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("probe %s --version: printed no version", program)
	}
	return fields[len(fields)-1], nil
}

func restageUpgrade(ctx context.Context, running string) (bool, error) {
	installed, err := executableAlias(daemonBinary)
	if errors.Is(err, exec.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	want, err := installedVersion(ctx, installed)
	if err != nil {
		return false, err
	}
	if !version.Equal(running, want) {
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

func replaceOutdatedHelpers(ctx context.Context) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	var errs []error
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		if err := replaceOutdatedHelper(ctx, m); err != nil {
			errs = append(errs, fmt.Errorf("helper %q: %w", m.Name, err))
		}
	}
	return errors.Join(errs...)
}

func replaceOutdatedHelper(ctx context.Context, m manifest.Manifest) error {
	program, err := executableAlias(m.Binary)
	if errors.Is(err, exec.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	want, err := installedVersion(ctx, program)
	if err != nil {
		return err
	}
	spec, err := helperruntime.Spec(m.Name, daemonkit.Program{}, 0)
	if err != nil {
		return err
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, helperReplaceTimeout)
	defer cancel()
	defer func() { _ = client.Close(ctx) }()
	health, err := client.Health(ctx)
	if errors.Is(err, daemonkit.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	if version.Equal(health.Build, want) {
		return nil
	}
	_, err = client.Drain(ctx, daemonkit.Expect{Build: health.Build, Generation: health.Generation})
	if errors.Is(err, daemonkit.ErrAbsent) || errors.Is(err, daemonkit.ErrDraining) {
		return nil
	}
	return err
}
