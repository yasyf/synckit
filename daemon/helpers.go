package daemon

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
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
	replaceHelpers   = replaceOutdatedHelpers
	installedVersion = probeVersion
)

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
