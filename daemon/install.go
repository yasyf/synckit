package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/launchd"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/manifest"
)

const (
	// installedAgentsFile records the labels the last install converged. v0.21
	// launchd keeps no state and never enumerates what a consumer owns, so this
	// record is the only thing that lets a later run sweep a helper agent whose
	// manifest has since vanished.
	installedAgentsFile     = "installed-agents.json"
	installedAgentsIdentity = "synckit-installed-agents-v1"

	// ensureDeadline bounds Client.Ensure, which refuses a context without one.
	ensureDeadline = 2 * time.Minute
	// settleDeadline bounds the proof that the booted-out serve daemon actually
	// left the process table.
	settleDeadline = 30 * time.Second
)

var (
	applyAgent = func(ctx context.Context, agent launchd.Agent) error {
		return launchd.Apply(ctx, launchctl, agent)
	}
	removeAgent = func(ctx context.Context, label string) error {
		return launchd.Remove(ctx, launchctl, label)
	}
	ensureServeAgent = ensureServe
	settleServeAgent = settleServe

	launchctl launchd.Runner = execLaunchctl
)

func newInstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install the synckitd LaunchAgents (reconcile tick, serve daemon, consumer helpers).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := install(cmd.Context()); err != nil {
				return err
			}
			cmd.Println("Installed synckitd agents.")
			return nil
		},
	}
}

func newUninstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "uninstall",
		Short: "Remove the synckitd LaunchAgents.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := uninstall(cmd.Context()); err != nil {
				return err
			}
			cmd.Println("Uninstalled synckitd agents.")
			return nil
		},
	}
}

// install converges every synckitd LaunchAgent. The order is load-bearing: the
// stray sweep reads the previous record before it is replaced, and the new
// record lands before any agent is applied so a partial install still sweeps on
// the next run. Ensure owns the serve label outright — its plist, its stable
// executable, and its live process — and runs before the Apply loop, which
// applies only the agents serviceAgents built, whose programs it already staged
// under <mesh>/bin.
func install(ctx context.Context) error {
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		return fmt.Errorf("initialize host mesh state: %w", err)
	}
	manifests, skipped, err := discoverScan()
	if err != nil {
		return err
	}
	agents, err := serviceAgents(manifests)
	if err != nil {
		return err
	}
	plan, err := launchd.NewPlan(agents)
	if err != nil {
		return fmt.Errorf("build synckitd agent plan: %w", err)
	}
	planned := plan.Agents()
	labels := make([]string, 0, len(planned))
	for _, agent := range planned {
		labels = append(labels, agent.Label)
	}
	recorded, err := readInstalledAgents()
	if err != nil {
		return err
	}
	retained := retainedLabels(skipped, recorded)
	if err := sweepStrayAgents(ctx, recorded, labels, retained); err != nil {
		return err
	}
	if err := writeInstalledAgents(append(slices.Clone(labels), retained...)); err != nil {
		return err
	}
	if err := ensureServeAgent(ctx); err != nil {
		return err
	}
	for _, agent := range planned {
		if err := applyAgent(ctx, agent); err != nil {
			return fmt.Errorf("apply agent %q: %w", agent.Label, err)
		}
	}
	return nil
}

// uninstall removes every agent synckit registered. Removing the serve label
// precedes settling it: launchd.Remove boots the job out, which is the only
// thing that stops a RestartAlways daemon — Settle observes and never signals,
// so settling first would just wait out its deadline against a daemon nothing
// had asked to leave. A failed label source surfaces its error and never aborts
// the sweep: one unreadable manifest must not leave every agent loaded, and the
// record survives the partial failure for the next run to finish.
func uninstall(ctx context.Context) error {
	labels, err := uninstallLabels()
	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	for _, label := range labels {
		if err := removeAgent(ctx, label); err != nil {
			errs = append(errs, fmt.Errorf("remove agent %q: %w", label, err))
			continue
		}
		if err := removeStagedProgram(label); err != nil {
			errs = append(errs, err)
		}
	}
	if err := removeAgent(ctx, serveAgentLabel); err != nil {
		errs = append(errs, fmt.Errorf("remove agent %q: %w", serveAgentLabel, err))
	}
	if err := settleServeAgent(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return removeInstalledAgents()
}

// uninstallLabels unions the durable record with a freshly derived label set:
// the record may predate a manifest registered since, and a manifest may have
// vanished since the record was written. The two sources are independent, so a
// failure in either contributes its error and no labels while the other still
// names everything it knows.
func uninstallLabels() ([]string, error) {
	var errs []error
	labels, err := readInstalledAgents()
	if err != nil {
		errs = append(errs, err)
	}
	manifests, err := discoverManifests()
	if err != nil {
		errs = append(errs, err)
	}
	planned, err := plannedLabels(manifests)
	if err != nil {
		errs = append(errs, err)
	}
	for _, label := range planned {
		if !slices.Contains(labels, label) {
			labels = append(labels, label)
		}
	}
	slices.Sort(labels)
	return labels, errors.Join(errs...)
}

// sweepStrayAgents removes every recorded label neither the new plan nor the
// retained set names — the stale-agent policy v0.20's controller state kept and
// stateless v0.21 launchd does not. Only a manifest genuinely absent from a
// scan that reported no skip for it makes its agent a stray. It runs before the
// record is replaced, so a removal that fails leaves its label recorded for the
// next run to retry.
func sweepStrayAgents(ctx context.Context, recorded, keep, retained []string) error {
	var errs []error
	for _, label := range recorded {
		if slices.Contains(keep, label) || slices.Contains(retained, label) {
			continue
		}
		if err := removeAgent(ctx, label); err != nil {
			errs = append(errs, fmt.Errorf("remove stray agent %q: %w", label, err))
			continue
		}
		if err := removeStagedProgram(label); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// retainedLabels names the agents an unloadable manifest leaves standing: the
// helper label each skipped consumer would have produced, kept only where the
// record already names it. A file that will not decode means its consumer is
// registered but stale, not removed, so its agent goes on serving under the
// registration it already has. The record keeps naming it too — dropping it
// would strand the agent, since a later run could no longer sweep it once that
// consumer really does unregister. A stem that is not a canonical service name,
// or one no record names, stands for no installed agent and is left out.
func retainedLabels(skipped []manifest.Skipped, recorded []string) []string {
	labels := make([]string, 0, len(skipped))
	for _, s := range skipped {
		label, err := serviceidentity.HelperLabel(s.Name)
		if err != nil || !slices.Contains(recorded, label) {
			continue
		}
		labels = append(labels, label)
	}
	return labels
}

// ensureServe makes the serve daemon be this build, ready and serving: Ensure
// places the stable executable, writes the plist for its own label, and evicts
// the incumbent it replaces.
func ensureServe(ctx context.Context) error {
	spec, err := stableServeSpec()
	if err != nil {
		return err
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		return err
	}
	ensureCtx, cancel := context.WithTimeout(ctx, ensureDeadline)
	defer cancel()
	if _, err := client.Ensure(ensureCtx); err != nil {
		return fmt.Errorf("ensure serve daemon: %w", err)
	}
	return nil
}

// settleServe proves the booted-out serve daemon left the process table. It
// reads no Program: Settle observes the owner record and the process table, and
// no owner record names nobody to settle — exactly what uninstalling a system
// that never served looks like.
func settleServe(ctx context.Context) error {
	client, err := daemonkit.Open(clientSpec())
	if err != nil {
		return err
	}
	settleCtx, cancel := context.WithTimeout(ctx, settleDeadline)
	defer cancel()
	_, err = client.Settle(settleCtx, daemonkit.Expect{})
	if err != nil && !errors.Is(err, daemonkit.ErrUnrecorded) {
		return fmt.Errorf("settle serve daemon: %w", err)
	}
	return nil
}

// installedAgents is the label set the last install converged.
type installedAgents struct {
	Identity string   `json:"identity"`
	Labels   []string `json:"labels"`
}

// Validate refuses any label outside synckit's own namespace: this record names
// the agents uninstall and the stray sweep remove, so a foreign label in it
// would aim a removal at a third party's agent.
func (a installedAgents) Validate() error {
	if a.Identity != installedAgentsIdentity {
		return fmt.Errorf("installed agents identity is %q, want %q", a.Identity, installedAgentsIdentity)
	}
	for _, label := range a.Labels {
		if err := launchd.ValidateLabel(label); err != nil {
			return err
		}
		if !synckitLabel(label) {
			return fmt.Errorf("installed agent label %q is outside %q", label, labelPrefix)
		}
	}
	return nil
}

func synckitLabel(label string) bool { return strings.HasPrefix(label, labelPrefix+".") }

func installedAgentsPath() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create synckit config dir: %w", err)
	}
	return filepath.Join(dir, installedAgentsFile), nil
}

func readInstalledAgents() ([]string, error) {
	path, err := installedAgentsPath()
	if err != nil {
		return nil, err
	}
	record, err := durable.ReadFile[installedAgents](path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read installed agents: %w", err)
	}
	return record.Labels, nil
}

func writeInstalledAgents(labels []string) error {
	path, err := installedAgentsPath()
	if err != nil {
		return err
	}
	data, err := durable.Marshal(installedAgents{Identity: installedAgentsIdentity, Labels: labels})
	if err != nil {
		return fmt.Errorf("encode installed agents: %w", err)
	}
	if err := durable.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write installed agents: %w", err)
	}
	return nil
}

func removeInstalledAgents() error {
	path, err := installedAgentsPath()
	if err != nil {
		return err
	}
	if err := durable.Remove(path); err != nil {
		return fmt.Errorf("remove installed agents: %w", err)
	}
	return nil
}

// execLaunchctl runs one /bin/launchctl invocation to completion. An exit code
// is an answer, not a failure; only a launchctl that could not run at all is an
// error, and it carries no status for launchd's classifier to decode.
func execLaunchctl(ctx context.Context, path string, args ...string) (string, int, error) {
	//nolint:gosec // G204: launchd.Runner's boundary; launchd itself passes its own binary path and fixed argv.
	out, err := exec.CommandContext(ctx, path, args...).CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}
