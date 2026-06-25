package daemon

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/service"
)

// newLauncher builds the launchctl boundary install/uninstall drive. It is a
// package var so a test can swap in a fake launcher and assert the rendered agents
// without loading a real LaunchAgent; production uses launchctl.
var newLauncher = service.NewLauncher

func newInstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install the synckitd LaunchAgents (reconcile tick, serve daemon, consumer helpers) and supersede legacy per-tool agents.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := install(cmd.Context(), newLauncher()); err != nil {
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
			if err := uninstall(cmd.Context(), newLauncher()); err != nil {
				return err
			}
			cmd.Println("Uninstalled synckitd agents.")
			return nil
		},
	}
}

// install installs the synckitd agents for every discovered manifest, then boots
// out the legacy per-tool agents the one shared daemon supersedes.
func install(ctx context.Context, launcher service.Launcher) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	cfg := toolConfig(manifests)
	if err := service.NewLaunchdManager(launcher).Install(ctx, cfg, false); err != nil {
		return err
	}
	for _, label := range legacyLabels {
		if err := launcher.Bootout(ctx, label); err != nil {
			return fmt.Errorf("bootout legacy agent %s: %w", label, err)
		}
	}
	return nil
}

// uninstall removes the synckitd agents for every discovered manifest.
func uninstall(ctx context.Context, launcher service.Launcher) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	return service.NewLaunchdManager(launcher).Uninstall(ctx, toolConfig(manifests))
}
