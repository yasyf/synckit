package daemon

import (
	"context"

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
		Short: "Install the synckitd LaunchAgents (reconcile tick, serve daemon, consumer helpers).",
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

// install installs the synckitd agents for every discovered manifest.
func install(ctx context.Context, launcher service.Launcher) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	cfg := toolConfig(manifests)
	return service.NewLaunchdManager(launcher).Install(ctx, cfg, false)
}

// uninstall removes the synckitd agents for every discovered manifest.
func uninstall(ctx context.Context, launcher service.Launcher) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	return service.NewLaunchdManager(launcher).Uninstall(ctx, toolConfig(manifests))
}
