package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	dkservice "github.com/yasyf/daemonkit/service"

	"github.com/yasyf/synckit/hostregistry"
)

const (
	serviceWorkerLimit  = 1
	serviceCloseTimeout = 30 * time.Second
)

type serviceController interface {
	Converge(context.Context, []dkservice.Agent) error
	Close(context.Context) error
}

var openServiceController = func(ctx context.Context) (serviceController, error) {
	config, err := serviceControllerConfig()
	if err != nil {
		return nil, err
	}
	return dkservice.NewController(ctx, config)
}

func serviceControllerConfig() (dkservice.ControllerConfig, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return dkservice.ControllerConfig{}, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return dkservice.ControllerConfig{}, fmt.Errorf("create synckit config dir: %w", err)
	}
	return dkservice.ControllerConfig{
		StatePath:   filepath.Join(dir, "services.db"),
		ProcessPath: filepath.Join(dir, "service-processes.db"),
		WorkerLimit: serviceWorkerLimit,
	}, nil
}

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

func install(ctx context.Context) error {
	return withServiceController(ctx, func(controller serviceController) error {
		manifests, err := discoverManifests()
		if err != nil {
			return err
		}
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("resolve synckitd executable: %w", err)
		}
		agents, err := serviceAgents(manifests, executable)
		if err != nil {
			return err
		}
		return controller.Converge(ctx, agents)
	})
}

func uninstall(ctx context.Context) error {
	return withServiceController(ctx, func(controller serviceController) error {
		return controller.Converge(ctx, nil)
	})
}

func withServiceController(ctx context.Context, run func(serviceController) error) (err error) {
	controller, err := openServiceController(ctx)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), serviceCloseTimeout)
		defer cancel()
		err = errors.Join(err, controller.Close(closeCtx))
	}()
	return run(controller)
}
