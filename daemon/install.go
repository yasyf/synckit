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
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/runtimeowner"
	"github.com/yasyf/synckit/rpc"
)

const (
	serviceWorkerLimit  = 1
	serviceCloseTimeout = 30 * time.Second
)

type serviceController interface {
	Converge(context.Context, []dkservice.Agent) error
	Status(context.Context, string) (dkservice.Status, error)
	StopRuntime(context.Context, dkservice.StopControlSpec) (wire.StopResult, error)
	Close(context.Context) error
}

var observeRuntimeHealth = func(ctx context.Context, sock string) (rpc.RuntimeHealth, error) {
	client := rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild})
	defer func() { _ = client.Close() }()
	return client.RuntimeHealth(ctx)
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
	processPath, err := runtimeowner.ServiceProcessPath()
	if err != nil {
		return dkservice.ControllerConfig{}, err
	}
	return dkservice.ControllerConfig{
		StatePath:   filepath.Join(dir, "services.db"),
		ProcessPath: processPath,
		WorkerLimit: serviceWorkerLimit,
	}, nil
}

func newInstallCmd(build string) *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install the synckitd LaunchAgents (reconcile tick, serve daemon, consumer helpers).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := install(cmd.Context(), build); err != nil {
				return err
			}
			cmd.Println("Installed synckitd agents.")
			return nil
		},
	}
}

func newUninstallCmd(build string) *cobra.Command {
	return &cobra.Command{
		Use:   "uninstall",
		Short: "Remove the synckitd LaunchAgents.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := uninstall(cmd.Context(), build); err != nil {
				return err
			}
			cmd.Println("Uninstalled synckitd agents.")
			return nil
		},
	}
}

func newStopControlCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "stop-control <socket>",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := dkservice.RunStopControlChild(cmd.Context(), runtimeowner.StopControlClientConfig(args[0]))
			return err
		},
	}
}

func install(ctx context.Context, build string) error {
	return withServiceController(ctx, func(controller serviceController) error {
		manifests, err := discoverManifests()
		if err != nil {
			return err
		}
		agents, err := serviceAgents(manifests)
		if err != nil {
			return err
		}
		if err := stopLoadedRuntime(ctx, controller, build, wire.StopIntentUpgrade); err != nil {
			return err
		}
		return controller.Converge(ctx, agents)
	})
}

func uninstall(ctx context.Context, build string) error {
	return withServiceController(ctx, func(controller serviceController) error {
		if err := stopLoadedRuntime(ctx, controller, build, wire.StopIntentUninstall); err != nil {
			return err
		}
		return controller.Converge(ctx, nil)
	})
}

func stopLoadedRuntime(ctx context.Context, controller serviceController, build string, intent wire.StopIntent) error {
	status, err := controller.Status(ctx, labelPrefix+".serve")
	if err != nil {
		return fmt.Errorf("inspect synckitd runtime service: %w", err)
	}
	if !status.Loaded {
		return nil
	}
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		return err
	}
	health, err := observeRuntimeHealth(ctx, sock)
	if err != nil {
		return fmt.Errorf("observe loaded synckitd runtime: %w", err)
	}
	if health.RuntimeBuild == "" || health.RuntimeProtocol != int(rpc.Version) || health.ProcessGeneration == "" || health.PID <= 1 {
		return errors.New("loaded synckitd runtime returned an incomplete identity")
	}
	if intent == wire.StopIntentUpgrade && health.RuntimeBuild == build {
		intent = wire.StopIntentRestart
	}
	spec, err := runtimeowner.StopControlSpec(sock, build, health.ProcessGeneration, intent)
	if err != nil {
		return err
	}
	_, err = controller.StopRuntime(ctx, spec)
	if err != nil {
		return fmt.Errorf("stop loaded synckitd runtime: %w", err)
	}
	return nil
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
