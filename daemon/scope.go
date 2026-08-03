package daemon

import (
	"context"
	"fmt"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/clirunner"
	"github.com/yasyf/synckit/internal/synctransport"
)

// processScope is the durable process ownership every command path runs under:
// one-shot commands for the ssh and tailscale legs, long-lived children for the
// spawned and remote service transports. Both daemonkit.Ctx (the daemon's own
// scope, handed over by Serve) and *daemonkit.Owned (a CLI's) satisfy it, so
// the reconcile tick and the resident daemon share one code path.
type processScope interface {
	hostregistry.Commander
	synctransport.Spawner
}

func withCLIProcessScope(ctx context.Context, run func(*daemonkit.Owned) error) error {
	dir, err := hostregistryDir()
	if err != nil {
		return err
	}
	return clirunner.WithOwned(ctx, dir, run)
}

func withCLIExecRunner(ctx context.Context, run func(hostregistry.Runner) error) error {
	return hostregistry.WithExecRunner(ctx, run)
}

func hostregistryDir() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit state directory: %w", err)
	}
	return dir, nil
}
