package daemon

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// statusDialTimeout bounds the liveness probe against the daemon socket.
const statusDialTimeout = 2 * time.Second

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print the mesh, registered manifests, socket path, and daemon liveness.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			reg, err := hostregistry.Mesh.Load()
			if err != nil {
				return err
			}
			manifests, err := discoverManifests()
			if err != nil {
				return err
			}
			sock, err := hostregistry.Mesh.SockPath()
			if err != nil {
				return err
			}

			cmd.Println("self: " + reg.Self)
			for _, h := range reg.Hosts {
				cmd.Println("host: " + h)
			}
			for _, m := range manifests {
				cmd.Printf("manifest: %s (binary=%s backend=%s)\n", m.Name, m.Binary, m.Watch.Backend)
			}
			cmd.Println("socket: " + sock)
			if daemonLive(cmd.Context(), sock) {
				cmd.Println("daemon: running")
				return nil
			}
			cmd.Println("daemon: not running")
			return nil
		},
	}
}

// daemonLive probes the daemon's status method over the socket, reporting whether
// it answered.
func daemonLive(ctx context.Context, sock string) bool {
	ctx, cancel := context.WithTimeout(ctx, statusDialTimeout)
	defer cancel()
	resp, err := rpc.Call(ctx, sock, &rpc.Request{Method: "status"})
	return err == nil && resp.OK
}

// handleStatus is the daemon's "status" rpc handler: it reports that the daemon is
// up plus the mesh and manifest counts a probe cares about.
func handleStatus(_ context.Context, _ map[string]any) (any, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, err
	}
	manifests, err := discoverManifests()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"self":      reg.Self,
		"hosts":     reg.Hosts,
		"manifests": len(manifests),
	}, nil
}
