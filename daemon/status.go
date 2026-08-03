package daemon

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/paths"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// statusDialTimeout bounds the liveness probe against the daemon socket.
const statusDialTimeout = 2 * time.Second

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print the mesh, registered manifests, label-derived socket paths, and daemon liveness.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			reg, err := hostregistry.Mesh.Load()
			if err != nil {
				return err
			}
			manifests, skipped, err := discoverScan()
			if err != nil {
				return err
			}
			socket, err := paths.Socket(serveLabel)
			if err != nil {
				return err
			}
			cmd.Println("self: " + reg.Self)
			for _, h := range reg.Hosts {
				cmd.Println("host: " + h)
			}
			for _, m := range manifests {
				cmd.Printf("manifest: %s (binary=%s)\n", m.Name, m.Binary)
				if m.Service.Kind != "resident" {
					continue
				}
				helper, err := residentSocket(m.Name)
				if err != nil {
					return err
				}
				cmd.Printf("  socket: %s\n", helper)
			}
			for _, s := range skipped {
				cmd.Printf("manifest: %s (skipped: %v)\n", s.Name, s.Err)
			}
			cmd.Println("label: " + serveLabel)
			cmd.Println("socket: " + socket)
			if daemonLive(cmd.Context()) {
				cmd.Println("daemon: running")
				return nil
			}
			cmd.Println("daemon: not running")
			return nil
		},
	}
}

// daemonLive probes the daemon's status method over its business lane,
// reporting whether it answered.
func daemonLive(ctx context.Context) bool {
	client, err := daemonClient()
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, statusDialTimeout)
	defer cancel()
	defer func() { _ = client.Close() }()
	resp, err := client.Call(ctx, &rpc.Request{Method: "status"})
	return err == nil && resp.OK
}

// handleStatus is the daemon's "status" rpc handler: it reports that the daemon is
// up plus the mesh and manifest counts a probe cares about.
func handleStatus(_ context.Context, _ map[string]any) (any, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, err
	}
	manifests, skipped, err := discoverScan()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"self":      reg.Self,
		"hosts":     reg.Hosts,
		"manifests": len(manifests),
		"skipped":   len(skipped),
	}, nil
}
