package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/paths"
	"github.com/yasyf/daemonkit/version"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// statusDialTimeout bounds the liveness probe against the daemon socket.
const statusDialTimeout = 2 * time.Second

func newStatusCmd(build string) *cobra.Command {
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
			running, live, err := daemonVersion(cmd.Context())
			if err != nil {
				return err
			}
			if !live {
				cmd.Println("daemon: not running")
				return nil
			}
			cmd.Println(daemonLine(build, running))
			return nil
		},
	}
}

func daemonVersion(ctx context.Context) (string, bool, error) {
	client, err := daemonClient()
	if err != nil {
		return "", false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, statusDialTimeout)
	defer cancel()
	defer func() { _ = client.Close() }()
	resp, err := client.Call(ctx, &rpc.Request{Method: "status"})
	if err != nil || !resp.OK {
		return "", false, nil
	}
	var reply struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(resp.Result, &reply); err != nil {
		return "", true, fmt.Errorf("decode daemon status: %w", err)
	}
	return reply.Version, true, nil
}

func daemonLine(installed, running string) string {
	if version.Equal(installed, running) {
		return "daemon: running " + running
	}
	if running == "" {
		running = "an unversioned build"
	}
	return fmt.Sprintf("daemon: running %s, but the installed synckitd is %s; the next reconcile tick restages it, or run synckitd install", running, installed)
}

func statusHandler(build string) func(context.Context, map[string]any) (any, error) {
	return func(context.Context, map[string]any) (any, error) {
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
			"version":   build,
		}, nil
	}
}
