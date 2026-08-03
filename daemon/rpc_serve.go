package daemon

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/paths"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
)

func newRPCServeV1Cmd() *cobra.Command {
	return &cobra.Command{
		Use:    rpc.RemoteServeCommand + " <service-id>",
		Short:  "Bridge one authenticated remote RPC session.",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return serveRemoteRPC(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), args[0])
		},
	}
}

func serveRemoteRPC(ctx context.Context, in io.Reader, out io.Writer, serviceID string) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	var selected *manifest.Manifest
	for i := range manifests {
		if manifests[i].Name == serviceID {
			selected = &manifests[i]
			break
		}
	}
	if selected == nil {
		return fmt.Errorf("rpc-serve-v1: service %q is not registered", serviceID)
	}
	if selected.Service.Kind != "resident" {
		return fmt.Errorf("rpc-serve-v1: service %q is not resident", serviceID)
	}
	socket, err := residentSocket(serviceID)
	if err != nil {
		return err
	}
	if err := rpc.ServeRemoteHello(in, out); err != nil {
		return err
	}
	return rpc.Proxy(ctx, in, out, socket)
}

// residentSocket is where a resident consumer's helper binds: the same
// label-derived path helperruntime serves on and syncservice.Resident opens, so
// the bridge and the local lane can never disagree about it.
func residentSocket(serviceID string) (string, error) {
	label, err := serviceidentity.HelperLabel(serviceID)
	if err != nil {
		return "", fmt.Errorf("rpc-serve-v1: service %q: %w", serviceID, err)
	}
	socket, err := paths.Socket(label)
	if err != nil {
		return "", fmt.Errorf("rpc-serve-v1: resolve %q socket: %w", serviceID, err)
	}
	return socket, nil
}
