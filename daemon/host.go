package daemon

import (
	"context"
	"encoding/json"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
)

func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "host",
		Short: "Manage the shared host mesh.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newHostAddCmd(), newHostAddrCmd(), newHostLsCmd(), newHostRmCmd(), newHostVerifyCmd())
	return cmd
}

func newHostAddrCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "addr",
		Short: "Manage a peer's alternate SSH dial addresses.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newHostAddrAddCmd())
	return cmd
}

func newHostAddrAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <target> <addr>",
		Short: "Record a LAN/.local dial address tried before target's tailnet FQDN.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := hostregistry.Mesh.AddAddr(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			cmd.Printf("recorded dial address %s for %s\n", args[1], args[0])
			return nil
		},
	}
}

func newHostAddCmd() *cobra.Command {
	var (
		self      string
		noRecurse bool
	)
	cmd := &cobra.Command{
		Use:   "add <user@node>",
		Short: "Register a peer and SSH-bootstrap the mesh on it.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			manifests, err := discoverManifests()
			if err != nil {
				return err
			}
			return AddHost(cmd.Context(), hostregistry.NewExecRunner(), manifests, args[0], self, noRecurse, func(msg string) {
				cmd.Println(msg)
			})
		},
	}
	cmd.Flags().StringVar(&self, "self", "", "Override the detected ssh target by which peers reach this machine.")
	cmd.Flags().BoolVar(&noRecurse, "no-recurse", false, "Register the peer only; skip the remote bootstrap.")
	return cmd
}

func newHostLsCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List the self identity and registered peers.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			reg, err := hostregistry.Mesh.Load()
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(reg)
			}
			cmd.Println("self: " + reg.Self)
			for _, h := range reg.Hosts {
				cmd.Println("host: " + h)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, `Print {"self":...,"hosts":[...]} as JSON.`)
	return cmd
}

func newHostRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <target>",
		Short: "Unregister a peer from the mesh.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := hostregistry.Mesh.RemoveHost(cmd.Context(), args[0]); err != nil {
				return err
			}
			cmd.Println("removed host " + args[0])
			return nil
		},
	}
}

func newHostVerifyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "verify",
		Short: "Probe every peer for reachability and each consumer's install state.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return verifyHosts(cmd.Context(), cmd, hostregistry.NewExecRunner())
		},
	}
}

func verifyHosts(ctx context.Context, cmd *cobra.Command, r hostregistry.Runner) error {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return err
	}
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}
	for _, target := range reg.Hosts {
		for _, m := range manifests {
			res := hostregistry.Mesh.VerifyBinary(ctx, r, target, m.Binary)
			printVerify(cmd, m.Binary, res)
		}
	}
	return nil
}

func printVerify(cmd *cobra.Command, binary string, res hostregistry.VerifyResult) {
	switch {
	case res.Err != nil:
		cmd.Printf("%s [%s]: unreachable: %v\n", res.Target, binary, res.Err)
	case res.Bootstrapped:
		cmd.Printf("%s [%s]: installed %s\n", res.Target, binary, res.Version)
	case res.Reachable:
		cmd.Printf("%s [%s]: reachable, not installed\n", res.Target, binary)
	default:
		cmd.Printf("%s [%s]: unknown\n", res.Target, binary)
	}
}
