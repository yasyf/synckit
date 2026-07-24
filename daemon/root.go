// Package daemon wires the synckitd cobra command tree and implements the generic
// per-machine sync daemon: it owns the one shared host mesh, rpc socket, reconcile
// tick, and watch supervisor, discovers declarative JSON manifests under
// ~/.config/synckit/manifests, and drives each consumer's typed sync service
// (list, reconcile, sync) over the transport its manifest declares — a unix
// socket, a spawned child's stdio, or ssh to a peer. It never imports a consumer —
// every consumer specific is read from a manifest.
package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/version"
)

// Execute builds and runs the synckitd root command under a context canceled on
// SIGINT/SIGTERM, exiting non-zero on error.
func Execute(stampedVersion string) {
	// The serving daemon re-execs this binary as daemonkit's trust-verifier child
	// for every connecting peer and for the serve-time self-probe; without this
	// dispatch the daemon refuses to start and every peer is rejected as untrusted.
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root := newRoot(version.Running(stampedVersion))
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "synckitd: %v\n", err)
		os.Exit(1)
	}
}

// newRoot builds the synckitd command tree; callers Execute it.
func newRoot(build string) *cobra.Command {
	root := &cobra.Command{
		Use:           "synckitd",
		Short:         "Generic per-machine sync daemon for synckit consumers.",
		Version:       build,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.AddCommand(
		newServeCmd(build),
		newReconcileCmd(),
		newHostCmd(),
		newRegisterCmd(),
		newUnregisterCmd(),
		newStatusCmd(),
		newInstallCmd(build),
		newUninstallCmd(build),
		newConsentCmd(),
		newRPCServeV1Cmd(),
	)
	return root
}
