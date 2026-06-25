// Package daemon wires the synckitd cobra command tree and implements the generic
// per-machine sync daemon: it owns the one shared host mesh, rpc socket, reconcile
// tick, and watch supervisor, discovers declarative JSON manifests under
// ~/.config/synckit/manifests, and shells out to each consumer's CLI for the
// domain actions (list, reconcile, sync) those manifests declare. It never imports
// a consumer — every consumer specific is read from a manifest and run as its
// binary.
package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// Execute builds and runs the synckitd root command under a context canceled on
// SIGINT/SIGTERM, exiting non-zero on error.
func Execute(version string) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root := newRoot(version)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "synckitd: %v\n", err)
		os.Exit(1)
	}
}

// newRoot builds the synckitd command tree; callers Execute it.
func newRoot(version string) *cobra.Command {
	root := &cobra.Command{
		Use:           "synckitd",
		Short:         "Generic per-machine sync daemon for synckit consumers.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.AddCommand(
		newServeCmd(),
		newReconcileCmd(),
		newHostCmd(),
		newRegisterCmd(),
		newUnregisterCmd(),
		newStatusCmd(),
		newInstallCmd(),
		newUninstallCmd(),
	)
	return root
}
