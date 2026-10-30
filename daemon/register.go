package daemon

import (
	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/registration"
)

func newRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register <manifest.json>",
		Short: "Validate a consumer manifest, install it, and nudge the daemon to reload.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			m, err := manifest.Load(args[0])
			if err != nil {
				return err
			}
			if err := registration.RegisterManifest(cmd.Context(), *m); err != nil {
				return err
			}
			cmd.Println("registered manifest " + m.Name)
			return nil
		},
	}
}

func newUnregisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unregister <name>",
		Short: "Remove a consumer manifest and nudge the daemon to reload.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := registration.UnregisterManifest(cmd.Context(), args[0]); err != nil {
				return err
			}
			cmd.Println("unregistered manifest " + args[0])
			return nil
		},
	}
}
