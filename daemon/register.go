package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
)

// reloadTimeout bounds the best-effort reload nudge to a running daemon.
const reloadTimeout = 5 * time.Second

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
			dest, err := hostregistry.RegisteredPath(m.Name)
			if err != nil {
				return err
			}
			if _, err := ensureManifestsDir(); err != nil {
				return err
			}
			raw, err := os.ReadFile(args[0]) //nolint:gosec // G304: path is the manifest the user explicitly asked to register; it is read back after manifest.Load already validated it.
			if err != nil {
				return fmt.Errorf("read manifest %q: %w", args[0], err)
			}
			//nolint:gosec // G703: RegisteredPath refuses any name that is not a plain base name, so dest stays within the manifests dir.
			if err := os.WriteFile(dest, raw, 0o600); err != nil {
				return fmt.Errorf("install manifest to %s: %w", dest, err)
			}
			cmd.Println("registered manifest " + m.Name)
			nudgeReload(cmd.Context())
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
			name := args[0]
			path, err := hostregistry.RegisteredPath(name)
			if err != nil {
				return err
			}
			//nolint:gosec // G703: RegisteredPath refuses any name that is not a plain base name, so path stays within the manifests dir.
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove manifest %s: %w", path, err)
			}
			cmd.Println("unregistered manifest " + name)
			nudgeReload(cmd.Context())
			return nil
		},
	}
}

// nudgeReload asks a running daemon to re-discover manifests. It is best-effort: a
// daemon that is down (no socket) is not an error, since the next serve start
// discovers the change anyway.
func nudgeReload(ctx context.Context) {
	client, err := daemonClient()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, reloadTimeout)
	defer cancel()
	defer func() { _ = client.Close() }()
	_, _ = client.Call(ctx, &rpc.Request{Method: "reload"})
}
