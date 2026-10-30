package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/durable"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
)

const reloadTimeout = 5 * time.Second

// RegisterManifest atomically installs a validated consumer manifest and asks the
// running local daemon to reload it. An unavailable daemon is not an error;
// its next start discovers the registration.
func RegisterManifest(ctx context.Context, m manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	dest, err := hostregistry.RegisteredPath(m.Name)
	if err != nil {
		return err
	}
	if _, err := ensureManifestsDir(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest %s: %w", m.Name, err)
	}
	if err := durable.WriteFile(dest, append(raw, '\n'), 0o600); err != nil {
		return fmt.Errorf("install manifest to %s: %w", dest, err)
	}
	nudgeReload(ctx)
	return nil
}

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
			if err := RegisterManifest(cmd.Context(), *m); err != nil {
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
