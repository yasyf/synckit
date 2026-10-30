package registration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/daemonkit/durable"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/servespec"
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
	dir, err := hostregistry.ManifestsDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create manifests dir %s: %w", dir, err)
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

// UnregisterManifest removes a consumer registration and asks the running daemon
// to reload. An absent registration or unavailable daemon is already settled.
func UnregisterManifest(ctx context.Context, name string) error {
	path, err := hostregistry.RegisteredPath(name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove manifest %s: %w", path, err)
	}
	nudgeReload(ctx)
	return nil
}

func nudgeReload(ctx context.Context) {
	client, err := servespec.Dial()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, reloadTimeout)
	defer cancel()
	defer func() { _ = client.Close() }()
	_, _ = client.Call(ctx, &rpc.Request{Method: "reload"})
}
