package meshtrust

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/yasyf/synckit/hostregistry"
)

// tailscaleFallbacks are the install locations searched when the CLI is not on
// PATH; a daemon's spawn environment often lacks the shell's.
var tailscaleFallbacks = []string{
	"/Applications/Tailscale.app/Contents/MacOS/Tailscale",
	"/opt/homebrew/bin/tailscale",
	"/usr/local/bin/tailscale",
}

// StatePath returns the mesh state file this machine's trust derives from.
func StatePath() (string, error) {
	path, err := hostregistry.Mesh.Path()
	if err != nil {
		return "", fmt.Errorf("resolve mesh state path: %w", err)
	}
	return path, nil
}

func loadRegistry() (registry, error) {
	g, err := hostregistry.Mesh.Load()
	if err != nil {
		return registry{}, fmt.Errorf("load mesh registry: %w", err)
	}
	return registry{Self: g.Self, Hosts: g.Hosts}, nil
}

func tailscaleBinary() (string, error) {
	if path, err := exec.LookPath("tailscale"); err == nil {
		return path, nil
	}
	for _, path := range tailscaleFallbacks {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return path, nil
		}
	}
	return "", fmt.Errorf("locate tailscale: not on PATH, nor at %v", tailscaleFallbacks)
}

func tailscaleStatus(ctx context.Context) ([]byte, error) {
	binary, err := tailscaleBinary()
	if err != nil {
		return nil, err
	}
	out, err := exec.CommandContext(ctx, binary, "status", "--json").Output() //nolint:gosec // G204: fixed argv against a binary resolved from PATH and the known install locations.
	if err != nil {
		return nil, fmt.Errorf("tailscale status: %w", err)
	}
	return out, nil
}
