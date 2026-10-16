package daemon

import (
	"fmt"
	"os"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

// ensureManifestsDir returns the manifests directory, creating it if absent.
func ensureManifestsDir() (string, error) {
	dir, err := hostregistry.ManifestsDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create manifests dir %s: %w", dir, err)
	}
	return dir, nil
}

// discoverManifests loads every manifest registered under the manifests dir,
// dropping the record of which files were skipped. A caller that converges
// launchd agents needs that record — it is what separates a consumer whose file
// is stale from one that unregistered — and uses discoverScan instead.
func discoverManifests() ([]manifest.Manifest, error) {
	manifests, _, err := discoverScan()
	return manifests, err
}

// discoverScan loads every manifest registered under the manifests dir and
// reports the ones that would not load.
func discoverScan() ([]manifest.Manifest, []manifest.Skipped, error) {
	dir, err := hostregistry.ManifestsDir()
	if err != nil {
		return nil, nil, err
	}
	manifests, skipped, err := manifest.Discover(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("discover manifests: %w", err)
	}
	return manifests, skipped, nil
}
