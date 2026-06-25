package daemon

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

const manifestsDirName = "manifests"

// manifestsDir returns ~/.config/synckit/manifests, the directory consumers
// register their manifests under.
func manifestsDir() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, manifestsDirName), nil
}

// ensureManifestsDir returns the manifests directory, creating it if absent.
func ensureManifestsDir() (string, error) {
	dir, err := manifestsDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create manifests dir %s: %w", dir, err)
	}
	return dir, nil
}

// discoverManifests loads every manifest registered under the manifests dir.
func discoverManifests() ([]manifest.Manifest, error) {
	dir, err := manifestsDir()
	if err != nil {
		return nil, err
	}
	manifests, err := manifest.Discover(dir)
	if err != nil {
		return nil, fmt.Errorf("discover manifests: %w", err)
	}
	return manifests, nil
}
