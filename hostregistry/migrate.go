package hostregistry

import (
	"context"
	"fmt"
)

// MigrateLegacyMesh seeds the shared Mesh from pre-cutover per-tool registries.
// It is idempotent: when Mesh already has a Self it returns nil immediately.
// Otherwise it scans legacyNames in order, takes Self from the first one whose
// registry carries a non-empty Self, and folds the union of every scanned
// legacy registry's Hosts into Mesh. When no legacy registry is populated it is
// a no-op.
func MigrateLegacyMesh(ctx context.Context, legacyNames ...string) error {
	current, err := Mesh.Load()
	if err != nil {
		return fmt.Errorf("load mesh: %w", err)
	}
	if current.Self != "" {
		return nil
	}
	var (
		self  string
		hosts []string
	)
	for _, name := range legacyNames {
		legacy, err := Config{Name: name}.Load()
		if err != nil {
			return fmt.Errorf("load legacy %s: %w", name, err)
		}
		if self == "" {
			self = legacy.Self
		}
		hosts = append(hosts, legacy.Hosts...)
	}
	if self == "" {
		return nil
	}
	if _, err := Mesh.Update(ctx, func(g *Registry) error {
		g.Self = self
		for _, h := range hosts {
			g.UpsertHost(h)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("seed mesh from legacy: %w", err)
	}
	return nil
}
