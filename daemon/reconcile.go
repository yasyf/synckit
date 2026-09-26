package daemon

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/syncservice"
)

func newReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Run one convergent reconcile pass for every registered consumer.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withCLIProcessScope(cmd.Context(), func(owned *daemonkit.Owned) error {
				results, err := reconcileAll(cmd.Context(), owned)
				if err != nil {
					return err
				}
				for _, res := range results {
					if res.Err != "" {
						cmd.Printf("%s: error: %s\n", res.Name, res.Err)
						continue
					}
					cmd.Printf("%s: reconciled\n", res.Name)
				}
				return delivery.Kick(cmd.Context(), "", "")
			})
		},
	}
}

// reconcileResult summarizes one consumer's reconcile pass for the tick output and
// the rpc reconcile response.
type reconcileResult struct {
	Name string `json:"name"`
	Err  string `json:"err,omitempty"`
}

func reconcileAll(ctx context.Context, scope processScope) ([]reconcileResult, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, fmt.Errorf("load mesh: %w", err)
	}
	manifests, err := discoverManifests()
	if err != nil {
		return nil, err
	}
	results := make([]reconcileResult, 0, len(manifests))
	for _, m := range manifests {
		results = append(results, reconcileOne(ctx, scope, m, reg))
	}
	return results, nil
}

func reconcileOne(
	ctx context.Context,
	scope processScope,
	m manifest.Manifest,
	registry *hostregistry.Registry,
) reconcileResult {
	c := syncservice.NewClient(dialTransport(scope, m, registry.Self, registry.Self))
	defer func() { _ = c.Close() }()

	if _, err := c.Reconcile(ctx, ""); err != nil {
		return reconcileResult{Name: m.Name, Err: err.Error()}
	}
	return reconcileResult{Name: m.Name}
}
