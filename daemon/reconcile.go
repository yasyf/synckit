package daemon

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/supervise"

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
			return withCLIProcessOwner(cmd.Context(), func(pool *supervise.Pool) error {
				results, err := reconcileAll(cmd.Context(), pool)
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
				return nil
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

// reconcileAll discovers every manifest and drives each consumer's typed
// reconcile over its local sync service — convergence happens in the consumer,
// which pull-merges its peers from the mesh internally. A per-consumer failure
// is captured in its result, never aborting the others.
func reconcileAll(ctx context.Context, pool *supervise.Pool) ([]reconcileResult, error) {
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
		results = append(results, reconcileOne(ctx, pool, m, reg.Self))
	}
	return results, nil
}

// reconcileOne runs a full reconcile against the consumer's exact-build typed
// service. Any failure is captured in the result's Err rather than returned, so a
// per-consumer fault never aborts the others.
func reconcileOne(ctx context.Context, pool *supervise.Pool, m manifest.Manifest, self string) reconcileResult {
	c := syncservice.NewClient(dialTransport(pool, m, self, self))
	defer func() { _ = c.Close() }()

	if _, err := c.Reconcile(ctx, ""); err != nil {
		return reconcileResult{Name: m.Name, Err: err.Error()}
	}
	return reconcileResult{Name: m.Name}
}
