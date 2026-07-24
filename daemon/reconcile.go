package daemon

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"

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
			return withCLIProcessOwner(cmd.Context(), func(_ *worker.Pool, children *proc.Manager) error {
				results, err := reconcileAll(cmd.Context(), children)
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
func reconcileAll(ctx context.Context, children *proc.Manager) ([]reconcileResult, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, fmt.Errorf("load mesh: %w", err)
	}
	manifests, err := discoverManifests()
	if err != nil {
		return nil, err
	}
	directory, err := hostregistry.Mesh.Dir()
	if err != nil {
		return nil, err
	}
	delivery := newDeliveryStore(directory)
	results := make([]reconcileResult, 0, len(manifests))
	for _, m := range manifests {
		results = append(results, reconcileOne(ctx, children, m, reg, delivery))
	}
	return results, nil
}

// reconcileOne runs a full reconcile against the consumer's exact-build typed
// service. Any failure is captured in the result's Err rather than returned, so a
// per-consumer fault never aborts the others.
func reconcileOne(
	ctx context.Context,
	children *proc.Manager,
	m manifest.Manifest,
	registry *hostregistry.Registry,
	delivery *deliveryStore,
) reconcileResult {
	c := syncservice.NewClient(dialTransport(children, m, registry.Self, registry.Self))
	defer func() { _ = c.Close() }()

	if _, err := c.Reconcile(ctx, ""); err != nil {
		return reconcileResult{Name: m.Name, Err: err.Error()}
	}
	notifier := manifestNotifier{local: c, m: m, self: registry.Self, children: children, delivery: delivery}
	for _, peer := range registry.Hosts {
		if err := notifier.Notify(ctx, peer, ""); err != nil {
			return reconcileResult{Name: m.Name, Err: err.Error()}
		}
	}
	return reconcileResult{Name: m.Name}
}
