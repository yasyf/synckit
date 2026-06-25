package daemon

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

func newReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Run one convergent reconcile pass for every registered consumer.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			results, err := reconcileAll(cmd.Context(), hostregistry.NewExecRunner())
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
		},
	}
}

// reconcileResult summarizes one consumer's reconcile pass for the tick output and
// the rpc reconcile response.
type reconcileResult struct {
	Name string `json:"name"`
	Err  string `json:"err,omitempty"`
}

// reconcileAll migrates the legacy mesh, discovers every manifest, and shells each
// consumer's reconcile action — convergence happens in the consumer, which
// pull-merges its peers from the mesh internally. A per-consumer failure is
// captured in its result, never aborting the others.
func reconcileAll(ctx context.Context, r hostregistry.Runner) ([]reconcileResult, error) {
	if err := hostregistry.MigrateLegacyMesh(ctx, "reposync", "cookiesync"); err != nil {
		return nil, fmt.Errorf("migrate legacy mesh: %w", err)
	}
	manifests, err := discoverManifests()
	if err != nil {
		return nil, err
	}
	results := make([]reconcileResult, 0, len(manifests))
	for _, m := range manifests {
		results = append(results, reconcileOne(ctx, r, m))
	}
	return results, nil
}

func reconcileOne(ctx context.Context, r hostregistry.Runner, m manifest.Manifest) reconcileResult {
	argv, err := manifest.Render(m.Actions.Reconcile, manifest.ActionVars{})
	if err != nil {
		return reconcileResult{Name: m.Name, Err: fmt.Sprintf("render reconcile: %v", err)}
	}
	if _, err := r.Local(ctx, m.Binary, argv...); err != nil {
		return reconcileResult{Name: m.Name, Err: err.Error()}
	}
	return reconcileResult{Name: m.Name}
}
