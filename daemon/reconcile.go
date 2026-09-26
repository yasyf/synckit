package daemon

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func newReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Run one convergent reconcile pass for every registered consumer.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			results, err := reconcileResident(cmd.Context())
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

func reconcileResident(ctx context.Context) ([]reconcileResult, error) {
	client, err := daemonClient()
	if err != nil {
		return nil, fmt.Errorf("dial synckitd: %w", err)
	}
	defer func() { _ = client.Close() }()
	resp, err := client.Call(ctx, &rpc.Request{Method: "reconcile"})
	if err != nil {
		return nil, fmt.Errorf("reconcile: %w", err)
	}
	if !resp.OK {
		return nil, fmt.Errorf("reconcile: %s", resp.Error)
	}
	var results []reconcileResult
	if err := json.Unmarshal(resp.Result, &results); err != nil {
		return nil, fmt.Errorf("decode reconcile result: %w", err)
	}
	return results, nil
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
