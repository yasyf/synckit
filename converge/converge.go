// Package converge is the generic convergent-reconcile orchestration over a
// [cregistry] registry: each host independently pulls every peer's registry,
// folds them into its own with the CRDT [cregistry.Merge], persists the merged
// result, then reconciles each present item through a domain [Driver].
//
// # The loop guard
//
// [Reconcile] is PULL-ONLY, and that is the property that keeps a mesh of hosts
// from a write storm. A pass reads every peer's registry through a [Fetcher]
// (a read-only fetch — it never mutates the peer), merges them locally, and the
// ONLY write it performs is the LOCAL [Driver.SaveRegistry]. It never asks a peer
// to reconcile and never pushes a registry to a peer. Convergence is therefore
// emergent: each host catches up by pulling on its own schedule, so a reconcile
// on host A can never cause host B to do work, and an offline peer simply folds
// in the missed mutations on its next pass. A single unreachable peer is logged
// and skipped, never fatal — the pass still converges against every peer that did
// answer, and the skipped one self-heals next time.
//
// # The Driver seam
//
// The [Driver] owns everything domain-specific: how the convergent registry is
// read from and written back into the tool's own state file (LoadRegistry /
// SaveRegistry, FK-preserving the rest of that file), and the per-item reconcile
// body (Reconcile). synckit owns only the generic shape — lock, pull-merge,
// persist, fan-out — so the same orchestration drives reposync's per-pair git
// fast-forward and cookiesync's group-wide cookie value-union: a Driver decides
// its own fan-out inside Reconcile.
package converge

import (
	"context"
	"log/slog"

	"github.com/yasyf/synckit/cregistry"
)

// Outcome is the result classification of reconciling one item: what the domain
// [Driver.Reconcile] did, reported back through [ItemResult] so a caller can
// summarize a pass without parsing strings. It is a plain string so a Driver can
// name its own domain outcomes.
type Outcome string

// ItemResult reports what one item's reconcile did: the item's registry id, the
// [Outcome] the [Driver] returned, and any error. A non-nil Err means that one
// item failed; it never aborts the rest of the pass.
type ItemResult struct {
	ID      string
	Outcome Outcome
	Err     error
}

// Driver is the domain seam reconcile is parameterized over: it reads and writes
// the convergent registry inside the tool's own state file and owns the per-item
// reconcile body. V is the per-item payload carried by the registry. Implementations
// are reposync's RepoDriver (git fast-forward) and, in a later phase, cookiesync's
// cookie driver (value-union) — synckit calls these in a fixed order and never
// reaches past them into the domain.
type Driver[V any] interface {
	// LoadRegistry reads the convergent registry — including tombstones — out of the
	// tool's state file.
	LoadRegistry(ctx context.Context) (cregistry.Registry[V], error)
	// SaveRegistry atomically persists the merged registry back into the tool's state
	// file, preserving every other key in that file. It is idempotent: persisting a
	// registry equal to what is on disk is a no-op write.
	SaveRegistry(ctx context.Context, reg cregistry.Registry[V]) error
	// Reconcile applies the domain reconcile for one present item: id is its registry
	// key, entry its current record, peers the full peer list (a Driver decides its
	// own fan-out), and origin the anti-echo provenance — the peer that triggered this
	// pass, to be skipped, or "" for a local trigger.
	Reconcile(ctx context.Context, id string, entry cregistry.Entry[V], peers []string, origin string) (Outcome, error)
}

// Fetcher reads a peer's convergent registry READ-ONLY for the pull-merge step. A
// fetch never mutates the peer; the implementation is typically an ssh call to the
// peer's state-read command. An error is reported per-peer and skips that peer, so a
// single unreachable host never aborts a pass.
type Fetcher[V any] interface {
	// Fetch returns peer's current convergent registry without modifying it.
	Fetch(ctx context.Context, peer string) (cregistry.Registry[V], error)
}

// Reconcile runs one convergent-reconcile pass under lock: load the local registry,
// pull-merge every peer except origin into it, persist the converged result, then
// reconcile each present item through d. It is PULL-ONLY — the sole write is the
// local SaveRegistry; see the package doc for the loop guard. An unreachable peer is
// logged and skipped, never fatal. lock wraps the whole pass so a concurrent pass on
// the same host is serialized. peers is the full mesh; origin is the anti-echo
// provenance ("" for a local trigger). It returns one [ItemResult] per present item;
// a single item's failure is carried in its result, not returned as the pass error.
func Reconcile[V any](
	ctx context.Context,
	lock func(ctx context.Context, fn func() error) error,
	d Driver[V],
	f Fetcher[V],
	peers []string,
	origin string,
) ([]ItemResult, error) {
	var results []ItemResult
	err := lock(ctx, func() error {
		local, err := d.LoadRegistry(ctx)
		if err != nil {
			return err
		}

		merged := local
		for _, peer := range peers {
			if peer == origin {
				continue
			}
			peerReg, err := f.Fetch(ctx, peer)
			if err != nil {
				slog.WarnContext(ctx, "converge: skip unreachable peer", "peer", peer, "err", err)
				continue
			}
			merged = cregistry.Merge(merged, peerReg)
		}

		if err := d.SaveRegistry(ctx, merged); err != nil {
			return err
		}

		results = make([]ItemResult, 0, len(merged))
		for id, entry := range merged.Present() {
			outcome, err := d.Reconcile(ctx, id, entry, peers, origin)
			results = append(results, ItemResult{ID: id, Outcome: outcome, Err: err})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
