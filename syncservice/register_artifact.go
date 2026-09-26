package syncservice

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

const acceptedPinPrefix = "synckit.accepted/"

// ErrIncompleteAck reports a consumer acknowledging a change's source
// revision while the receiver's own store lacks some root closure.
var ErrIncompleteAck = errors.New("syncservice: consumer acknowledged a change with incomplete artifact roots")

type acceptStore interface {
	Complete(ctx context.Context, roots []artifact.Ref) (int, error)
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
	Pins(ctx context.Context) ([]artifact.PinSet, error)
}

// RegisterArtifactConsumer binds svc's v1 and v2 sync methods and store's
// artifact methods on d. apply.v2 returns the typed refusal, before decoding
// the change, while monitor's live State is not unrestricted; it computes
// root readiness from store and refuses an acknowledgement while any root
// closure is incomplete.
func RegisterArtifactConsumer(d *rpc.Dispatcher, svc ArtifactConsumer, store *artifact.Store, monitor netpolicy.Monitor) {
	artifact.Register(d, store, monitor)
	registerArtifactConsumer(d, svc, store, monitor)
}

func registerArtifactConsumer(d *rpc.Dispatcher, svc ArtifactConsumer, store acceptStore, monitor netpolicy.Monitor) {
	RegisterConsumer(d, svc)
	d.Register(MethodCapabilities, func(ctx context.Context, _ map[string]any) (any, error) {
		caps, err := svc.Capabilities(ctx)
		if err != nil {
			return nil, err
		}
		return withArtifactMethods(caps), nil
	})
	d.RegisterExclusive(MethodExportV2, func(ctx context.Context, p map[string]any) (any, error) {
		var request ExportRequest
		if err := decodeParams(p, &request); err != nil {
			return nil, err
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		change, err := svc.ExportArtifacts(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := change.Validate(false); err != nil {
			return nil, err
		}
		return change, nil
	})
	d.RegisterExclusive(MethodApplyV2, func(ctx context.Context, p map[string]any) (any, error) {
		if refusal := artifact.LiveRefusal(monitor); refusal != nil {
			return ApplyResult{AckedRevision: NewRevision(0), Paused: refusal}, nil
		}
		var change ChangeEnvelope
		if err := decodeParams(p, &change); err != nil {
			return nil, err
		}
		if err := change.Validate(true); err != nil {
			return nil, err
		}
		return applyArtifacts(ctx, svc, store, change)
	})
}

func applyArtifacts(ctx context.Context, svc ArtifactConsumer, store acceptStore, change ChangeEnvelope) (ApplyResult, error) {
	owner := acceptedPinPrefix + change.Origin
	prior, err := pinnedRoots(ctx, store, owner)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := store.SetPins(ctx, owner, unionRefs(prior, change.Artifacts)); err != nil {
		return ApplyResult{}, fmt.Errorf("syncservice: pin incoming roots from %s: %w", change.Origin, err)
	}
	ready, err := readyRoots(ctx, store, change.Artifacts)
	if err != nil {
		return ApplyResult{}, err
	}
	result, err := svc.ApplyArtifacts(ctx, change, ready)
	if err != nil {
		return ApplyResult{}, err
	}
	acked := result.AckedRevision == change.SourceRevision
	if acked && len(ready) < len(change.Artifacts) {
		return ApplyResult{}, fmt.Errorf("%w: %d of %d roots ready", ErrIncompleteAck, len(ready), len(change.Artifacts))
	}
	resolved := prior
	if !result.Stale && (acked || result.Partial) {
		resolved = change.Artifacts
	}
	if err := store.SetPins(ctx, owner, resolved); err != nil {
		return ApplyResult{}, fmt.Errorf("syncservice: narrow accepted roots from %s: %w", change.Origin, err)
	}
	return result, nil
}

func pinnedRoots(ctx context.Context, store acceptStore, owner string) ([]artifact.Ref, error) {
	pins, err := store.Pins(ctx)
	if err != nil {
		return nil, fmt.Errorf("syncservice: read pins: %w", err)
	}
	for _, pin := range pins {
		if pin.Owner == owner {
			return pin.Roots, nil
		}
	}
	return nil, nil
}

func unionRefs(prior, next []artifact.Ref) []artifact.Ref {
	union := slices.Clone(prior)
	for _, ref := range next {
		if !slices.ContainsFunc(union, func(r artifact.Ref) bool { return r.Digest == ref.Digest }) {
			union = append(union, ref)
		}
	}
	return union
}

func readyRoots(ctx context.Context, store acceptStore, roots []artifact.Ref) ([]artifact.Ref, error) {
	ready := make([]artifact.Ref, 0, len(roots))
	for _, root := range roots {
		missing, err := store.Complete(ctx, []artifact.Ref{root})
		if err != nil {
			return nil, fmt.Errorf("syncservice: check root %s: %w", root.Digest, err)
		}
		if missing == 0 {
			ready = append(ready, root)
		}
	}
	return ready, nil
}

func withArtifactMethods(caps Capabilities) Capabilities {
	methods := slices.Clone(caps.Methods)
	for _, method := range ArtifactCapabilities(caps.Name).Methods {
		if !slices.Contains(methods, method) {
			methods = append(methods, method)
		}
	}
	return Capabilities{Name: caps.Name, Methods: methods}
}
