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

const (
	acceptedPinPrefix  = "synckit.accepted/"
	acceptingPinPrefix = "synckit.accepting/"
)

// ErrIncompleteAck reports a consumer acknowledging a change's source
// revision while the receiver's own store lacks some root closure.
var ErrIncompleteAck = errors.New("syncservice: consumer acknowledged a change with incomplete artifact roots")

type acceptStore interface {
	Complete(ctx context.Context, roots []artifact.Ref) (int, error)
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
}

// RegisterArtifactConsumer binds svc's v1 and v2 sync methods and store's
// artifact methods on d. apply.v2 computes root readiness from store and
// refuses an acknowledgement while any root closure is incomplete.
func RegisterArtifactConsumer(d *rpc.Dispatcher, svc ArtifactConsumer, store *artifact.Store, monitor netpolicy.Monitor) {
	artifact.Register(d, store, monitor)
	registerArtifactConsumer(d, svc, store)
}

func registerArtifactConsumer(d *rpc.Dispatcher, svc ArtifactConsumer, store acceptStore) {
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
	accepting := acceptingPinPrefix + change.Origin
	if err := store.SetPins(ctx, accepting, change.Artifacts); err != nil {
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
	if !result.Stale && (acked || result.Partial) {
		if err := store.SetPins(ctx, acceptedPinPrefix+change.Origin, change.Artifacts); err != nil {
			return ApplyResult{}, fmt.Errorf("syncservice: pin accepted roots from %s: %w", change.Origin, err)
		}
	}
	if err := store.SetPins(ctx, accepting, nil); err != nil {
		return ApplyResult{}, fmt.Errorf("syncservice: unpin incoming roots from %s: %w", change.Origin, err)
	}
	return result, nil
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
