package syncservice

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

const (
	acceptedPinPrefix = "synckit.accepted/"
	ackedPinPrefix    = "synckit.acked/"
)

// ErrIncompleteAck reports a consumer acknowledging a change's source
// revision while the receiver's own store lacks some root closure.
var ErrIncompleteAck = errors.New("syncservice: consumer acknowledged a change with incomplete artifact roots")

type acceptStore interface {
	Complete(ctx context.Context, roots []artifact.Ref) (int, error)
	SetPins(ctx context.Context, owner string, roots []artifact.Ref) error
	Pins(ctx context.Context) ([]artifact.PinSet, error)
}

// RegisterArtifactConsumer binds svc's v1 and v2 sync methods and store's
// artifact methods on d. apply.v2 returns the typed artifact.Refusal, before
// decoding the change, while monitor's live State is not unrestricted or has
// moved past the epoch the sender admitted the transfer under; it computes
// root readiness from store and refuses an acknowledgement while any root
// closure is incomplete. Each change's roots stay pinned under an owner of
// their own from before svc sees them until svc refuses the change, fails an
// attempt that pinned it, or acknowledges any change from the same origin, so
// a failure after svc records a change never unpins roots svc holds.
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
		_, refusal, err := artifact.Refusal(monitor, p)
		if err != nil {
			return nil, err
		}
		if refusal != nil {
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
	owner := acceptedPinPrefix + change.Origin + "/" + change.ChangeID
	accepted, err := acceptedOwners(ctx, store, change.Origin)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := store.SetPins(ctx, owner, change.Artifacts); err != nil {
		return ApplyResult{}, fmt.Errorf("syncservice: pin incoming roots from %s: %w", change.Origin, err)
	}
	ready, err := readyRoots(ctx, store, change.Artifacts)
	var result ApplyResult
	if err == nil {
		result, err = svc.ApplyArtifacts(ctx, change, ready)
	}
	if err != nil {
		if slices.Contains(accepted, owner) {
			return ApplyResult{}, err
		}
		return ApplyResult{}, errors.Join(err, release(ctx, store, change.Origin, owner))
	}
	fullAck := result.AckedRevision == change.SourceRevision
	if fullAck && len(ready) < len(change.Artifacts) {
		return ApplyResult{}, fmt.Errorf("%w: %d of %d roots ready", ErrIncompleteAck, len(ready), len(change.Artifacts))
	}
	holds := result.Stale && fullAck && result.HeldDigest == change.PayloadDigest
	if (result.Stale && !holds) || result.NeedSnapshot || (!fullAck && !result.Partial) {
		return result, release(ctx, store, change.Origin, owner)
	}
	if err := store.SetPins(ctx, ackedPinPrefix+change.Origin, change.Artifacts); err != nil {
		return ApplyResult{}, fmt.Errorf("syncservice: record acked roots from %s: %w", change.Origin, err)
	}
	if !slices.Contains(accepted, owner) {
		accepted = append(accepted, owner)
	}
	return result, release(ctx, store, change.Origin, accepted...)
}

func acceptedOwners(ctx context.Context, store acceptStore, origin string) ([]string, error) {
	pins, err := store.Pins(ctx)
	if err != nil {
		return nil, fmt.Errorf("syncservice: read pins: %w", err)
	}
	prefix := acceptedPinPrefix + origin + "/"
	var owners []string
	for _, pin := range pins {
		if changeID, ok := strings.CutPrefix(pin.Owner, prefix); ok && !strings.Contains(changeID, "/") {
			owners = append(owners, pin.Owner)
		}
	}
	return owners, nil
}

func release(ctx context.Context, store acceptStore, origin string, owners ...string) error {
	for _, owner := range owners {
		if err := store.SetPins(ctx, owner, nil); err != nil {
			return fmt.Errorf("syncservice: release accepted roots from %s: %w", origin, err)
		}
	}
	return nil
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
