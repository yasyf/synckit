package syncservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/rpc"
)

// RegisterConsumer binds every svc.-namespaced method on d to the matching method of
// svc, adapting each rpc handler's untyped params to the typed call and returning the
// result struct. Apply, Export, and Reconcile may mutate service state, so they are
// registered exclusive — they queue behind the dispatcher's exclusive mutex instead
// of contending on a product's non-reentrant state lock.
func RegisterConsumer(d *rpc.Dispatcher, svc SyncConsumer) {
	d.Register(MethodCapabilities, func(ctx context.Context, _ map[string]any) (any, error) {
		return svc.Capabilities(ctx)
	})
	d.Register(MethodList, func(ctx context.Context, _ map[string]any) (any, error) {
		return svc.List(ctx)
	})
	d.RegisterExclusive(MethodReconcile, func(ctx context.Context, p map[string]any) (any, error) {
		return svc.Reconcile(ctx, optString(p, "origin"))
	})
	d.RegisterExclusive(MethodExport, func(ctx context.Context, p map[string]any) (any, error) {
		var request ExportRequest
		if err := decodeParams(p, &request); err != nil {
			return nil, err
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		change, err := svc.Export(ctx, request)
		if err == nil {
			err = change.Validate(false)
		}
		return change, err
	})
	d.RegisterExclusive(MethodApply, func(ctx context.Context, p map[string]any) (any, error) {
		var change ChangeEnvelope
		if err := decodeParams(p, &change); err != nil {
			return nil, err
		}
		if err := change.Validate(true); err != nil {
			return nil, err
		}
		return svc.Apply(ctx, change)
	})
}

// optString returns the string at key in p, or "" when p is nil, the key is absent,
// or the value is not a string.
func optString(p map[string]any, key string) string {
	s, _ := p[key].(string)
	return s
}

func decodeParams(params map[string]any, target any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("syncservice: encode params: %w", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("syncservice: decode params: %w", err)
	}
	return nil
}
