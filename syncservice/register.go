package syncservice

import (
	"context"

	"github.com/yasyf/synckit/rpc"
)

// RegisterConsumer binds every svc.-namespaced method on d to the matching method of
// svc, adapting each rpc handler's untyped params to the typed call and returning the
// result struct. Sync and Reconcile run the flock-wrapped converge pass, so they are
// registered exclusive — they queue behind the dispatcher's exclusive mutex instead
// of contending on the non-reentrant flock; the read-only methods stay concurrent.
// GetState returns the [RawRegistry] directly as the handler result so
// rpc.EncodeResponse marshals it as raw bytes, preserving the registry's int64 CRDT
// stamps; the other methods return their result structs for the dispatcher to encode.
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
	d.RegisterExclusive(MethodSync, func(ctx context.Context, p map[string]any) (any, error) {
		return svc.Sync(ctx, optString(p, "origin"))
	})
	d.Register(MethodGetState, func(ctx context.Context, _ map[string]any) (any, error) {
		return svc.GetState(ctx)
	})
}

// optString returns the string at key in p, or "" when p is nil, the key is absent,
// or the value is not a string.
func optString(p map[string]any, key string) string {
	s, _ := p[key].(string)
	return s
}
