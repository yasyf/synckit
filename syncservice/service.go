package syncservice

import (
	"context"
	"encoding/json"
)

// ProtocolVersion is the wire version of the typed sync contract. A client compares
// it against a peer's reported [Capabilities] to detect a version skew before
// driving the rest of the protocol.
const ProtocolVersion = 1

// The svc.-namespaced rpc method names that make up the typed sync contract.
const (
	// MethodCapabilities reports the peer's name, protocol version, and methods.
	MethodCapabilities = "svc.capabilities"
	// MethodList enumerates the items this peer tracks for sync.
	MethodList = "svc.list"
	// MethodReconcile converges this peer against an origin host.
	MethodReconcile = "svc.reconcile"
	// MethodSync runs a full sync against an origin host.
	MethodSync = "svc.sync"
	// MethodGetState returns this peer's opaque registry state.
	MethodGetState = "svc.get_state"
)

// WatchItem is one tracked unit of sync: a stable id, the directories whose changes
// trigger a sync, and a fingerprint of its current state.
type WatchItem struct {
	ID          string   `json:"id"`
	WatchDirs   []string `json:"watch_dirs"`
	Fingerprint string   `json:"fingerprint"`
}

// Capabilities is a peer's self-description: its name, the protocol version it
// speaks, and the method names it serves.
type Capabilities struct {
	Name            string   `json:"name"`
	ProtocolVersion int      `json:"protocol_version"`
	Methods         []string `json:"methods"`
}

// ReconcileResult reports the outcome of a reconcile: how many items converged.
type ReconcileResult struct {
	Converged int `json:"converged"`
}

// SyncResult reports the outcome of a sync: how many items converged.
type SyncResult struct {
	Converged int `json:"converged"`
}

// RawRegistry is the consumer's registry state carried as opaque JSON. It is never
// decoded inside the contract so its int64 CRDT stamps round-trip byte-exact.
type RawRegistry = json.RawMessage

// SyncConsumer is the typed sync surface a consumer implements and the daemon serves
// over rpc. [RegisterConsumer] binds each method to the matching rpc handler.
type SyncConsumer interface {
	// Capabilities reports this consumer's name, protocol version, and methods.
	Capabilities(ctx context.Context) (Capabilities, error)
	// List enumerates the items this consumer tracks for sync.
	List(ctx context.Context) ([]WatchItem, error)
	// Reconcile converges this consumer against the named origin host.
	Reconcile(ctx context.Context, origin string) (ReconcileResult, error)
	// Sync runs a full sync of this consumer against the named origin host.
	Sync(ctx context.Context, origin string) (SyncResult, error)
	// GetState returns this consumer's opaque registry state.
	GetState(ctx context.Context) (RawRegistry, error)
}
