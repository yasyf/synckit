// Package syncservice is the typed sync contract layered over the generic rpc
// transport. A consumer exposes capability, listing, local reconciliation, and
// exact v1 export/apply operations by registering a [SyncConsumer] on an
// [rpc.Dispatcher] via [RegisterConsumer].
//
// A [Client] is the typed caller. It speaks the same wire over a [Transport].
// [Socket] reaches a resident Unix socket. Synckit's daemon alone constructs
// spawned and remote transports under its daemonkit process manager. The client
// decodes each method's result into its Go type, so callers never touch the raw
// envelope.
//
// Exported payloads stay opaque, bounded, and digest-bound. Revisions use canonical
// decimal strings, and payload bytes use base64 in the JSON envelope, so product
// integers never round-trip through float64.
package syncservice
