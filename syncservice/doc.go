// Package syncservice is the typed sync contract layered over the generic rpc
// transport. A consumer exposes its sync operations as svc.-namespaced rpc methods
// (capabilities, list, reconcile, sync, get_state) by registering a [SyncConsumer]
// on an [rpc.Dispatcher] via [RegisterConsumer]; the daemon serves them.
//
// A [Client] is the typed caller. It speaks the same wire over a [Transport].
// [Socket] reaches a resident Unix socket; [WithTransportRunner] owns one scoped,
// crash-recoverable process pool whose [TransportRunner.Stdio] and
// [TransportRunner.SSHStdio] methods reach spawned local and remote bridges. The
// client decodes each method's result into its Go type, so callers never touch the
// raw envelope.
//
// The registry payload is the one value the contract keeps opaque. get_state returns
// it as a [RawRegistry] (a json.RawMessage) and the client surfaces it as the same
// raw bytes, never decoding through map[string]any or any: a CRDT registry carries
// int64 microsecond stamps (cregistry.Micros) that JSON's any-decoding would corrupt
// to float64, so the bytes must round-trip exactly from the holder of the state to
// the caller.
package syncservice
