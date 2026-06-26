// Command rpcstub is a trivial typed-sync consumer used by the syncservice stdio
// transport test: it serves the typed contract over its stdin/stdout via
// rpc.ServeConn until EOF. Diagnostics go to stderr so they never corrupt the
// framing stdout.
package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// stateJSON is a registry literal with a 16-digit int64 added_at stamp. A correct
// round trip preserves these bytes exactly; decoding through any would render the
// stamp as the float64 "1.7192736e+15".
const stateJSON = `{"x":{"added_at":1719273600000000,"removed_at":0,"value":{"k":"v"}}}`

// stub is the in-file SyncConsumer the stub program serves.
type stub struct{}

func (stub) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("stub"), nil
}

func (stub) List(context.Context) ([]syncservice.WatchItem, error) {
	return []syncservice.WatchItem{
		{ID: "only", WatchDirs: []string{"/d"}, Fingerprint: "fp"},
	}, nil
}

func (stub) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{Converged: 1}, nil
}

func (stub) Sync(context.Context, string) (syncservice.SyncResult, error) {
	return syncservice.SyncResult{Converged: 1}, nil
}

func (stub) GetState(context.Context) (syncservice.RawRegistry, error) {
	return syncservice.RawRegistry(stateJSON), nil
}

// stdio wraps the process's stdin (read) and stdout (write) as one bidirectional
// pipe for rpc.ServeConn.
type stdio struct {
	in  io.Reader
	out io.Writer
}

func (s stdio) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s stdio) Write(p []byte) (int, error) { return s.out.Write(p) }

func main() {
	log.SetOutput(os.Stderr)
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, stub{})
	if err := rpc.ServeConn(context.Background(), stdio{in: os.Stdin, out: os.Stdout}, d); err != nil {
		log.Fatalf("rpcstub: serve: %v", err)
	}
}
