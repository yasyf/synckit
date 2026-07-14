// Command oncestub is a typed-sync consumer that answers exactly one request and then
// exits: its reader reports EOF after the first request line, so rpc.ServeConn handles a
// single Do and returns. The syncservice failover test uses it as a candidate that dies
// permanently after one successful Do, proving the next Do is free to fail over.
package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// stub reports the name "once" so a test can tell its response apart from rpcstub's.
type stub struct{}

func (stub) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("once"), nil
}

func (stub) List(context.Context) ([]syncservice.WatchItem, error) { return nil, nil }

func (stub) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{}, nil
}

func (stub) Sync(context.Context, string) (syncservice.SyncResult, error) {
	return syncservice.SyncResult{}, nil
}

func (stub) GetState(context.Context) (syncservice.RawRegistry, error) {
	return syncservice.RawRegistry("{}"), nil
}

// oneLine delivers r's bytes up to and including the first newline, then reports EOF, so
// rpc.ServeConn serves one request and shuts down.
type oneLine struct {
	r    io.Reader
	done bool
}

func (o *oneLine) Read(p []byte) (int, error) {
	if o.done {
		return 0, io.EOF
	}
	n, err := o.r.Read(p[:1])
	if n > 0 && p[0] == '\n' {
		o.done = true
	}
	return n, err
}

// stdio wraps the process's stdin (read) and stdout (write) as one bidirectional pipe.
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
	if err := rpc.ServeConn(context.Background(), stdio{in: &oneLine{r: os.Stdin}, out: os.Stdout}, d); err != nil {
		log.Fatalf("oncestub: serve: %v", err)
	}
}
