package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const stateJSON = `{"x":{"added_at":1719273600000000,"removed_at":0,"value":{"k":"v"}}}`

type stub struct{}

func (stub) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("stub"), nil
}

func (stub) List(context.Context) ([]syncservice.WatchItem, error) {
	return []syncservice.WatchItem{{ID: "only", Fingerprint: "fp"}}, nil
}

func (stub) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{Converged: 1}, nil
}

func (stub) Sync(context.Context, string) (syncservice.SyncResult, error) {
	return syncservice.SyncResult{Converged: 1}, nil
}

func (stub) GetState(ctx context.Context) (syncservice.RawRegistry, error) {
	if raw := os.Getenv("SYNCKIT_TEST_GET_STATE_DELAY"); raw != "" {
		delay, err := time.ParseDuration(raw)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return syncservice.RawRegistry(stateJSON), nil
}

func main() {
	dir, err := os.MkdirTemp("", "rpcstub")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "rpc.sock")
	listener, err := rpc.Listen(context.Background(), sock)
	if err != nil {
		panic(err)
	}
	dispatcher := rpc.NewDispatcher()
	syncservice.RegisterConsumer(dispatcher, stub{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rpc.NewServer(dispatcher).Serve(ctx, listener) }()
	err = rpc.Proxy(ctx, os.Stdin, os.Stdout, sock)
	cancel()
	if serveErr := <-done; err == nil {
		err = serveErr
	}
	if err != nil {
		panic(err)
	}
}
