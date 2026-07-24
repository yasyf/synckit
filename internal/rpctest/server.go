// Package rpctest owns exact daemonkit-backed RPC servers for cross-package tests.
package rpctest

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"

	"github.com/yasyf/synckit/internal/runtimeowner"
	"github.com/yasyf/synckit/rpc"
)

// Server is one ready exact-wire test runtime.
type Server struct {
	runtime *dkdaemon.Runtime
	once    sync.Once
	err     error
}

// Start binds socket and publishes dispatcher through daemonkit Runtime.
func Start(ctx context.Context, socket, stateDir string, dispatcher *rpc.Dispatcher) (*Server, error) {
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return nil, err
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 4, QueueCapacity: 4, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 4096, MaxStdoutBytes: 4096, MaxStderrBytes: 4096,
	}, &proc.Reaper{Store: &proc.FileStore{Path: filepath.Join(stateDir, "workers.db")}, Generation: generation})
	if err != nil {
		return nil, err
	}
	children, err := proc.NewManager(4, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(stateDir, "children.db")}, Generation: generation,
	})
	if err != nil {
		return nil, err
	}
	policy, err := runtimeowner.TrustPolicy()
	if err != nil {
		return nil, err
	}
	var slot *dkdaemon.PublicationSlot[*rpc.Dispatcher]
	rpcServer := rpc.NewServer(func(publication dkdaemon.Publication) (*rpc.Dispatcher, error) {
		return slot.Value(publication)
	})
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: "synckit-rpctest", RuntimeProtocol: int(rpc.Version),
		Wire: rpcServer.Wire, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(stateDir, "stop.db")},
		Workers:          workers, Children: children,
	})
	if err != nil {
		return nil, err
	}
	slot = dkdaemon.NewPublicationSlot[*rpc.Dispatcher](runtime)
	activation, err := runtime.Begin(ctx)
	if err != nil {
		return nil, err
	}
	publication, err := slot.Stage(activation, dispatcher)
	if err == nil {
		err = activation.CommitReady(publication)
	}
	if err != nil {
		_ = activation.Fail(err)
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return nil, errors.Join(err, runtime.Close(closeCtx))
	}
	return &Server{runtime: runtime}, nil
}

// Close settles the runtime and all process owners.
func (s *Server) Close() error {
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.err = s.runtime.Close(ctx)
	})
	return s.err
}
