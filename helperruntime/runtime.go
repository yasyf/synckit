// Package helperruntime composes one resident consumer helper with daemonkit.
package helperruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"

	"github.com/yasyf/synckit/internal/runtimehealth"
	"github.com/yasyf/synckit/internal/runtimeowner"
	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

const shutdownTimeout = 30 * time.Second

// App identifies one consumer helper generation.
type App struct {
	Name         string
	RuntimeBuild string
}

// Product is the product-owned runtime state published at readiness.
type Product interface {
	Drain(context.Context) error
	Close(context.Context) error
}

// Config supplies the exact daemonkit process owners and product preparation.
type Config struct {
	App       App
	Socket    string
	Server    *rpc.Server
	Workers   *worker.Pool
	Children  *proc.Manager
	StopStore *proc.FileStore
	Prepare   func(dkdaemon.Activation) (Product, error)
}

// Runtime owns daemonkit readiness and the product publication lifetime.
type Runtime struct {
	daemon  *dkdaemon.Runtime
	slot    *dkdaemon.PublicationSlot[Product]
	prepare func(dkdaemon.Activation) (Product, error)
}

// New constructs one exact helper runtime. It performs no I/O or preparation.
func New(config Config) (*Runtime, error) {
	if _, err := serviceidentity.HelperLabel(config.App.Name); err != nil {
		return nil, fmt.Errorf("helperruntime: app name: %w", err)
	}
	if config.App.RuntimeBuild == "" {
		return nil, errors.New("helperruntime: runtime build is required")
	}
	if config.Socket == "" || config.Server == nil || config.Workers == nil || config.Children == nil || config.StopStore == nil || config.Prepare == nil {
		return nil, errors.New("helperruntime: socket, server, workers, children, stop store, and prepare are required")
	}
	policy, err := runtimeowner.TrustPolicy()
	if err != nil {
		return nil, err
	}
	var daemonRuntime *dkdaemon.Runtime
	daemonRuntime, err = wire.NewRuntime(wire.RuntimeConfig{
		Socket: config.Socket, RuntimeBuild: config.App.RuntimeBuild, RuntimeProtocol: int(rpc.Version),
		Wire: config.Server.Wire, TrustPolicy: policy, StopControlStore: config.StopStore,
		Observations: []wire.ObservationRoute{runtimehealth.Observation(func(ctx context.Context) (dkdaemon.Health, error) {
			return daemonRuntime.Health(ctx)
		})},
		Workers: config.Workers, Children: config.Children, ShutdownTimeout: shutdownTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Runtime{
		daemon: daemonRuntime, slot: dkdaemon.NewPublicationSlot[Product](daemonRuntime), prepare: config.Prepare,
	}, nil
}

// Run prepares, publishes, serves, drains, and settles one helper generation.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil || r.daemon == nil {
		return errors.New("helperruntime: runtime is required")
	}
	activation, err := r.daemon.Begin(ctx)
	if err != nil {
		return err
	}
	product, err := r.prepare(activation)
	if err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, r.closeDaemon(ctx))
	}
	publication, err := r.slot.Stage(activation, product)
	if err == nil {
		err = activation.CommitReady(publication)
	}
	if err != nil {
		_ = activation.Fail(err)
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		return errors.Join(err, product.Close(closeCtx), r.daemon.Close(closeCtx))
	}

	done := make(chan error, 1)
	go func(waitCtx, closeBase context.Context) {
		select {
		case <-waitCtx.Done():
		case <-activation.Context().Done():
		}
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(closeBase), shutdownTimeout)
		defer cancel()
		done <- errors.Join(product.Drain(closeCtx), r.daemon.Close(closeCtx), product.Close(closeCtx))
	}(ctx, ctx)
	waitErr := r.daemon.Wait(context.WithoutCancel(ctx))
	closeErr := <-done
	if ctx.Err() != nil && (waitErr == nil || errors.Is(waitErr, ctx.Err())) {
		waitErr = nil
	}
	return errors.Join(waitErr, closeErr)
}

func (r *Runtime) closeDaemon(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), shutdownTimeout)
	defer cancel()
	return r.daemon.Close(ctx)
}
