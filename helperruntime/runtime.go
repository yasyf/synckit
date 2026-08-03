// Package helperruntime composes one resident consumer helper with daemonkit.
package helperruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

const shutdownGrace = 30 * time.Second

// App identifies one consumer helper.
type App struct {
	Name string
}

// Product is the product-owned runtime state published at readiness.
type Product interface {
	Drain(context.Context) error
	Close(context.Context) error
}

// Config supplies the exact helper identity and product preparation.
type Config struct {
	App        App
	Program    daemonkit.Program
	Dispatcher *rpc.Dispatcher
	MaxFrame   daemonkit.Bytes
	Prepare    func(daemonkit.Ctx) (Product, error)
}

// Spec is the one daemonkit identity a consumer helper serves and every caller
// opens: the socket, lock, state dir, and launchd job all derive from the
// helper label, so neither end declares a path. A zero program is the client
// half — Open never reads it.
func Spec(name string, program daemonkit.Program, maxFrame daemonkit.Bytes) (daemonkit.Daemon, error) {
	label, err := serviceidentity.HelperLabel(name)
	if err != nil {
		return daemonkit.Daemon{}, fmt.Errorf("helperruntime: app name: %w", err)
	}
	if maxFrame == 0 {
		maxFrame = rpc.MaxFrame
	}
	return daemonkit.Daemon{
		Label:    daemonkit.Label(label),
		Program:  program,
		Schemas:  []daemonkit.Schema{rpc.WireBuild},
		Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Restart:  daemonkit.RestartAlways,
		Shutdown: daemonkit.Grace(shutdownGrace),
		MaxFrame: maxFrame,
	}, nil
}

// Runtime owns one helper's daemonkit lifetime.
type Runtime struct {
	spec       daemonkit.Daemon
	dispatcher *rpc.Dispatcher
	prepare    func(daemonkit.Ctx) (Product, error)
}

type runtimeProduct struct {
	product    Product
	dispatcher *rpc.Dispatcher
}

// New constructs one exact helper runtime. It performs no I/O or preparation.
func New(config Config) (*Runtime, error) {
	if config.Dispatcher == nil || config.Prepare == nil {
		return nil, errors.New("helperruntime: dispatcher and prepare are required")
	}
	spec, err := Spec(config.App.Name, config.Program, config.MaxFrame)
	if err != nil {
		return nil, err
	}
	return &Runtime{spec: spec, dispatcher: config.Dispatcher, prepare: config.Prepare}, nil
}

// Run serves one helper generation until ctx ends or the daemon drains.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("helperruntime: runtime is required")
	}
	_, err := daemonkit.Serve(ctx, r.spec, func(c daemonkit.Ctx) (daemonkit.Product, error) {
		product, err := r.prepare(c)
		if err != nil {
			return nil, err
		}
		return &runtimeProduct{product: product, dispatcher: r.dispatcher}, nil
	})
	return err
}

func (p *runtimeProduct) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.dispatcher.Handle(ctx, request)
}

func (p *runtimeProduct) Drain(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return p.product.Drain(ctx)
}

func (p *runtimeProduct) Close(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return p.product.Close(ctx)
}
