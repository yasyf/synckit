// Package rpctest owns exact daemonkit-backed RPC servers for cross-package tests.
package rpctest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/rpc"
)

const (
	readyBudget = 20 * time.Second
	readyProbe  = 50 * time.Millisecond
	closeBudget = 15 * time.Second

	// readyMethod is a probe no dispatcher registers: the daemon answering it at
	// all — with an unknown-method Response — is the readiness proof.
	readyMethod = "synckit.rpctest.ready"
)

// Spec is the daemon identity Start serves and every client Opens. Callers
// isolate its socket, lock, and state dir by pointing DAEMONKIT_HOME at a short
// temporary directory — a deep t.TempDir() path overflows sockaddr_un.
func Spec(label string) daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:    daemonkit.Label(label),
		Schemas:  []daemonkit.Schema{rpc.WireBuild},
		Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Shutdown: daemonkit.Grace(closeBudget),
		MaxFrame: rpc.MaxFrame,
	}
}

// Server is one ready in-process daemon serving a synckit dispatcher.
type Server struct {
	spec   daemonkit.Daemon
	stop   context.CancelFunc
	served chan error
	once   sync.Once
	err    error
}

// Start serves label's daemon on its own goroutine and returns once it has
// published readiness.
func Start(ctx context.Context, label string, dispatcher *rpc.Dispatcher) (*Server, error) {
	if dispatcher == nil {
		return nil, errors.New("rpctest: dispatcher is required")
	}
	spec := Spec(label)
	serveCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, spec, func(daemonkit.Ctx) (daemonkit.Product, error) {
			return product{dispatcher: dispatcher}, nil
		})
		served <- err
	}()
	server := &Server{spec: spec, stop: stop, served: served}
	client, err := server.Client()
	if err != nil {
		return nil, errors.Join(err, server.Close())
	}
	defer func() { _ = client.Close() }()
	if err := WaitReady(ctx, client); err != nil {
		return nil, errors.Join(err, server.Close())
	}
	return server, nil
}

// WaitReady blocks until the daemon admits a business request. It is not
// Client.WaitReady: that pins the serving process through the control lane and
// refuses a peer PID equal to its own, which an in-process Serve always is.
func WaitReady(ctx context.Context, client *rpc.Client) error {
	deadline := time.Now().Add(readyBudget)
	for {
		probeCtx, cancel := context.WithTimeout(ctx, readyProbe)
		_, err := client.Call(probeCtx, &rpc.Request{Method: readyMethod})
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rpctest: daemon never became ready: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readyProbe):
		}
	}
}

// Client returns a typed synckit client over the served daemon's business lane.
func (s *Server) Client() (*rpc.Client, error) {
	client, err := daemonkit.Open(s.spec)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	}), nil
}

// Close drains the daemon and waits for Serve to return.
func (s *Server) Close() error {
	s.once.Do(func() {
		s.stop()
		select {
		case s.err = <-s.served:
		case <-time.After(closeBudget):
			s.err = errors.New("rpctest: serve did not settle")
		}
	})
	return s.err
}

type product struct{ dispatcher *rpc.Dispatcher }

func (p product) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.dispatcher.Handle(ctx, request)
}

func (product) Drain(daemonkit.Budget) error { return nil }

func (product) Close(daemonkit.Budget) error { return nil }
