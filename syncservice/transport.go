package syncservice

import (
	"context"
	"errors"
	"sync"

	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/clirunner"
	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/rpc"
)

// ErrTransportRunnerClosed means a scoped runner or one of its transports was
// used after the callback returned.
var ErrTransportRunnerClosed = errors.New("syncservice: transport runner scope closed")

// TransportRunner creates persistent local and SSH transports within one shared
// crash-recoverable process scope. Its methods are safe for concurrent use.
type TransportRunner interface {
	Stdio(name string, args ...string) Transport
	SSHStdio(peer, remoteCmd string) Transport
}

// Socket returns a persistent transport to the resident Unix socket.
func Socket(sock string) Transport { return synctransport.Socket(sock) }

// WithTransportRunner owns one process pool across every concurrent transport
// created by callback. All transports settle before this function returns.
func WithTransportRunner(ctx context.Context, callback func(TransportRunner) error) error {
	if callback == nil {
		return errors.New("syncservice: transport runner callback is required")
	}
	directory, err := hostregistry.Mesh.Dir()
	if err != nil {
		return err
	}
	return clirunner.WithPool(ctx, directory, func(pool *supervise.Pool) (err error) {
		runner := &scopedTransportRunner{pool: pool, active: true, transports: make(map[*scopedTransport]struct{})}
		defer func() { err = errors.Join(err, runner.close()) }()
		return callback(runner)
	})
}

type scopedTransportRunner struct {
	mu         sync.Mutex
	pool       *supervise.Pool
	active     bool
	transports map[*scopedTransport]struct{}
}

func (r *scopedTransportRunner) Stdio(name string, args ...string) Transport {
	return r.add(func() synctransport.Transport { return synctransport.NewStdio(r.pool, name, args...) })
}

func (r *scopedTransportRunner) SSHStdio(peer, remoteCmd string) Transport {
	return r.add(func() synctransport.Transport { return synctransport.NewSSHStdio(r.pool, peer, remoteCmd) })
}

func (r *scopedTransportRunner) add(create func() synctransport.Transport) Transport {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active {
		return closedTransport{}
	}
	transport := &scopedTransport{owner: r, transport: create()}
	r.transports[transport] = struct{}{}
	return transport
}

func (r *scopedTransportRunner) close() error {
	r.mu.Lock()
	r.active = false
	transports := make([]*scopedTransport, 0, len(r.transports))
	for transport := range r.transports {
		transports = append(transports, transport)
	}
	r.mu.Unlock()
	r.pool.Close()
	r.pool.Cancel()
	var err error
	for _, transport := range transports {
		err = errors.Join(err, transport.Close())
	}
	return err
}

type scopedTransport struct {
	owner     *scopedTransportRunner
	transport synctransport.Transport
	closeOnce sync.Once
	closeErr  error
}

func (t *scopedTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	t.owner.mu.Lock()
	active := t.owner.active
	t.owner.mu.Unlock()
	if !active {
		return nil, ErrTransportRunnerClosed
	}
	return t.transport.Do(ctx, req)
}

func (t *scopedTransport) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = t.transport.Close()
		t.owner.mu.Lock()
		delete(t.owner.transports, t)
		t.owner.mu.Unlock()
	})
	return t.closeErr
}

type closedTransport struct{}

func (closedTransport) Do(context.Context, *rpc.Request) (*Response, error) {
	return nil, ErrTransportRunnerClosed
}

func (closedTransport) Close() error { return nil }
