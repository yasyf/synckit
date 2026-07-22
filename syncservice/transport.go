package syncservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

const closeGrace = 2 * time.Second

var (
	transportBackoffBase = 500 * time.Millisecond
	transportBackoffMax  = 30 * time.Second
	errTransportBackoff  = errors.New("syncservice: transport backing off")
)

// Socket returns a persistent transport to the resident Unix socket.
func Socket(sock string) Transport {
	return &socketTransport{client: rpc.NewClient(rpc.ClientConfig{
		Dial:  wire.UnixDialer(sock),
		Build: rpc.Build,
	})}
}

type socketTransport struct {
	client *rpc.Client
}

func (t *socketTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	return call(ctx, t.client, req)
}

func (t *socketTransport) Close() error { return t.client.Close() }

// Stdio returns a persistent v1 transport over a spawned bridge's stdin/stdout.
func Stdio(pool *supervise.Pool, name string, args ...string) Transport {
	return &cmdTransport{pool: pool, candidates: [][]string{append([]string{name}, args...)}}
}

// SSHStdio returns a persistent v1 transport through a peer's raw rpc bridge.
func SSHStdio(pool *supervise.Pool, peer, remoteCmd string) Transport {
	return &cmdTransport{pool: pool, resolve: func() ([][]string, error) {
		addrs, err := hostregistry.DialAddrs(peer)
		if err != nil {
			return nil, fmt.Errorf("dial addresses for %s: %w", peer, err)
		}
		candidates := make([][]string, len(addrs))
		for i, addr := range addrs {
			candidates[i] = hostregistry.SSHArgv(addr, remoteCmd)
		}
		return candidates, nil
	}}
}

// cmdTransport owns one persistent daemonkit session and its bridge process.
// A failed operation is returned exactly once; only a later Do may reconnect.
type cmdTransport struct {
	pool       *supervise.Pool
	resolve    func() ([][]string, error)
	candidates [][]string

	mu      sync.Mutex
	client  *rpc.Client
	session *supervise.SessionProcess
	closed  bool

	idx        int
	resetCount int
	resetAt    time.Time
}

func (t *cmdTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("syncservice: transport closed")
	}
	if t.client == nil {
		t.client = rpc.NewClient(rpc.ClientConfig{Dial: t.dialLocked, Build: rpc.Build})
	}
	resp, err := call(ctx, t.client, req)
	if err == nil {
		t.resetCount = 0
		return resp, nil
	}
	if errors.Is(err, errTransportBackoff) {
		return nil, err
	}
	if ctxErr := terminalContextError(ctx); ctxErr != nil {
		t.reset(ctx)
		t.armBackoff()
		return nil, ctxErr
	}
	var transportErr *rpc.TransportError
	if errors.As(err, &transportErr) && transportErr.Outcome == wire.PreSendFailure && t.idx+1 < len(t.candidates) {
		t.idx++
	}
	t.reset(ctx)
	t.armBackoff()
	return nil, err
}

func terminalContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func call(ctx context.Context, client *rpc.Client, req *rpc.Request) (*Response, error) {
	resp, err := client.Call(ctx, req)
	if err != nil {
		return nil, err
	}
	return &Response{OK: resp.OK, Result: resp.Result, Error: resp.Error}, nil
}

func (t *cmdTransport) dialLocked(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.closed {
		return nil, errors.New("syncservice: transport closed")
	}
	if wait := t.backoffRemaining(); wait > 0 {
		return nil, fmt.Errorf("%w %s after %d consecutive failures", errTransportBackoff, wait.Round(time.Millisecond), t.resetCount)
	}
	if err := t.ensureCandidates(); err != nil {
		return nil, err
	}
	if t.session != nil {
		t.reset(ctx)
	}
	return t.spawn(ctx)
}

func (t *cmdTransport) spawn(ctx context.Context) (net.Conn, error) {
	argv := t.candidates[t.idx]
	session, err := t.pool.StartSession(ctx, supervise.SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          argv[0],
		Args:          argv[1:],
		Stderr:        os.Stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", argv[0], err)
	}
	t.session = session
	return session.Conn(), nil
}

func (t *cmdTransport) ensureCandidates() error {
	if t.candidates != nil {
		return nil
	}
	candidates, err := t.resolve()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return errors.New("syncservice: no dial candidates")
	}
	t.candidates = candidates
	return nil
}

func (t *cmdTransport) backoffRemaining() time.Duration {
	if t.resetCount < 2 || transportBackoffBase <= 0 {
		return 0
	}
	delay := transportBackoffBase
	for range t.resetCount - 2 {
		delay *= 2
		if delay >= transportBackoffMax {
			delay = transportBackoffMax
			break
		}
	}
	if elapsed := time.Since(t.resetAt); elapsed < delay {
		return delay - elapsed
	}
	return 0
}

func (t *cmdTransport) armBackoff() {
	t.resetCount++
	t.resetAt = time.Now()
}

func (t *cmdTransport) reset(ctx context.Context) {
	if t.session == nil {
		return
	}
	_ = t.session.Stop(context.WithoutCancel(ctx))
	t.session = nil
}

func (t *cmdTransport) activeCandidate() string {
	argv := t.candidates[t.idx]
	if t.resolve != nil {
		return argv[len(argv)-2]
	}
	return argv[0]
}

func (t *cmdTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	if t.client != nil {
		_ = t.client.Close()
	}
	if t.session == nil {
		return nil
	}
	active := t.activeCandidate()
	waitCtx, cancel := context.WithTimeout(context.Background(), closeGrace)
	waitErr := t.session.Wait(waitCtx)
	cancel()
	var stopErr error
	if waitErr != nil {
		stopErr = t.session.Stop(context.Background())
	}
	t.session = nil
	if err := errors.Join(waitErr, stopErr); err != nil {
		return fmt.Errorf("wait for %s to exit: %w", active, err)
	}
	return nil
}
