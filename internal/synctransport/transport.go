// Package synctransport implements module-private process-backed transports.
package synctransport

import (
	"context"
	"encoding/json"
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
	backoffBase = 500 * time.Millisecond
	backoffMax  = 30 * time.Second
	errBackoff  = errors.New("syncservice: transport backing off")
)

// Response is the exact raw-result sync-service response envelope.
type Response struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error,omitempty"`
}

// Transport carries one typed sync-service request.
type Transport interface {
	Do(context.Context, *rpc.Request) (*Response, error)
	Close() error
}

// Socket returns a persistent transport to a resident Unix socket.
func Socket(sock string) Transport {
	return &socketTransport{client: rpc.NewClient(rpc.ClientConfig{
		Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild,
	})}
}

type socketTransport struct{ client *rpc.Client }

func (t *socketTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	return call(ctx, t.client, req)
}

func (t *socketTransport) Close() error { return t.client.Close() }

// NewStdio returns a module-private persistent transport over a spawned bridge.
func NewStdio(pool *supervise.Pool, name string, args ...string) Transport {
	return &CommandTransport{pool: pool, candidates: [][]string{append([]string{name}, args...)}}
}

// NewSSHStdio returns a module-private persistent transport through a peer.
func NewSSHStdio(pool *supervise.Pool, peer, remoteCmd string) Transport {
	return &CommandTransport{pool: pool, resolve: func() ([][]string, error) {
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

// NewCandidates returns a module-private transport over exact test candidates.
func NewCandidates(pool *supervise.Pool, candidates [][]string) *CommandTransport {
	return &CommandTransport{pool: pool, candidates: candidates}
}

// CommandTransport owns one persistent daemonkit session and bridge process.
type CommandTransport struct {
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

// Do sends one typed request over the persistent bridge.
func (t *CommandTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("syncservice: transport closed")
	}
	if t.client == nil {
		t.client = rpc.NewClient(rpc.ClientConfig{Dial: t.dialLocked, WireBuild: rpc.WireBuild})
	}
	resp, err := call(ctx, t.client, req)
	if err == nil {
		t.resetCount = 0
		return resp, nil
	}
	if errors.Is(err, errBackoff) {
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

func (t *CommandTransport) dialLocked(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.closed {
		return nil, errors.New("syncservice: transport closed")
	}
	if wait := t.backoffRemaining(); wait > 0 {
		return nil, fmt.Errorf("%w %s after %d consecutive failures", errBackoff, wait.Round(time.Millisecond), t.resetCount)
	}
	if err := t.ensureCandidates(); err != nil {
		return nil, err
	}
	if t.session != nil {
		t.reset(ctx)
	}
	return t.spawn(ctx)
}

func (t *CommandTransport) spawn(ctx context.Context) (net.Conn, error) {
	argv := t.candidates[t.idx]
	session, err := t.pool.StartSession(ctx, supervise.SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask, Path: argv[0], Args: argv[1:], Stderr: os.Stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", argv[0], err)
	}
	t.session = session
	return session.Conn(), nil
}

func (t *CommandTransport) ensureCandidates() error {
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

func (t *CommandTransport) backoffRemaining() time.Duration {
	if t.resetCount < 2 || backoffBase <= 0 {
		return 0
	}
	delay := backoffBase
	for range t.resetCount - 2 {
		delay *= 2
		if delay >= backoffMax {
			delay = backoffMax
			break
		}
	}
	if elapsed := time.Since(t.resetAt); elapsed < delay {
		return delay - elapsed
	}
	return 0
}

func (t *CommandTransport) armBackoff() {
	t.resetCount++
	t.resetAt = time.Now()
}

func (t *CommandTransport) reset(ctx context.Context) {
	if t.session == nil {
		return
	}
	_ = t.session.Stop(context.WithoutCancel(ctx))
	t.session = nil
}

func (t *CommandTransport) activeCandidate() string {
	argv := t.candidates[t.idx]
	if t.resolve != nil {
		return argv[len(argv)-2]
	}
	return argv[0]
}

// Close settles the active bridge and permanently closes the transport.
func (t *CommandTransport) Close() error {
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

// EnsureCandidates resolves deferred peer addresses for module tests.
func (t *CommandTransport) EnsureCandidates() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ensureCandidates()
}

// Candidates returns a defensive copy of the resolved argv candidates.
func (t *CommandTransport) Candidates() [][]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([][]string, len(t.candidates))
	for i := range t.candidates {
		result[i] = append([]string(nil), t.candidates[i]...)
	}
	return result
}

// State returns the selected candidate and whether a session is active.
func (t *CommandTransport) State() (int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.idx, t.session != nil
}

// FailureState returns the consecutive reset count and current backoff wait.
func (t *CommandTransport) FailureState() (int, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resetCount, t.backoffRemaining()
}

// ReplaceCandidates changes the exact argv set for module tests.
func (t *CommandTransport) ReplaceCandidates(candidates [][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.candidates = candidates
	t.idx = 0
}

// BackdateReset moves the latest failure time backwards for module tests.
func (t *CommandTransport) BackdateReset(age time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resetAt = time.Now().Add(-age)
}

// SetBackoff replaces command backoff timing for module tests.
func SetBackoff(base, maximum time.Duration) func() {
	previousBase, previousMax := backoffBase, backoffMax
	backoffBase, backoffMax = base, maximum
	return func() { backoffBase, backoffMax = previousBase, previousMax }
}
