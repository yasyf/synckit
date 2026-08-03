// Package synctransport implements Synckit's fixed local and remote service transports.
package synctransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

const (
	closeGrace     = 3 * time.Second
	spawnBudget    = 30 * time.Second
	maxStderrBytes = 64 << 10
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

// Spawner starts one long-lived owned child under a durable process-ownership
// scope. Both *daemonkit.Owned (a CLI scope) and daemonkit.Ctx (a daemon's)
// satisfy it.
type Spawner interface {
	Spawn(ctx context.Context, cmd daemonkit.Cmd, channel daemonkit.Channel, stderr io.Writer) (*daemonkit.Child, error)
}

type failedTransport struct{ err error }

// Failed returns a transport that reports one construction failure.
func Failed(err error) Transport { return failedTransport{err: err} }

func (t failedTransport) Do(context.Context, *rpc.Request) (*Response, error) { return nil, t.err }
func (failedTransport) Close() error                                          { return nil }

// Local returns a persistent transport to the resident daemon spec names. The
// lane verifies the accepting process against spec's Trust.Serving on every
// acquisition, so a same-UID squatter that rebound the socket never sees a
// payload.
func Local(spec daemonkit.Daemon) Transport {
	client, err := daemonkit.Open(spec)
	if err != nil {
		return Failed(err)
	}
	return &localTransport{client: rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	})}
}

type localTransport struct{ client *rpc.Client }

func (t *localTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	response, err := t.client.Call(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (t *localTransport) Close() error { return t.client.Close() }

// NewSpawned returns the fixed local spawned-service transport: one sealed
// child per session, reached over the handoff socketpair daemonkit establishes
// before the child runs its first instruction.
func NewSpawned(spawner Spawner, executable, serviceID string) Transport {
	return &spawnedTransport{spawner: spawner, executable: executable, serviceID: serviceID}
}

type spawnedTransport struct {
	spawner    Spawner
	executable string
	serviceID  string

	mu      sync.Mutex
	client  *rpc.Client
	session *spawnedProcess
	closed  bool
}

func (t *spawnedTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("syncservice: spawned transport closed")
	}
	if t.client == nil {
		if err := t.start(ctx); err != nil {
			return nil, err
		}
	}
	response, err := t.client.Call(ctx, request)
	if err != nil {
		_ = t.reset(ctx)
		return nil, err
	}
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (t *spawnedTransport) start(ctx context.Context) error {
	cmd, err := spawnedCommand(t.executable, []string{rpc.RemoteServeCommand, t.serviceID})
	if err != nil {
		return err
	}
	cmd.Limits = rpc.SpawnLimits()
	session, err := startSpawnedProcess(ctx, t.spawner, cmd, daemonkit.ChannelHandoff)
	if err != nil {
		return fmt.Errorf("syncservice: spawn local service: %w", err)
	}
	t.client = rpc.NewClient(rpc.ClientConfig{
		Open: func(openCtx context.Context) (*daemonkit.Business, error) {
			return session.child.Business(openCtx, rpc.Contract())
		},
	})
	t.session = session
	return nil
}

func (t *spawnedTransport) reset(ctx context.Context) error {
	var err error
	if t.client != nil {
		err = errors.Join(err, t.client.Close())
		t.client = nil
	}
	if t.session != nil {
		err = errors.Join(err, t.session.close(ctx))
		t.session = nil
	}
	return err
}

func (t *spawnedTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	return t.reset(context.Background())
}

// NewRemote returns the sole strict SSH transport for one registered host fact.
func NewRemote(spawner Spawner, fact hostregistry.SSHHostFact, knownHostsPath, serviceID string) Transport {
	return &remoteTransport{
		spawner: spawner, fact: fact, knownHostsPath: knownHostsPath, serviceID: serviceID,
	}
}

type remoteTransport struct {
	spawner        Spawner
	fact           hostregistry.SSHHostFact
	knownHostsPath string
	serviceID      string

	mu      sync.Mutex
	client  *rpc.Client
	session *spawnedProcess
	index   int
	closed  bool
}

func (t *remoteTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("syncservice: remote transport closed")
	}
	if t.client == nil {
		t.client = rpc.NewClient(rpc.ClientConfig{Open: t.dial})
	}
	response, err := t.client.Call(ctx, request)
	if err != nil {
		if undispatched(err) && t.index+1 < len(t.fact.Addresses) {
			t.index++
		}
		_ = t.reset(ctx)
		return nil, err
	}
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

// undispatched reports whether the call provably never reached the peer's
// dispatch, so the next address may carry it. The predicate is the client's own
// TransportError, not daemonkit's classifier: a lane that failed to open —
// ssh never spawned, the hello never verified — carries no daemonkit call
// error to classify, and that unreachable-address case is the one failover
// exists for.
func undispatched(err error) bool {
	var transportErr *rpc.TransportError
	return errors.As(err, &transportErr) && transportErr.Undispatched
}

// dial opens one ssh session to the current dial address and hands its stdio
// channel to the business lane. SSH authenticated the transport, so the lane is
// daemonkit's named BusinessOverConn waiver: no kernel peer credentials exist
// on a pipe to another machine.
func (t *remoteTransport) dial(ctx context.Context) (*daemonkit.Business, error) {
	if t.index >= len(t.fact.Addresses) {
		return nil, errors.New("syncservice: remote host has no dial address")
	}
	argv, err := hostregistry.RemoteSSHArgv(t.fact, t.fact.Addresses[t.index], t.knownHostsPath, t.serviceID)
	if err != nil {
		return nil, err
	}
	cmd, err := spawnedCommand(argv[0], argv[1:])
	if err != nil {
		return nil, err
	}
	session, err := startSpawnedProcess(ctx, t.spawner, cmd, daemonkit.ChannelStdio)
	if err != nil {
		return nil, fmt.Errorf("syncservice: spawn %s: %w", argv[0], err)
	}
	// Conn hands the parent side of the child's stdio over for good: a failed
	// attach leaves nobody else holding it, so this is where it closes.
	conn, err := session.child.Conn()
	if err != nil {
		return nil, errors.Join(err, session.close(ctx))
	}
	business, err := t.attach(ctx, conn)
	if err != nil {
		return nil, errors.Join(err, conn.Close(), session.close(ctx), session.stderrError())
	}
	t.session = session
	return business, nil
}

func (t *remoteTransport) attach(ctx context.Context, conn net.Conn) (*daemonkit.Business, error) {
	nonce, err := rpc.NewRemoteNonce()
	if err != nil {
		return nil, err
	}
	if err := rpc.VerifyRemoteHello(ctx, conn, nonce); err != nil {
		return nil, err
	}
	return daemonkit.BusinessOverConn(ctx, conn, rpc.Contract())
}

func (t *remoteTransport) reset(ctx context.Context) error {
	var err error
	if t.client != nil {
		err = errors.Join(err, t.client.Close())
		t.client = nil
	}
	if t.session != nil {
		err = errors.Join(err, t.session.close(ctx))
		t.session = nil
	}
	return err
}

func (t *remoteTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	return t.reset(context.Background())
}

// spawnedProcess is one owned child plus the bounded stderr daemonkit drains
// for its whole life.
type spawnedProcess struct {
	child  *daemonkit.Child
	stderr *daemonkit.Capture
	once   sync.Once
	err    error
}

// startSpawnedProcess spawns cmd on channel under its own session, bounding the
// spawn — the record write, the exec-posture verification, the channel — by
// spawnBudget rather than by the caller's whole request deadline.
func startSpawnedProcess(
	parent context.Context,
	spawner Spawner,
	cmd daemonkit.Cmd,
	channel daemonkit.Channel,
) (*spawnedProcess, error) {
	ctx, cancel := context.WithTimeout(parent, spawnBudget)
	defer cancel()
	stderr := daemonkit.NewCapture(maxStderrBytes)
	child, err := spawner.Spawn(ctx, cmd, channel, stderr)
	if err != nil {
		return nil, err
	}
	return &spawnedProcess{child: child, stderr: stderr}, nil
}

func (p *spawnedProcess) close(parent context.Context) error {
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), closeGrace)
		defer cancel()
		_, stopErr := p.child.Stop(ctx)
		p.err = errors.Join(stopErr, p.child.StderrErr())
	})
	return p.err
}

func (p *spawnedProcess) stderrError() error {
	data := bytes.TrimSpace(p.stderr.Bytes())
	if len(data) == 0 {
		return nil
	}
	suffix := ""
	if p.stderr.Truncated() {
		suffix = " (truncated)"
	}
	return fmt.Errorf("ssh stderr%s: %s", suffix, data)
}

// spawnedCommand is the exact command shape every synckit child runs: its own
// session, a sealed environment carrying only HOME, and the same-user exec
// posture stated at the spawn site.
func spawnedCommand(executable string, args []string) (daemonkit.Cmd, error) {
	if executable == "" {
		return daemonkit.Cmd{}, errors.New("syncservice: empty process argv")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return daemonkit.Cmd{}, fmt.Errorf("syncservice: resolve home: %w", err)
	}
	return daemonkit.Cmd{
		Path: executable, Args: args, Dir: filepath.Dir(executable),
		Env: []string{"HOME=" + home}, Session: true, Exec: daemonkit.ServingSameUser(),
	}, nil
}
