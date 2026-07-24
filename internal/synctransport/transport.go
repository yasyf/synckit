// Package synctransport implements Synckit's fixed local and remote service transports.
package synctransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

const (
	closeGrace      = 3 * time.Second
	maxStderrBytes  = 64 << 10
	spawnPolicyName = "synckit.spawn-policy.v1\x00"
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

type failedTransport struct{ err error }

// Failed returns a transport that reports one construction failure.
func Failed(err error) Transport { return failedTransport{err: err} }

func (t failedTransport) Do(context.Context, *rpc.Request) (*Response, error) { return nil, t.err }
func (failedTransport) Close() error                                          { return nil }

// Socket returns a persistent transport to one resident Unix socket.
func Socket(socket string) Transport {
	return &socketTransport{client: rpc.NewClient(rpc.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: rpc.WireBuild,
	})}
}

type socketTransport struct{ client *rpc.Client }

func (t *socketTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	response, err := t.client.Call(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (t *socketTransport) Close() error { return t.client.Close() }

// NewSpawned returns the fixed local spawned-service transport.
func NewSpawned(manager *proc.Manager, executable, serviceID string) Transport {
	return &spawnedTransport{manager: manager, executable: executable, serviceID: serviceID}
}

type spawnedTransport struct {
	manager    *proc.Manager
	executable string
	serviceID  string

	mu      sync.Mutex
	client  *rpc.SpawnedClient
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
	request, err := spawnedRequest(t.executable, []string{rpc.RemoteServeCommand, t.serviceID}, true)
	if err != nil {
		return err
	}
	child, receipt, err := t.manager.Prepare(ctx, request)
	if err != nil {
		return fmt.Errorf("syncservice: prepare local service: %w", err)
	}
	stderr, err := child.TakeStderr()
	if err != nil {
		_ = child.Stop(context.WithoutCancel(ctx))
		return err
	}
	session := newSpawnedProcess(child, stderr)
	if err := child.Start(ctx); err != nil {
		_ = session.close(ctx)
		return err
	}
	endpoint, err := child.ClaimSpawnedSession(ctx, receipt)
	if err != nil {
		_ = session.close(ctx)
		return err
	}
	client, err := rpc.NewSpawnedClient(ctx, endpoint)
	if err != nil {
		_ = session.close(ctx)
		return err
	}
	t.session = session
	t.client = client
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
func NewRemote(manager *proc.Manager, fact hostregistry.SSHHostFact, knownHostsPath, serviceID string) Transport {
	return &remoteTransport{
		manager: manager, fact: fact, knownHostsPath: knownHostsPath, serviceID: serviceID,
	}
}

type remoteTransport struct {
	manager        *proc.Manager
	fact           hostregistry.SSHHostFact
	knownHostsPath string
	serviceID      string

	mu     sync.Mutex
	client *rpc.Client
	pipe   *managedPipe
	index  int
	closed bool
}

func (t *remoteTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("syncservice: remote transport closed")
	}
	if t.client == nil {
		t.client = rpc.NewClient(rpc.ClientConfig{Dial: t.dial, WireBuild: rpc.WireBuild})
	}
	response, err := t.client.Call(ctx, request)
	if err != nil {
		var transportErr *rpc.TransportError
		if errors.As(err, &transportErr) && transportErr.Outcome == wire.PreSendFailure && t.index+1 < len(t.fact.Addresses) {
			t.index++
		}
		_ = t.reset(ctx)
		return nil, err
	}
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (t *remoteTransport) dial(ctx context.Context) (net.Conn, error) {
	if t.index >= len(t.fact.Addresses) {
		return nil, errors.New("syncservice: remote host has no dial address")
	}
	argv, err := hostregistry.RemoteSSHArgv(t.fact, t.fact.Addresses[t.index], t.knownHostsPath, t.serviceID)
	if err != nil {
		return nil, err
	}
	pipe, err := startManagedPipe(ctx, t.manager, argv)
	if err != nil {
		return nil, err
	}
	nonce, err := rpc.NewRemoteNonce()
	if err == nil {
		err = rpc.VerifyRemoteHello(ctx, pipe, nonce)
	}
	if err != nil {
		closeErr := pipe.close(ctx)
		return nil, errors.Join(err, closeErr, pipe.stderrError())
	}
	t.pipe = pipe
	return pipe, nil
}

func (t *remoteTransport) reset(ctx context.Context) error {
	var err error
	if t.client != nil {
		err = errors.Join(err, t.client.Close())
		t.client = nil
	}
	if t.pipe != nil {
		err = errors.Join(err, t.pipe.close(ctx))
		t.pipe = nil
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

type spawnedProcess struct {
	child  *proc.PreparedChild
	stderr *boundedCapture
	once   sync.Once
	err    error
}

func newSpawnedProcess(child *proc.PreparedChild, stderr *os.File) *spawnedProcess {
	return &spawnedProcess{child: child, stderr: newBoundedCapture(stderr)}
}

func (p *spawnedProcess) close(parent context.Context) error {
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), closeGrace)
		defer cancel()
		p.err = errors.Join(p.child.Stop(ctx), p.stderr.wait(ctx))
	})
	return p.err
}

type managedPipe struct {
	reader *os.File
	writer *os.File
	spawnedProcess
}

func startManagedPipe(ctx context.Context, manager *proc.Manager, argv []string) (*managedPipe, error) {
	if len(argv) == 0 {
		return nil, errors.New("syncservice: empty process argv")
	}
	request, err := spawnedRequest(argv[0], argv[1:], false)
	if err != nil {
		return nil, err
	}
	child, _, err := manager.Prepare(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("syncservice: prepare %s: %w", argv[0], err)
	}
	stdin, err := child.TakeStdin()
	if err != nil {
		_ = child.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	stdout, err := child.TakeStdout()
	if err != nil {
		_ = stdin.Close()
		_ = child.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	stderr, err := child.TakeStderr()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = child.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	pipe := &managedPipe{
		reader: stdout, writer: stdin,
		spawnedProcess: spawnedProcess{child: child, stderr: newBoundedCapture(stderr)},
	}
	if err := child.Start(ctx); err != nil {
		_ = pipe.close(ctx)
		return nil, err
	}
	return pipe, nil
}

func spawnedRequest(executable string, args []string, sealed bool) (proc.SpawnRequest, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return proc.SpawnRequest{}, fmt.Errorf("syncservice: resolve home: %w", err)
	}
	signature, err := proc.NewSignatureDigest(sha256.Sum256([]byte(spawnPolicyName + executable)))
	if err != nil {
		return proc.SpawnRequest{}, err
	}
	stdin, stdout := proc.StdioPipe, proc.StdioPipe
	if sealed {
		stdin, stdout = proc.StdioNull, proc.StdioNull
	}
	return proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass: proc.RecoveryTask, Executable: executable, Args: args,
		Dir: filepath.Dir(executable), Env: []string{"HOME=" + home},
		Stdin: stdin, Stdout: stdout, Stderr: proc.StdioPipe,
		SpawnedSession: sealed, ExpectedSignature: &signature,
	})
}

func (p *managedPipe) Read(buffer []byte) (int, error)  { return p.reader.Read(buffer) }
func (p *managedPipe) Write(buffer []byte) (int, error) { return p.writer.Write(buffer) }

func (p *managedPipe) Close() error {
	return p.close(context.Background())
}

func (p *managedPipe) close(parent context.Context) error {
	p.once.Do(func() {
		p.err = errors.Join(p.writer.Close(), p.reader.Close())
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), closeGrace)
		defer cancel()
		p.err = errors.Join(p.err, p.child.Stop(ctx), p.stderr.wait(ctx))
	})
	return p.err
}

func (p *managedPipe) LocalAddr() net.Addr  { return pipeAddress("local") }
func (p *managedPipe) RemoteAddr() net.Addr { return pipeAddress("child") }
func (p *managedPipe) SetDeadline(deadline time.Time) error {
	return errors.Join(p.reader.SetDeadline(deadline), p.writer.SetDeadline(deadline))
}

func (p *managedPipe) SetReadDeadline(deadline time.Time) error {
	return p.reader.SetReadDeadline(deadline)
}

func (p *managedPipe) SetWriteDeadline(deadline time.Time) error {
	return p.writer.SetWriteDeadline(deadline)
}

func (p *managedPipe) stderrError() error {
	data, truncated := p.stderr.snapshot()
	if len(data) == 0 {
		return nil
	}
	suffix := ""
	if truncated {
		suffix = " (truncated)"
	}
	return fmt.Errorf("ssh stderr%s: %s", suffix, bytes.TrimSpace(data))
}

type pipeAddress string

func (pipeAddress) Network() string  { return "pipe" }
func (a pipeAddress) String() string { return string(a) }

type boundedCapture struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	truncated bool
	reader    io.ReadCloser
	closeOnce sync.Once
	closeErr  error
	done      chan error
}

func newBoundedCapture(reader io.ReadCloser) *boundedCapture {
	capture := &boundedCapture{reader: reader, done: make(chan error, 1)}
	go func() {
		limited := &io.LimitedReader{R: reader, N: maxStderrBytes + 1}
		capture.mu.Lock()
		_, firstErr := io.Copy(&capture.buffer, limited)
		if capture.buffer.Len() > maxStderrBytes {
			capture.buffer.Truncate(maxStderrBytes)
			capture.truncated = true
		}
		capture.mu.Unlock()
		_, drainErr := io.Copy(io.Discard, reader)
		capture.done <- errors.Join(firstErr, drainErr, capture.closeReader())
	}()
	return capture
}

func (c *boundedCapture) wait(ctx context.Context) error {
	select {
	case err := <-c.done:
		return err
	case <-ctx.Done():
		return errors.Join(ctx.Err(), c.closeReader(), <-c.done)
	}
}

func (c *boundedCapture) closeReader() error {
	c.closeOnce.Do(func() { c.closeErr = c.reader.Close() })
	return c.closeErr
}

func (c *boundedCapture) snapshot() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buffer.Bytes()), c.truncated
}
