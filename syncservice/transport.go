package syncservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

const (
	closeGrace      = 2 * time.Second
	tunnelWaitDelay = 5 * time.Second
)

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
func Stdio(name string, args ...string) Transport {
	return &cmdTransport{candidates: [][]string{append([]string{name}, args...)}}
}

// SSHStdio returns a persistent v1 transport through a peer's raw rpc bridge.
func SSHStdio(peer, remoteCmd string) Transport {
	return &cmdTransport{resolve: func() ([][]string, error) {
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
	resolve    func() ([][]string, error)
	candidates [][]string

	mu     sync.Mutex
	client *rpc.Client
	cmd    *exec.Cmd
	conn   *pipeConn
	closed bool

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
		t.reset()
		t.armBackoff()
		return nil, ctxErr
	}
	var transportErr *rpc.TransportError
	if errors.As(err, &transportErr) && transportErr.Outcome == wire.PreSendFailure && t.idx+1 < len(t.candidates) {
		t.idx++
	}
	t.reset()
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
	if t.cmd != nil {
		t.reset()
	}
	return t.spawn()
}

func (t *cmdTransport) spawn() (net.Conn, error) {
	argv := t.candidates[t.idx]
	//nolint:gosec // argv comes from a trusted manifest or registered host.
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = tunnelWaitDelay
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe for %s: %w", argv[0], err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe for %s: %w", argv[0], err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", argv[0], err)
	}
	conn := &pipeConn{in: in, out: out}
	t.cmd = cmd
	t.conn = conn
	return conn, nil
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

func (t *cmdTransport) reset() {
	if t.cmd == nil {
		return
	}
	if t.conn != nil {
		_ = t.conn.Close()
	}
	killGroup(t.cmd.Process.Pid)
	_ = t.cmd.Wait()
	t.cmd = nil
	t.conn = nil
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
	if t.cmd == nil {
		return nil
	}
	active := t.activeCandidate()
	waited := make(chan error, 1)
	go func() { waited <- t.cmd.Wait() }()
	var err error
	select {
	case err = <-waited:
	case <-time.After(closeGrace):
		killGroup(t.cmd.Process.Pid)
		err = <-waited
	}
	killGroup(t.cmd.Process.Pid)
	t.cmd = nil
	t.conn = nil
	if err != nil {
		return fmt.Errorf("wait for %s to exit: %w", active, err)
	}
	return nil
}

func killGroup(pid int) { _ = syscall.Kill(-pid, syscall.SIGKILL) }

type pipeConn struct {
	in  io.WriteCloser
	out io.ReadCloser

	closeOnce sync.Once
	mu        sync.Mutex
	timer     *time.Timer
	deadline  uint64
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.out.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return c.in.Write(p) }

func (c *pipeConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if c.timer != nil {
			c.timer.Stop()
		}
		c.mu.Unlock()
		err = errors.Join(c.in.Close(), c.out.Close())
	})
	return err
}

func (c *pipeConn) LocalAddr() net.Addr  { return pipeAddr("local") }
func (c *pipeConn) RemoteAddr() net.Addr { return pipeAddr("bridge") }

func (c *pipeConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline++
	generation := c.deadline
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if deadline.IsZero() {
		return nil
	}
	delay := time.Until(deadline)
	if delay < 0 {
		delay = 0
	}
	c.timer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		current := c.deadline == generation
		c.mu.Unlock()
		if current {
			_ = c.Close()
		}
	})
	return nil
}

func (c *pipeConn) SetReadDeadline(deadline time.Time) error  { return c.SetDeadline(deadline) }
func (c *pipeConn) SetWriteDeadline(deadline time.Time) error { return c.SetDeadline(deadline) }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }
