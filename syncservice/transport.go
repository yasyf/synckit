package syncservice

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// closeGrace bounds how long Close waits for a child to exit on stdin EOF before it
// kills the child outright.
const closeGrace = 2 * time.Second

// Socket returns a [Transport] that reaches the resident daemon over its unix socket
// at sock. Each Do dials, writes one request line, and reads one response line via
// rpc.CallRaw — the resident socket's one-request-per-connection wire — so the raw
// response bytes (and any int64 CRDT stamps) survive byte-exact.
func Socket(sock string) Transport {
	return socketTransport{sock: sock}
}

type socketTransport struct {
	sock string
}

func (t socketTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	respLine, err := rpc.CallRaw(ctx, t.sock, line)
	if err != nil {
		return nil, err
	}
	return decodeEnvelope(respLine)
}

func (socketTransport) Close() error { return nil }

// Stdio returns a [Transport] that frames requests over a spawned `name args...`
// child's stdin (write) and stdout (read). The child is started lazily on the first
// Do and re-spawned after any framing error.
func Stdio(name string, args ...string) Transport {
	return &cmdTransport{name: name, args: args}
}

// SSHStdio returns a [Transport] that frames requests over `ssh` to a peer's
// rpc-serve bridge, using the same ssh argv (brew-shellenv wrapper and connection
// options) the host registry uses for every remote command.
func SSHStdio(peer, remoteCmd string) Transport {
	argv := hostregistry.SSHArgv(peer, remoteCmd)
	return &cmdTransport{name: argv[0], args: argv[1:]}
}

// cmdTransport frames typed rpc over a spawned child's stdio. The pipe is strict
// request/response, so Do is serialized behind mu; a framing error kills and reaps
// the child so the next Do self-heals by re-spawning.
type cmdTransport struct {
	name string
	args []string

	mu     sync.Mutex
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	outc   io.ReadCloser
	closed bool
}

func (t *cmdTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.start(); err != nil {
		return nil, err
	}

	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	type result struct {
		resp *Response
		err  error
	}
	done := make(chan result, 1)
	// Snapshot the pipes for the goroutine: when ctx expires first, Do returns and
	// reset() clears the receiver fields while exchange may still be running.
	in, out := t.in, t.out
	go func() {
		resp, err := exchange(in, out, line)
		done <- result{resp: resp, err: err}
	}()

	select {
	case <-ctx.Done():
		t.reset()
		return nil, ctx.Err()
	case r := <-done:
		if r.err != nil {
			t.reset()
			return nil, r.err
		}
		return r.resp, nil
	}
}

// start spawns the child and wires its stdio if it is not already running. The caller
// holds mu. A closed transport never re-spawns: a late Do (a watch debounce timer
// firing after generation teardown) returns an error rather than reviving the child.
// Stderr goes to os.Stderr so the child's diagnostics stay off the framing stdout.
func (t *cmdTransport) start() error {
	if t.closed {
		return fmt.Errorf("syncservice: transport closed")
	}
	if t.cmd != nil {
		return nil
	}
	//nolint:gosec // G204: name and args come from trusted local state (a configured child binary or ssh to a registered peer), not untrusted input.
	cmd := exec.Command(t.name, t.args...)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe for %s: %w", t.name, err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe for %s: %w", t.name, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", t.name, err)
	}
	t.cmd = cmd
	t.in = in
	t.outc = out
	t.out = bufio.NewReader(out)
	return nil
}

// reset kills and reaps the child and clears its state so the next Do re-spawns. The
// caller holds mu.
func (t *cmdTransport) reset() {
	if t.cmd == nil {
		return
	}
	_ = t.cmd.Process.Kill()
	_ = t.cmd.Wait()
	t.cmd = nil
	t.in = nil
	t.out = nil
	t.outc = nil
}

// Close closes the child's stdin so it exits on EOF, then kills and reaps it if it is
// still alive. It is idempotent: closing an already-closed transport is a no-op.
func (t *cmdTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	if t.cmd == nil {
		return nil
	}
	_ = t.in.Close()
	waited := make(chan error, 1)
	go func() { waited <- t.cmd.Wait() }()

	var err error
	select {
	case err = <-waited:
	case <-time.After(closeGrace):
		_ = t.cmd.Process.Kill()
		err = <-waited
	}
	t.cmd = nil
	t.in = nil
	t.out = nil
	t.outc = nil
	if err != nil {
		return fmt.Errorf("wait for %s to exit: %w", t.name, err)
	}
	return nil
}

// exchange writes one request line plus '\n' to the child and reads one response
// line. It runs on a goroutine that can outlive Do's mu critical section — a ctx
// expiry abandons it mid-flight — so it takes its generation's pipes as parameters
// rather than reading receiver fields that reset() clears concurrently. reset's
// kill and reap close the pipes, so an abandoned exchange fails its next I/O and
// exits through the buffered done channel.
func exchange(in io.Writer, out *bufio.Reader, line []byte) (*Response, error) {
	if _, err := in.Write(line); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	if _, err := in.Write([]byte{'\n'}); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	respLine, err := rpc.ReadLine(out, rpc.MaxLine)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return decodeEnvelope(respLine)
}

// decodeEnvelope parses one response line into a [Response], keeping its result as
// raw JSON bytes so int64 CRDT stamps round-trip byte-exact.
func decodeEnvelope(line []byte) (*Response, error) {
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &resp, nil
}
