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
	"syscall"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// closeGrace bounds how long Close waits for a child to exit on stdin EOF before it
// kills the child outright.
const closeGrace = 2 * time.Second

// tunnelWaitDelay backstops Wait's teardown after a group kill so a descendant holding the
// framing pipes cannot wedge it.
const tunnelWaitDelay = 5 * time.Second

// transportBackoffBase and transportBackoffMax bound the exponential backoff start()
// applies once respawns pile up: the first reset self-heals immediately, each further
// consecutive reset (no success between) doubles the wait from base, capped at max, so
// a wedged tunnel stops storming respawns. Vars so tests shrink or disable them.
var (
	transportBackoffBase = 500 * time.Millisecond
	transportBackoffMax  = 30 * time.Second
)

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
	return &cmdTransport{candidates: [][]string{append([]string{name}, args...)}}
}

// SSHStdio returns a [Transport] that frames requests over `ssh` to a peer's rpc-serve
// bridge. It resolves DialAddrs lazily on the first spawn — not at construction — so
// addresses recorded after it is built are used and a resolution failure surfaces from
// Do rather than a silent [peer] fallback. It holds one ssh argv per dial candidate
// (LAN/.local first, the tailnet FQDN last). Within a request it fails over to the next
// candidate only before the first response byte; once a byte arrives it pins to that
// candidate for the rest of the request. Across requests a still-live child is reused at
// its candidate; only a terminal request (one that killed the child) re-selects from the
// top — so a request may reach more than one candidate. That at-least-once delivery (see
// [cmdTransport.Do]) means every method routed through it must be idempotent or convergent.
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

// cmdTransport frames typed rpc over a spawned child's stdio. The pipe is strict
// request/response, so Do is serialized behind mu; a framing error kills and reaps the
// child so the next Do self-heals by re-spawning. For a multi-address ssh tunnel it holds
// one candidate argv per dial address and, within a request, advances idx to the next
// only before the first response byte; across requests a killed child re-selects from the
// top. Consecutive failures (framing or spawn) arm start()'s backoff.
type cmdTransport struct {
	// resolve lazily produces the dial candidates on the first spawn; nil when
	// candidates were supplied directly. SSHStdio sets it so a DialAddrs failure
	// surfaces from Do and addresses recorded after construction are picked up at spawn.
	resolve    func() ([][]string, error)
	candidates [][]string

	mu     sync.Mutex
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	outc   io.ReadCloser
	closed bool

	idx        int
	resetCount int
	resetAt    time.Time
}

// Do frames one request over the current dial candidate and returns its response,
// serialized behind mu. Within a request, before the first response byte a dead candidate
// fails over to the next dial address, so a request may reach more than one candidate:
// this within-request failover is at-least-once delivery, strictly pre-first-response.
// Once a candidate emits its first response byte the request pins to it and never fails
// over again — a partial or dropped response surfaces as an error, never a re-send. The
// pin is per request, but a still-live child from a prior successful Do is reused at its
// candidate across Dos; only a terminal Do — one that killed the child — makes the next Do
// re-select from the top, LAN first. Every method carried over a tunnel must therefore be idempotent
// or convergent — sync and reconcile are by construction (a convergent registry merged
// under the exclusive dispatch lane).
func (t *cmdTransport) Do(ctx context.Context, req *rpc.Request) (*Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	// No live child (terminal previous Do, or first Do): re-select from the top. A live
	// child from a prior successful Do is reused at its candidate.
	if t.cmd == nil {
		t.idx = 0
	}

	type result struct {
		resp    *Response
		gotByte bool
		err     error
	}
	// bound is per request: set on the first response byte to block failover, reset each
	// Do so a prior candidate is free to be re-selected or failed past.
	bound := false
	for {
		if err := t.start(); err != nil {
			return nil, err
		}

		done := make(chan result, 1)
		// Snapshot the pipes for the goroutine: when ctx expires first, Do returns and
		// reset() clears the receiver fields while exchange may still be running.
		in, out := t.in, t.out
		go func() {
			resp, gotByte, err := exchange(in, out, line)
			done <- result{resp: resp, gotByte: gotByte, err: err}
		}()

		select {
		case <-ctx.Done():
			t.reset()
			t.armBackoff()
			return nil, ctx.Err()
		case r := <-done:
			if r.gotByte {
				bound = true
			}
			if r.err == nil {
				t.resetCount = 0
				return r.resp, nil
			}
			t.reset()
			// Pre-first-response only, and not once the caller gave up: spawning another
			// candidate then would write a live request past a dead deadline.
			if !bound && t.idx+1 < len(t.candidates) {
				if ctxErr := ctx.Err(); ctxErr != nil {
					t.armBackoff()
					return nil, ctxErr
				}
				t.idx++
				continue
			}
			t.armBackoff()
			return nil, r.err
		}
	}
}

// start makes the current dial candidate's child ready to frame, spawning it if one is
// not already running. The caller holds mu. A closed transport never re-spawns; a Do
// inside the backoff window armed by consecutive failures fails fast; and a spawn failure
// arms that same backoff, so a persistently unspawnable child (a missing binary) throttles
// rather than storming respawns. A resolver failure stays unthrottled.
func (t *cmdTransport) start() error {
	if t.closed {
		return fmt.Errorf("syncservice: transport closed")
	}
	if t.cmd != nil {
		return nil
	}
	if wait := t.backoffRemaining(); wait > 0 {
		return fmt.Errorf("syncservice: transport backing off %s after %d consecutive failures", wait.Round(time.Millisecond), t.resetCount)
	}
	if err := t.ensureCandidates(); err != nil {
		return err
	}
	if err := t.spawn(); err != nil {
		t.armBackoff()
		return err
	}
	return nil
}

// spawn starts the current dial candidate's child and wires its stdio. The caller holds
// mu. Stderr goes to os.Stderr so the child's diagnostics stay off the framing stdout.
func (t *cmdTransport) spawn() error {
	argv := t.candidates[t.idx]
	//nolint:gosec // G204: argv comes from trusted local state (a configured child binary or ssh to a registered peer), not untrusted input.
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = tunnelWaitDelay
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe for %s: %w", argv[0], err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe for %s: %w", argv[0], err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", argv[0], err)
	}
	t.cmd = cmd
	t.in = in
	t.outc = out
	t.out = bufio.NewReader(out)
	return nil
}

// ensureCandidates resolves the dial candidates on the first spawn, so SSHStdio's
// DialAddrs runs at spawn (picking up addresses recorded after construction) and a
// resolution error surfaces from Do. The caller holds mu; a direct-candidate transport
// has no resolver and is a no-op.
func (t *cmdTransport) ensureCandidates() error {
	if t.candidates != nil {
		return nil
	}
	candidates, err := t.resolve()
	if err != nil {
		return err
	}
	t.candidates = candidates
	return nil
}

// backoffRemaining is how long start() must still wait before respawning: zero for the
// first reset (immediate self-heal) or when backoff is disabled, otherwise the
// exponential delay for the consecutive-reset count minus the time already elapsed.
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

// armBackoff records a terminal Do failure so the next start() throttles: two
// consecutive failed Dos (no success between) arm the exponential backoff window. The
// caller holds mu.
func (t *cmdTransport) armBackoff() {
	t.resetCount++
	t.resetAt = time.Now()
}

// reset SIGKILLs the child's whole process group — so a tunnel's ssh helper that inherited
// the framing pipes dies with it rather than orphaning — reaps the child, and clears its
// state so the next start re-spawns. The caller holds mu.
func (t *cmdTransport) reset() {
	if t.cmd == nil {
		return
	}
	killGroup(t.cmd.Process.Pid)
	_ = t.cmd.Wait()
	t.cmd = nil
	t.in = nil
	t.out = nil
	t.outc = nil
}

// activeCandidate names the dial target the live child runs on: the ssh dial address for
// a tunnel (whose argv[0] is the bare "ssh"), or the child binary for a direct transport.
// The caller holds mu and a live child, so t.idx indexes the running candidate.
func (t *cmdTransport) activeCandidate() string {
	argv := t.candidates[t.idx]
	if t.resolve != nil {
		return argv[len(argv)-2]
	}
	return argv[0]
}

// Close closes the child's stdin so it exits on EOF, then SIGKILLs its whole process group
// and reaps it — so a tunnel's ssh helper that inherited the framing pipes dies too rather
// than orphaning. It is idempotent: closing an already-closed transport is a no-op.
func (t *cmdTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	if t.cmd == nil {
		return nil
	}
	active := t.activeCandidate()
	_ = t.in.Close()
	waited := make(chan error, 1)
	go func() { waited <- t.cmd.Wait() }()

	var err error
	select {
	case err = <-waited:
	case <-time.After(closeGrace):
		killGroup(t.cmd.Process.Pid)
		err = <-waited
	}
	killGroup(t.cmd.Process.Pid) // reap any descendant that outlived the leader
	t.cmd = nil
	t.in = nil
	t.out = nil
	t.outc = nil
	if err != nil {
		return fmt.Errorf("wait for %s to exit: %w", active, err)
	}
	return nil
}

// killGroup best-effort SIGKILLs the process group led by pid; a negative pid targets the
// group and an already-dead group (ESRCH) is fine.
func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

// exchange writes one request line plus '\n' to the child and reads one response
// line. It runs on a goroutine that can outlive Do's mu critical section — a ctx
// expiry abandons it mid-flight — so it takes its generation's pipes as parameters
// rather than reading receiver fields that reset() clears concurrently. reset's
// kill and reap close the pipes, so an abandoned exchange fails its next I/O and
// exits through the buffered done channel. The returned bool reports whether a first
// response byte arrived: it is the transport's bind signal, so a partial response that
// then drops still pins the candidate rather than failing over.
func exchange(in io.Writer, out *bufio.Reader, line []byte) (*Response, bool, error) {
	if _, err := in.Write(line); err != nil {
		return nil, false, fmt.Errorf("write request: %w", err)
	}
	if _, err := in.Write([]byte{'\n'}); err != nil {
		return nil, false, fmt.Errorf("write request: %w", err)
	}
	if _, err := out.Peek(1); err != nil {
		return nil, false, fmt.Errorf("read response: %w", err)
	}
	respLine, err := rpc.ReadLine(out, rpc.MaxLine)
	if err != nil {
		return nil, true, fmt.Errorf("read response: %w", err)
	}
	resp, err := decodeEnvelope(respLine)
	return resp, true, err
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
