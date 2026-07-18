package rpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

// ErrFrameTooLarge aliases wire.ErrFrameTooLarge: a request frame crossed the
// MaxLine cap before its newline. Aliasing keeps a single sentinel identity across
// the daemonkit boundary rather than re-declaring an equivalent error.
var ErrFrameTooLarge = wire.ErrFrameTooLarge

// ErrFrameContainsLF aliases wire.ErrFrameContainsLF: a raw frame handed to the
// framing writer carried an embedded newline that would split the stream.
var ErrFrameContainsLF = wire.ErrFrameContainsLF

// peerPolicy is the resident socket's trust policy: the same-effective-UID floor
// with no code-signing Requirement, the daemonkit equivalent of the LOCAL_PEERCRED
// same-UID check the framing swap replaces.
var peerPolicy = trust.Policy{}

type peerPIDKey struct{}

type peerSIDKey struct{}

// PeerPID returns the PID of the client process on the unix-socket connection that
// carried the request, captured via LOCAL_PEERPID when the connection was accepted.
// It reports false when no PID was captured: the sockopt failed or is unsupported,
// or the handler ctx never passed through Serve (ServeConn, a bare ctx in tests).
func PeerPID(ctx context.Context) (int, bool) {
	pid, ok := ctx.Value(peerPIDKey{}).(int)
	return pid, ok
}

// PeerSID returns the session ID of the client process on the unix-socket connection
// that carried the request, derived via getsid(2) from the PID captured at accept.
// It reports false when no session ID was captured: the peer PID was unavailable or
// getsid failed, or the handler ctx never passed through Serve.
func PeerSID(ctx context.Context) (int, bool) {
	sid, ok := ctx.Value(peerSIDKey{}).(int)
	return sid, ok
}

// Listen binds a unix socket at sockPath under a proc.SingleEntrant flock, so a
// second serve gets proc.ErrPeerStarting rather than stealing a running daemon's
// socket. The returned listener holds the lock for its lifetime and releases it on
// Close; a stale socket left by a crashed daemon (lock free) is unlinked and rebound.
// ctx bounds SingleEntrant's post-eviction lock poll; the bind itself is prompt.
func Listen(ctx context.Context, sockPath string) (net.Listener, error) {
	se := proc.SingleEntrant{
		Socket: sockPath,
		Evict:  func() (bool, error) { return false, nil },
	}
	ln, lock, err := se.Listen(ctx)
	if err != nil {
		return nil, err
	}
	return &lockedListener{Listener: ln, lock: lock}, nil
}

// lockedListener releases the SingleEntrant socket lock when the listener closes,
// keeping the bind single-entrant for the listener's whole lifetime.
type lockedListener struct {
	net.Listener
	lock *os.File
}

func (l *lockedListener) Close() error {
	err := l.Listener.Close()
	_ = l.lock.Close()
	return err
}

// Serve accepts and handles one request per connection until ctx is canceled, then
// returns nil. Canceling ctx closes ln, which unblocks Accept; it does not close ln
// itself, since the caller owns the listener. Each connection is checked for a
// same-UID peer before any byte is read, then bounded by ReadTimeout and MaxLine;
// the dispatch ctx carries the peer PID and session ID (see PeerPID, PeerSID) and is
// canceled when the client connection closes, so an abandoned request never runs to
// the full DispatchTimeout.
func Serve(ctx context.Context, ln net.Listener, d *Dispatcher) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept rpc connection: %w", err)
		}
		go handleConn(ctx, conn.(*net.UnixConn), d)
	}
}

func handleConn(ctx context.Context, conn *net.UnixConn, d *Dispatcher) {
	defer func() { _ = conn.Close() }()

	fr := wire.NewFraming(conn)
	// MaxLine-1: wire bounds the payload, ReadLine counted the '\n' — keeps the boundary.
	fr.MaxLine = MaxLine - 1

	peer, err := wire.PeerFromConn(conn)
	if err != nil {
		_ = fr.WriteJSON(&Response{OK: false, Error: fmt.Sprintf("read peer credentials: %v", err)})
		return
	}
	if err := peerPolicy.Check(peer); err != nil {
		_ = fr.WriteJSON(&Response{OK: false, Error: err.Error()})
		return
	}

	if pid, err := peerPID(conn); err == nil {
		ctx = context.WithValue(ctx, peerPIDKey{}, pid)
		if sid, err := sidOf(pid); err == nil {
			ctx = context.WithValue(ctx, peerSIDKey{}, sid)
		}
	}

	if err := conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		return
	}
	line, err := fr.ReadFrame()
	if err != nil {
		_ = fr.WriteJSON(&Response{OK: false, Error: fmt.Sprintf("read request: %v", err)})
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear; dispatch may run long

	req, err := DecodeRequest(line)
	if err != nil {
		_ = fr.WriteJSON(&Response{OK: false, Error: err.Error()})
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Read the raw conn, not the framing reader, so bytes it buffered past the line
	// don't fire this; the deferred Close unblocks the Read when the handler returns.
	go func() {
		_, _ = conn.Read(make([]byte, 1))
		cancel()
	}()
	_ = fr.WriteJSON(d.Dispatch(ctx, req))
}

// ServeConn runs a streaming request/response loop over one long-lived bidirectional
// pipe — a spawned child's stdin/stdout, or an ssh pipe — until the writer closes
// (clean io.EOF, returns nil) or ctx is canceled. It reads each request line bounded
// by MaxLine, dispatches it, and writes the response line back, so many requests
// share one connection. Trust is established out of band (process ancestry, ssh
// auth), so there is no peer-credential check; the resident unix socket keeps its
// per-connection peercred check via Serve. ServeConn does not close rw.
func ServeConn(ctx context.Context, rw io.ReadWriter, d *Dispatcher) error {
	r := bufio.NewReader(rw)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := ReadLine(r, MaxLine)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read request: %w", err)
		}
		req, err := DecodeRequest(line)
		if err != nil {
			writeResponse(rw, &Response{OK: false, Error: err.Error()})
			continue
		}
		writeResponse(rw, d.Dispatch(ctx, req))
	}
}

func writeResponse(conn io.Writer, resp *Response) {
	data, err := EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(data)
}
