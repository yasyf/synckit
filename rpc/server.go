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
)

// Listen binds a unix socket at sockPath, first unlinking any stale socket left by a
// crashed daemon so a relaunch does not fail with EADDRINUSE.
func Listen(sockPath string) (net.Listener, error) {
	if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale rpc socket %s: %w", sockPath, err)
	}
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen on rpc socket %s: %w", sockPath, err)
	}
	return ln, nil
}

// Serve accepts and handles one request per connection until ctx is canceled, then
// returns nil. Canceling ctx closes ln, which unblocks Accept; it does not close ln
// itself, since the caller owns the listener. Each connection is checked for a
// same-UID peer before any byte is read, then bounded by ReadTimeout and MaxLine;
// the dispatch ctx is canceled when the client connection closes, so an abandoned
// request never runs to the full DispatchTimeout.
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

	switch uid, err := peerUID(conn); {
	case err != nil:
		writeResponse(conn, &Response{OK: false, Error: fmt.Sprintf("read peer uid: %v", err)})
		return
	case uid != os.Getuid():
		writeResponse(conn, &Response{OK: false, Error: fmt.Sprintf("peer uid %d is not %d", uid, os.Getuid())})
		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		return
	}
	line, err := ReadLine(bufio.NewReader(conn), MaxLine)
	if err != nil {
		writeResponse(conn, &Response{OK: false, Error: fmt.Sprintf("read request: %v", err)})
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear; dispatch may run long

	req, err := DecodeRequest(line)
	if err != nil {
		writeResponse(conn, &Response{OK: false, Error: err.Error()})
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The protocol allows no client bytes after the request line, so any Read result
	// — a byte, EOF, or an error — means the client is gone or misbehaving. Reading
	// the raw conn, not the bufio.Reader above, keeps bytes the reader already
	// buffered past the line from firing this; the deferred Close unblocks the Read
	// once the handler finishes, so the goroutine never outlives the connection.
	go func() {
		_, _ = conn.Read(make([]byte, 1))
		cancel()
	}()
	writeResponse(conn, d.Dispatch(ctx, req))
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
