package rpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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
// same-UID peer before any byte is read, then bounded by ReadTimeout and MaxLine.
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
	line, err := readLine(bufio.NewReader(conn), MaxLine)
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
	writeResponse(conn, d.Dispatch(ctx, req))
}

func writeResponse(conn net.Conn, resp *Response) {
	data, err := EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(data)
}
