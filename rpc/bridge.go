package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
)

// Proxy carries one exact persistent daemonkit session between stdio and the
// resident Unix socket. It never parses, retries, or replays frames.
func Proxy(ctx context.Context, in io.Reader, out io.Writer, sockPath string) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
	if err != nil {
		return fmt.Errorf("dial daemon at %s: %w", sockPath, err)
	}
	defer func() { _ = conn.Close() }()

	type copyResult struct {
		input bool
		err   error
	}
	done := make(chan copyResult, 2)
	go func() {
		_, err := io.Copy(conn, in)
		if unix, ok := conn.(*net.UnixConn); ok {
			_ = unix.CloseWrite()
		}
		done <- copyResult{input: true, err: err}
	}()
	go func() {
		_, err := io.Copy(out, conn)
		done <- copyResult{err: err}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case first := <-done:
		if first.err != nil {
			return fmt.Errorf("proxy session: %w", first.err)
		}
		if !first.input {
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case second := <-done:
		if second.err != nil {
			return fmt.Errorf("proxy session: %w", second.err)
		}
		return nil
	}
}
