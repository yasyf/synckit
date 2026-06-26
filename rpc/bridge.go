package rpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
)

// Proxy bridges newline-delimited rpc frames from in (stdin) to the resident unix
// socket at sockPath and writes each response frame back to out (stdout). It reads
// one request line, forwards the raw bytes to the socket via CallRaw, and writes the
// raw response line plus a single '\n', looping until in hits io.EOF (returns nil).
// The payload is never JSON-decoded, so a get_state response's int64 stamps survive
// byte-exact. A CallRaw failure (a transient socket error) is reported as an error
// response line and the loop continues, so one bad round trip does not kill the
// bridge.
func Proxy(ctx context.Context, in io.Reader, out io.Writer, sockPath string) error {
	r := bufio.NewReader(in)
	for {
		line, err := ReadLine(r, MaxLine)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read request: %w", err)
		}

		resp, err := CallRaw(ctx, sockPath, line)
		if err != nil {
			writeResponse(out, &Response{OK: false, Error: err.Error()})
			continue
		}
		if _, err := out.Write(resp); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
		if _, err := out.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
	}
}
