package rpc

import (
	"bufio"
	"context"
	"fmt"
	"net"
)

// TransportError marks a transport failure: the daemon socket could not be reached
// or sent no response line. It wraps the underlying cause so callers can both match
// on the type and unwrap to the dial or read error.
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string { return e.Err.Error() }

func (e *TransportError) Unwrap() error { return e.Err }

// Call sends one Request to the daemon at sockPath and returns its Response. It dials
// the unix socket, writes one request line, reads one response line bounded by
// MaxLine, and closes. A missing or unreachable socket is reported as a
// *TransportError.
func Call(ctx context.Context, sockPath string, req *Request) (*Response, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("dial daemon at %s: %w", sockPath, err)}
	}
	defer func() { _ = conn.Close() }()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set deadline: %w", err)
		}
	}

	data, err := EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	line, err := readLine(bufio.NewReader(conn), MaxLine)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("read response from %s: %w", sockPath, err)}
	}
	return DecodeResponse(line)
}
