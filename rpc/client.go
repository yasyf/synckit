package rpc

import (
	"bufio"
	"context"
	"encoding/json"
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

// CallRaw sends one raw request line to the daemon at sockPath and returns its raw
// response line. It dials the unix socket, writes reqLine followed by a single '\n',
// reads one response line bounded by MaxLine, and closes, returning the line without
// the trailing '\n'. A missing or unreachable socket, and a socket that sends no
// response line, are both reported as a *TransportError. The payload is never
// decoded, so int64 stamps in the request or response survive byte-exact.
func CallRaw(ctx context.Context, sockPath string, reqLine []byte) ([]byte, error) {
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

	if _, err := conn.Write(reqLine); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	if _, err := conn.Write([]byte{'\n'}); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	line, err := ReadLine(bufio.NewReader(conn), MaxLine)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("read response from %s: %w", sockPath, err)}
	}
	return line, nil
}

// Call sends one Request to the daemon at sockPath and returns its Response. It dials
// the unix socket, writes one request line, reads one response line bounded by
// MaxLine, and closes. A missing or unreachable socket is reported as a
// *TransportError.
func Call(ctx context.Context, sockPath string, req *Request) (*Response, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	line, err := CallRaw(ctx, sockPath, data)
	if err != nil {
		return nil, err
	}
	return DecodeResponse(line)
}
