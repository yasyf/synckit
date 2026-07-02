// Package rpc is the generic unix-socket RPC transport shared across synckit tools:
// a {method, params} request line in, a {ok, result, error} response line out, then
// the connection closes. The wire is newline-delimited JSON (one object + '\n' per
// message); a Dispatcher routes method names to registered handlers — concurrent by
// default, serialized only for methods bound via RegisterExclusive; the server
// enforces a same-UID peer check and a 16 MiB per-line bound before doing any work. Method names, param shapes, and result
// types are the consuming tool's domain — this package knows none of them.
package rpc

import (
	"encoding/json"
	"fmt"
	"time"
)

// ReadTimeout bounds how long a connection may take to deliver its request line, so
// a peer that connects and never writes cannot park a handler goroutine.
const ReadTimeout = 30 * time.Second

// DispatchTimeout caps how long a single dispatched handler may run. The handler ctx
// inherits the daemon's lifetime, which has no deadline, so without this a handler
// that blocks (e.g. on a cross-process flock) could wait forever.
const DispatchTimeout = 10 * time.Minute

// MaxLine bounds a single request or response line. A peer that streams bytes
// without a newline is rejected once it crosses this, never buffered unbounded.
const MaxLine = 16 << 20

// Request is one RPC command: a method name and an arbitrary params object, encoded
// as one JSON line.
type Request struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

// Response is the daemon's reply to one Request, encoded as one JSON line. Result is
// always present (null when the handler returned nil); Error is set only when the
// request failed.
type Response struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result"`
	Error  string `json:"error,omitempty"`
}

// EncodeRequest renders req as one newline-terminated JSON line.
func EncodeRequest(req *Request) ([]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	return append(data, '\n'), nil
}

// DecodeRequest parses one JSON line into a Request.
func DecodeRequest(line []byte) (*Request, error) {
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, fmt.Errorf("decode request: %w", err)
	}
	return &req, nil
}

// EncodeResponse renders resp as one newline-terminated JSON line.
func EncodeResponse(resp *Response) ([]byte, error) {
	data, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return append(data, '\n'), nil
}

// DecodeResponse parses one JSON line into a Response.
func DecodeResponse(line []byte) (*Response, error) {
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &resp, nil
}
