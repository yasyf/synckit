// Package rpc maps synckit's method registry onto daemonkit's exact persistent wire.
package rpc

import (
	"encoding/json"
	"fmt"
	"time"
)

// DispatchTimeout caps how long a single dispatched handler may run. The handler ctx
// inherits the daemon's lifetime, which has no deadline, so without this a handler
// that blocks (e.g. on a cross-process flock) could wait forever.
const DispatchTimeout = 10 * time.Minute

const (
	// MaxPayload bounds one encoded synckit request or response body.
	MaxPayload = 16 << 20
	// MaxFrame is the smallest daemonkit frame whose payload ceiling still carries
	// MaxPayload: a terminal base64s its body and reserves 4 KiB of envelope, so a
	// frame sized at the payload itself would cap bodies at three quarters of it.
	// Every Contract.MaxFrame on a synckit session states this same number —
	// daemonkit refuses a contract that disagrees with what a spawn conveyed.
	MaxFrame = (MaxPayload*4+2)/3 + 4<<10
	callOp   = "synckit.rpc.call"
)

// Request is one RPC command: a method name and an arbitrary params object.
type Request struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

// Response is the daemon's reply to one Request. Result stays raw so integer CRDT
// stamps never pass through float64.
type Response struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error,omitempty"`
}

// EncodeRequest renders req as a daemonkit payload.
func EncodeRequest(req *Request) ([]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	return data, nil
}

// DecodeRequest parses one daemonkit payload into a Request.
func DecodeRequest(payload []byte) (*Request, error) {
	var req Request
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decode request: %w", err)
	}
	return &req, nil
}

// EncodeResponse renders resp as a daemonkit payload.
func EncodeResponse(resp *Response) ([]byte, error) {
	data, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return data, nil
}

// DecodeResponse parses one daemonkit payload into a Response.
func DecodeResponse(payload []byte) (*Response, error) {
	var resp Response
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &resp, nil
}
