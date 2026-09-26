// Package rpc maps synckit's method registry onto daemonkit's exact persistent wire.
package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

	unknownMethodPrefix = "unknown method "
)

// ErrUnknownMethod reports that the peer daemon predates the called method: its
// dispatcher registers no handler under that name. Every synckitd since the
// dispatcher first shipped sends the same reply for it, so a caller can tell a
// daemon that needs upgrading from one whose handler failed.
var ErrUnknownMethod = errors.New("unknown method")

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

// ReplyError turns the Error of a failed Response into an error. The reply a
// dispatcher sends for a method it lacks wraps ErrUnknownMethod and keeps the
// method name; every other reply is an opaque remote failure.
func ReplyError(message string) error {
	if method, ok := strings.CutPrefix(message, unknownMethodPrefix); ok {
		if _, err := strconv.Unquote(method); err == nil {
			return fmt.Errorf("%w %s", ErrUnknownMethod, method)
		}
	}
	return errors.New(message)
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
