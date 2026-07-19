package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/daemonkit/wire"
)

// TransportError reports daemonkit's proven delivery outcome for a failed call.
type TransportError struct {
	Outcome wire.Outcome
	Err     error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("rpc transport %s: %v", e.Outcome, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// ClientConfig configures one reconnectable persistent synckit RPC client.
type ClientConfig struct {
	Dial  wire.Dialer
	Build string
}

// Client owns at most one persistent daemonkit session. A failed call retires its
// session; only a later operation may establish another one.
type Client struct {
	config ClientConfig

	mu      sync.Mutex
	session *wire.Client
	closed  bool
}

// NewClient returns a lazy persistent client with an exact build identity.
func NewClient(config ClientConfig) *Client {
	if config.Dial == nil || config.Build == "" {
		panic("rpc: Dial and Build are required")
	}
	return &Client{config: config}
}

// Call sends req once. It never replays a request whose delivery is uncertain.
func (c *Client) Call(ctx context.Context, req *Request) (*Response, error) {
	payload, err := EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	session, err := c.current(ctx)
	if err != nil {
		return nil, &TransportError{Outcome: wire.PreSendFailure, Err: err}
	}
	result, err := session.Call(ctx, callOp, "", payload)
	if err != nil {
		c.retire(session)
		return nil, &TransportError{Outcome: result.Outcome, Err: err}
	}
	if result.Outcome != wire.Delivered {
		c.retire(session)
		reason := result.Response.Reason
		if reason == "" {
			reason = result.Response.Err
		}
		return nil, &TransportError{Outcome: result.Outcome, Err: errors.New(reason)}
	}
	if result.Response.Err != "" {
		return nil, errors.New(result.Response.Err)
	}
	resp, err := DecodeResponse(result.Response.Payload)
	if err != nil {
		c.retire(session)
		return nil, &TransportError{Outcome: wire.Delivered, Err: err}
	}
	return resp, nil
}

func (c *Client) current(ctx context.Context) (*wire.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("rpc: client closed")
	}
	if c.session != nil {
		return c.session, nil
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:     c.config.Dial,
		Build:    c.config.Build,
		MaxFrame: MaxFrame,
	})
	if err != nil {
		return nil, err
	}
	c.session = session
	return session, nil
}

func (c *Client) retire(session *wire.Client) {
	c.mu.Lock()
	if c.session == session {
		c.session = nil
	}
	c.mu.Unlock()
	_ = session.Close()
}

// Close closes the persistent session and permanently rejects later calls.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	session := c.session
	c.session = nil
	c.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close()
}
