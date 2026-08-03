package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
)

// laneCloseGrace bounds releasing one retired or closed business lane; every
// daemonkit verb refuses a context without a deadline, and a lane being dropped
// has no caller deadline left to borrow.
const laneCloseGrace = 5 * time.Second

// callBudget is the deadline a Call rides when its caller states none. The
// deadline is not optional — daemonkit refuses an undeadlined Call, and the one
// it is given rides the wire into the handler's ctx — so synckit states the
// bound its own server side already enforces: DispatchTimeout caps the handler,
// and the client outwaits it by a minute rather than abandoning a request the
// daemon is still working. A caller with a tighter deadline keeps it.
const callBudget = DispatchTimeout + time.Minute

// TransportError reports a failed call and whether daemonkit proved it never
// reached dispatch. Undispatched is a safety predicate: true guarantees a safe
// resend, false means unknown — never "dispatched".
type TransportError struct {
	Undispatched bool
	Err          error
}

func (e *TransportError) Error() string {
	if e.Undispatched {
		return fmt.Sprintf("rpc transport (undispatched): %v", e.Err)
	}
	return fmt.Sprintf("rpc transport: %v", e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// Lane supplies one fresh business lane to the peer a Client speaks to. The
// three suppliers name their own authentication: daemonkit.Client.Business
// (kernel-verified against Trust.Serving), daemonkit.Child.Business
// (directional confinement over a spawned socketpair), and
// daemonkit.BusinessOverConn (the caller authenticated the transport).
type Lane func(context.Context) (*daemonkit.Business, error)

// ClientConfig configures one reconnectable persistent synckit RPC client.
type ClientConfig struct {
	Open Lane
}

// Client owns at most one persistent business lane. A failed call retires its
// lane; only a later operation may ask the supplier for another one.
type Client struct {
	open Lane

	mu     sync.Mutex
	lane   *daemonkit.Business
	closed bool
}

// NewClient returns a lazy persistent client over config's lane supplier.
func NewClient(config ClientConfig) *Client {
	if config.Open == nil {
		panic("rpc: Open is required")
	}
	return &Client{open: config.Open}
}

// Call sends req once. It never replays a request whose delivery is uncertain.
func (c *Client) Call(ctx context.Context, req *Request) (*Response, error) {
	payload, err := EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	if _, stated := ctx.Deadline(); !stated {
		bounded, cancel := context.WithTimeout(ctx, callBudget)
		defer cancel()
		ctx = bounded
	}
	lane, err := c.current(ctx)
	if err != nil {
		return nil, &TransportError{Undispatched: true, Err: err}
	}
	result, err := lane.Call(ctx, callOp, payload)
	if err != nil {
		c.retire(ctx, lane)
		return nil, &TransportError{Undispatched: daemonkit.Undispatched(err), Err: err}
	}
	resp, err := DecodeResponse(result.Body)
	if err != nil {
		c.retire(ctx, lane)
		return nil, &TransportError{Err: err}
	}
	return resp, nil
}

func (c *Client) current(ctx context.Context) (*daemonkit.Business, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("rpc: client closed")
	}
	if c.lane != nil {
		return c.lane, nil
	}
	lane, err := c.open(ctx)
	if err != nil {
		return nil, err
	}
	c.lane = lane
	return lane, nil
}

func (c *Client) retire(parent context.Context, lane *daemonkit.Business) {
	c.mu.Lock()
	if c.lane == lane {
		c.lane = nil
	}
	c.mu.Unlock()
	closeLane(parent, lane)
}

// Close releases the persistent lane and permanently rejects later calls.
//
//nolint:contextcheck // Transport.Close carries no context and daemonkit refuses an undeadlined release, so the grace is minted here.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	lane := c.lane
	c.lane = nil
	c.mu.Unlock()
	if lane == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), laneCloseGrace)
	defer cancel()
	return lane.Close(ctx)
}

func closeLane(parent context.Context, lane *daemonkit.Business) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), laneCloseGrace)
	defer cancel()
	_ = lane.Close(ctx)
}
