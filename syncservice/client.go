package syncservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/rpc"
)

// Response is the typed sync envelope: an rpc response whose result is kept as raw
// JSON bytes rather than decoded into any, so the registry's int64 CRDT stamps never
// pass through float64.
type Response struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error,omitempty"`
}

// Transport carries one typed request to a sync service and returns its raw-result
// [Response]. Implementations reach the service over a unix socket, a spawned child's
// stdio, or ssh to a peer's rpc-serve bridge.
type Transport interface {
	// Do sends req and returns the service's response.
	Do(ctx context.Context, req *rpc.Request) (*Response, error)
	// Close releases any resources the transport holds (a spawned child, say).
	Close() error
}

// Client is the typed caller for the sync contract. It drives a [Transport] and
// decodes each method's result into its Go type.
type Client struct {
	tx Transport
}

// NewClient returns a Client that issues requests over tx.
func NewClient(tx Transport) *Client {
	return &Client{tx: tx}
}

// Close releases the underlying transport.
func (c *Client) Close() error {
	return c.tx.Close()
}

// Call invokes an RPC method outside the built-in sync contract. Non-OK
// responses become errors naming method; non-null results are JSON-decoded into
// out. A nil out discards the result.
func (c *Client) Call(ctx context.Context, method string, params map[string]any, out any) error {
	return c.call(ctx, &rpc.Request{Method: method, Params: params}, out)
}

// Capabilities asks the peer for its name and methods.
func (c *Client) Capabilities(ctx context.Context) (Capabilities, error) {
	var out Capabilities
	err := c.call(ctx, &rpc.Request{Method: MethodCapabilities}, &out)
	return out, err
}

// List asks the peer for the items it tracks for sync.
func (c *Client) List(ctx context.Context) ([]WatchItem, error) {
	var out []WatchItem
	err := c.call(ctx, &rpc.Request{Method: MethodList}, &out)
	return out, err
}

// Reconcile asks the peer to converge against origin and returns the result.
func (c *Client) Reconcile(ctx context.Context, origin string) (ReconcileResult, error) {
	var out ReconcileResult
	err := c.call(ctx, &rpc.Request{Method: MethodReconcile, Params: originParams(origin)}, &out)
	return out, err
}

// Sync asks the peer to run a full sync against origin and returns the result.
func (c *Client) Sync(ctx context.Context, origin string) (SyncResult, error) {
	var out SyncResult
	err := c.call(ctx, &rpc.Request{Method: MethodSync, Params: originParams(origin)}, &out)
	return out, err
}

// GetState asks the peer for its registry state and returns it as opaque JSON. It
// bypasses the typed decode so the registry's int64 CRDT stamps survive byte-exact.
func (c *Client) GetState(ctx context.Context) (RawRegistry, error) {
	resp, err := c.tx.Do(ctx, &rpc.Request{Method: MethodGetState})
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("%s: %s", MethodGetState, resp.Error)
	}
	return RawRegistry(resp.Result), nil
}

// call issues req over the transport and, when out is non-nil and the result is a
// non-null JSON value, decodes the raw result into out. A non-OK response becomes an
// error naming the method.
func (c *Client) call(ctx context.Context, req *rpc.Request, out any) error {
	resp, err := c.tx.Do(ctx, req)
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("%s: %s", req.Method, resp.Error)
	}
	if out != nil && len(resp.Result) > 0 && string(resp.Result) != "null" {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			return fmt.Errorf("decode %s result: %w", req.Method, err)
		}
	}
	return nil
}

// originParams returns the params map carrying origin, or nil when origin is empty so
// the request omits the key entirely.
func originParams(origin string) map[string]any {
	if origin == "" {
		return nil
	}
	return map[string]any{"origin": origin}
}
