package syncservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/rpc"
)

// Response is the typed sync envelope: an rpc response whose result is kept as raw
// JSON bytes rather than decoded into any, so the registry's int64 CRDT stamps never
// pass through float64.
type Response = synctransport.Response

// Transport carries one typed request to a sync service and returns its raw-result
// [Response]. Public callers use a resident Unix socket; Synckit's daemon alone owns
// spawned and remote transport construction.
type Transport = synctransport.Transport

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

// Export asks the source service for one immutable full or delta change.
func (c *Client) Export(ctx context.Context, request ExportRequest) (ChangeEnvelope, error) {
	if err := request.Validate(); err != nil {
		return ChangeEnvelope{}, err
	}
	params, err := structParams(request)
	if err != nil {
		return ChangeEnvelope{}, err
	}
	var out ChangeEnvelope
	err = c.call(ctx, &rpc.Request{Method: MethodExport, Params: params}, &out)
	if err == nil {
		err = out.Validate(false)
	}
	return out, err
}

// Apply delivers one immutable source change and returns its exact acknowledgement.
func (c *Client) Apply(ctx context.Context, change ChangeEnvelope) (ApplyResult, error) {
	if err := change.Validate(true); err != nil {
		return ApplyResult{}, err
	}
	params, err := structParams(change)
	if err != nil {
		return ApplyResult{}, err
	}
	var out ApplyResult
	err = c.call(ctx, &rpc.Request{Method: MethodApply, Params: params}, &out)
	return out, err
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

func structParams(value any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	return params, nil
}
