package delivery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/internal/servespec"
	"github.com/yasyf/synckit/rpc"
)

// Status asks the resident synckitd for every PeerStatus of serviceID, or of
// every service when serviceID is empty.
func Status(ctx context.Context, serviceID string) ([]PeerStatus, error) {
	var out []PeerStatus
	err := call(ctx, MethodStatus, map[string]any{"service_id": serviceID}, &out)
	return out, err
}

// Kick asks the resident synckitd to run one delivery of serviceID to peer.
// An empty serviceID kicks every service and an empty peer every mesh host.
// It fails when the daemon is not running.
func Kick(ctx context.Context, serviceID, peer string) error {
	return call(ctx, MethodKick, map[string]any{"service_id": serviceID, "peer": peer}, nil)
}

func call(ctx context.Context, method string, params map[string]any, out any) error {
	client, err := servespec.Dial()
	if err != nil {
		return fmt.Errorf("dial synckitd: %w", err)
	}
	defer func() { _ = client.Close() }()
	resp, err := client.Call(ctx, &rpc.Request{Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if !resp.OK {
		return fmt.Errorf("%s: %s", method, resp.Error)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(resp.Result, out); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}
	return nil
}
