package daemon

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/rpc"
)

func registerDelivery(d *rpc.Dispatcher, sup *supervisor) {
	d.Register(delivery.MethodStatus, func(ctx context.Context, p map[string]any) (any, error) {
		var params delivery.StatusParams
		if err := decodeStruct(p, &params); err != nil {
			return nil, err
		}
		scheduler, err := sup.deliveries()
		if err != nil {
			return nil, err
		}
		return scheduler.status(ctx, params.ServiceID)
	})
	d.Register(delivery.MethodKick, func(_ context.Context, p map[string]any) (any, error) {
		var params delivery.KickParams
		if err := decodeStruct(p, &params); err != nil {
			return nil, err
		}
		scheduler, err := sup.deliveries()
		if err != nil {
			return nil, err
		}
		return map[string]any{"kicked": true}, scheduler.Kick(params.ServiceID, params.Peer)
	})
}

func decodeStruct(params map[string]any, target any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode params: %w", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode params: %w", err)
	}
	return nil
}
