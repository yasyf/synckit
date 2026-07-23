// Package runtimehealth adapts daemonkit health to Synckit's wire route.
package runtimehealth

import (
	"context"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/rpc"
)

// Observation exposes product runtime health on Synckit's fixed wire route.
func Observation(health func(context.Context) (dkdaemon.Health, error)) wire.ObservationRoute {
	return wire.ObservationRoute{
		Op: rpc.RuntimeHealthOp, AvailableBeforeReady: true,
		Handler: func(ctx context.Context, _ wire.ObservationRequest) (wire.ObservationResponse, error) {
			value, err := health(ctx)
			if err != nil {
				return wire.ObservationResponse{}, err
			}
			payload, err := rpc.EncodeRuntimeHealth(rpc.RuntimeHealth{
				RuntimeBuild: value.RuntimeBuild, RuntimeProtocol: value.RuntimeProtocol,
				ProcessGeneration: value.ProcessGeneration, PID: value.PID, State: string(value.State),
				Draining: value.Draining, Busy: value.Busy, Ready: value.Ready,
			})
			if err != nil {
				return wire.ObservationResponse{}, err
			}
			return wire.ObservationResponse{Payload: payload}, nil
		},
	}
}
