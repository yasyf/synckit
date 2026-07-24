package rpc

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

var spawnedSessionLimits = wire.SessionLimits{
	Workers: 8, Backlog: 32, MaxFrame: MaxFrame,
	InboundQueue: 64, OutboundQueue: 128, StreamQueue: 16, EventQueue: 16,
	HandshakeTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
	CancelSettlementTimeout: 10 * time.Second,
}

// SpawnedClient is the typed Synckit client over one sealed daemonkit child session.
type SpawnedClient struct{ wire *wire.SpawnedClient }

// NewSpawnedClient consumes one receipt-bound child endpoint.
func NewSpawnedClient(ctx context.Context, endpoint proc.SpawnedSessionEndpoint) (*SpawnedClient, error) {
	client, err := wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: WireBuild, Limits: spawnedSessionLimits,
	})
	if err != nil {
		return nil, err
	}
	return &SpawnedClient{wire: client}, nil
}

// Call sends one request exactly once.
func (c *SpawnedClient) Call(ctx context.Context, request *Request) (*Response, error) {
	if c == nil || c.wire == nil {
		return nil, errors.New("rpc: spawned client is required")
	}
	payload, err := EncodeRequest(request)
	if err != nil {
		return nil, err
	}
	result, err := c.wire.Call(ctx, callOp, "", payload)
	if err != nil {
		return nil, &TransportError{Outcome: result.Outcome, Err: err}
	}
	if result.Outcome != wire.Delivered {
		reason := result.Response.Reason
		if reason == "" {
			reason = result.Response.Err
		}
		return nil, &TransportError{Outcome: result.Outcome, Err: errors.New(reason)}
	}
	if result.Response.Err != "" {
		return nil, errors.New(result.Response.Err)
	}
	response, err := DecodeResponse(result.Response.Payload)
	if err != nil {
		return nil, &TransportError{Outcome: wire.Delivered, Err: err}
	}
	return response, nil
}

// Close settles the sealed wire session.
func (c *SpawnedClient) Close() error {
	if c == nil || c.wire == nil {
		return nil
	}
	return c.wire.Close()
}

// RunSpawnedServer claims and serves the one inherited child session.
func RunSpawnedServer(ctx context.Context, identity proc.SpawnedSessionIdentity, dispatcher *Dispatcher) error {
	if dispatcher == nil {
		return errors.New("rpc: dispatcher is required")
	}
	server := NewServer(dispatcher)
	return wire.RunSpawnedSession(ctx, wire.SpawnedSessionConfig{
		Identity: identity, WireBuild: WireBuild, Limits: spawnedSessionLimits,
		Handlers: []wire.HandlerSpec{{Op: callOp, Handler: server.dispatch, Concurrent: true}},
	})
}
