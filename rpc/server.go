package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"
)

type peerPIDKey struct{}

type peerSIDKey struct{}

// PeerPID returns the authenticated persistent session's process ID.
func PeerPID(ctx context.Context) (int, bool) {
	pid, ok := ctx.Value(peerPIDKey{}).(int)
	return pid, ok
}

// PeerSID returns the session ID derived from the authenticated peer process.
func PeerSID(ctx context.Context) (int, bool) {
	sid, ok := ctx.Value(peerSIDKey{}).(int)
	return sid, ok
}

// Listen binds a raw Unix listener. Long-lived daemons delegate listener
// ownership and takeover to daemonkit Runtime.
func Listen(ctx context.Context, sockPath string) (net.Listener, error) {
	var config net.ListenConfig
	return config.Listen(ctx, "unix", sockPath)
}

// Server resolves one admitted Dispatcher for each persistent request.
type Server struct {
	resolve DispatcherResolver
	Wire    *wire.Server
}

// DispatcherResolver resolves the graph already admitted for one wire request.
type DispatcherResolver func(dkdaemon.Publication) (*Dispatcher, error)

// NewServer returns a server with synckit's exact RPC schema identity.
func NewServer(resolve DispatcherResolver) *Server {
	if resolve == nil {
		panic("rpc: dispatcher resolver is required")
	}
	server := &Server{resolve: resolve}
	server.Wire = &wire.Server{WireBuild: WireBuild, MaxFrame: MaxFrame}
	server.Wire.Register(wire.HandlerSpec{Op: callOp, Handler: server.dispatch, Concurrent: true})
	return server
}

func (s *Server) dispatch(ctx context.Context, request wire.Request) (any, error) {
	req, err := DecodeRequest(request.Payload)
	if err != nil {
		payload, encodeErr := EncodeResponse(&Response{OK: false, Error: err.Error()})
		return json.RawMessage(payload), encodeErr
	}
	dispatcher, err := s.resolve(request.Publication)
	if err != nil {
		return nil, err
	}
	peer := request.Peer
	ctx = context.WithValue(ctx, peerPIDKey{}, peer.PID)
	if sid, err := sidOf(peer.PID); err == nil {
		ctx = context.WithValue(ctx, peerSIDKey{}, sid)
	}
	payload, err := EncodeResponse(dispatcher.Dispatch(ctx, req))
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return json.RawMessage(payload), nil
}
