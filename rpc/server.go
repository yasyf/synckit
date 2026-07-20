package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

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

// Server maps one Dispatcher onto exact persistent daemonkit sessions.
type Server struct {
	Dispatcher *Dispatcher
	Wire       *wire.Server
}

// NewServer returns a server with synckit's exact RPC schema identity.
func NewServer(dispatcher *Dispatcher) *Server {
	if dispatcher == nil {
		panic("rpc: dispatcher is required")
	}
	server := &Server{Dispatcher: dispatcher}
	server.Wire = &wire.Server{Build: Build, MaxFrame: MaxFrame}
	server.Wire.RegisterConcurrent(callOp, server.dispatch)
	return server
}

// Serve accepts authenticated v4 sessions until ctx is canceled and publishes
// readiness only after the listener and worker pool are live.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	return s.Wire.Serve(ctx, listener, func() error { return nil }, allow, allow)
}

// ServeSession serves one spawned-parent session over independent streams.
// Daemonkit owns framing, identity proof, deadlines, cancellation, and closure.
func (s *Server) ServeSession(ctx context.Context, reader io.ReadCloser, writer io.WriteCloser) error {
	conn, err := wire.NewDuplexConn(reader, writer)
	if err != nil {
		return err
	}
	identity, err := wire.SpawnedParentSessionIdentity()
	if err != nil {
		_ = conn.Close()
		return err
	}
	return s.Wire.ServeSession(ctx, conn, identity, func() error { return nil }, allow, allow)
}

func (s *Server) dispatch(ctx context.Context, request wire.Request) (any, error) {
	req, err := DecodeRequest(request.Payload)
	if err != nil {
		payload, encodeErr := EncodeResponse(&Response{OK: false, Error: err.Error()})
		return json.RawMessage(payload), encodeErr
	}
	peer := request.Session.Peer()
	ctx = context.WithValue(ctx, peerPIDKey{}, peer.PID)
	if sid, err := sidOf(peer.PID); err == nil {
		ctx = context.WithValue(ctx, peerSIDKey{}, sid)
	}
	payload, err := EncodeResponse(s.Dispatcher.Dispatch(ctx, req))
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return json.RawMessage(payload), nil
}

func allow() (func(), error) { return func() {}, nil }
