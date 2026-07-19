package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/yasyf/daemonkit/proc"
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

// Listen binds sockPath under daemonkit's single-entrant listener ownership.
func Listen(ctx context.Context, sockPath string) (net.Listener, error) {
	se := proc.SingleEntrant{
		Socket: sockPath,
		Evict:  func() (bool, error) { return false, nil },
	}
	ln, lock, err := se.Listen(ctx)
	if err != nil {
		return nil, err
	}
	return &lockedListener{Listener: ln, lock: lock}, nil
}

type lockedListener struct {
	net.Listener
	lock *os.File
}

func (l *lockedListener) Close() error {
	err := l.Listener.Close()
	_ = l.lock.Close()
	return err
}

// Server maps one Dispatcher onto exact persistent daemonkit sessions.
type Server struct {
	Dispatcher *Dispatcher
	Build      string
	Trust      func(wire.Peer) error
}

// NewServer returns a server with synckit's exact RPC schema identity.
func NewServer(dispatcher *Dispatcher) *Server {
	if dispatcher == nil {
		panic("rpc: dispatcher is required")
	}
	return &Server{Dispatcher: dispatcher, Build: Build}
}

// Serve accepts authenticated v4 sessions until ctx is canceled.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	server := &wire.Server{
		Build:    s.Build,
		Trust:    s.Trust,
		MaxFrame: MaxFrame,
	}
	server.RegisterConcurrent(callOp, s.dispatch)
	return server.Serve(ctx, listener, s.Dispatcher.admission(), allow)
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
