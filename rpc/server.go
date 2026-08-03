package rpc

import (
	"context"
	"fmt"
	"net"

	"github.com/yasyf/daemonkit"
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
// ownership and takeover to daemonkit's Serve.
func Listen(ctx context.Context, sockPath string) (net.Listener, error) {
	var config net.ListenConfig
	return config.Listen(ctx, "unix", sockPath)
}

// Handle serves one admitted daemonkit business request off d's registry, so a
// daemon product and a spawned child session put the same bytes on the wire.
// An undecodable payload is an error Response rather than a session failure;
// only an unroutable op or an unencodable response fails the session.
func (d *Dispatcher) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	if request.Op != callOp {
		return daemonkit.Reply{}, fmt.Errorf("rpc: op %q is not %q", request.Op, callOp)
	}
	decoded, err := DecodeRequest(request.Body)
	if err != nil {
		return reply(&Response{OK: false, Error: err.Error()})
	}
	ctx = context.WithValue(ctx, peerPIDKey{}, request.Caller.PID)
	if sid, err := sidOf(request.Caller.PID); err == nil {
		ctx = context.WithValue(ctx, peerSIDKey{}, sid)
	}
	return reply(d.Dispatch(ctx, decoded))
}

func reply(response *Response) (daemonkit.Reply, error) {
	payload, err := EncodeResponse(response)
	if err != nil {
		return daemonkit.Reply{}, err
	}
	return daemonkit.Reply{Body: payload}, nil
}
