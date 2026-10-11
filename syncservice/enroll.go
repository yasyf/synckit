package syncservice

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/yasyf/synckit/rpc"
)

const (
	// MethodIdentity reports the consumer's own identity for a joining peer to pin.
	MethodIdentity = "synckit.syncservice.identity.v1"
	// MethodEnroll pins a joining mesh host's identity in the local consumer.
	MethodEnroll = "synckit.syncservice.enroll.v1"
)

// EnrollMethods lists the methods an [Enroller] adds to its [Capabilities].
var EnrollMethods = []string{MethodIdentity, MethodEnroll}

// MaxIdentity is the largest identity document an enrollment carries.
const MaxIdentity = 64 << 10

// EnrollRequest carries the identity document Host's consumer reported when
// Host joined the mesh.
type EnrollRequest struct {
	Host     string `json:"host"`
	Identity []byte `json:"identity"`
}

// Validate checks the host name and that Identity is one bounded JSON value.
func (r EnrollRequest) Validate() error {
	if r.Host == "" || strings.ContainsAny(r.Host, "\x00\r\n") {
		return errors.New("syncservice: enroll host is invalid")
	}
	return validIdentity(r.Identity)
}

func validIdentity(identity []byte) error {
	if len(identity) == 0 || len(identity) > MaxIdentity || !json.Valid(identity) {
		return errors.New("syncservice: identity must be bounded valid JSON")
	}
	return nil
}

// Enroller is a SyncConsumer that pins each joining host's identity, read by
// `synckitd host add` from the peer's consumer, to authenticate its changes.
type Enroller interface {
	SyncConsumer
	// Identity returns this consumer's JSON identity document.
	Identity(ctx context.Context) ([]byte, error)
	// Enroll pins request.Identity as the identity of request.Host.
	Enroll(ctx context.Context, request EnrollRequest) error
}

func registerEnroller(d *rpc.Dispatcher, svc Enroller) {
	d.Register(MethodIdentity, func(ctx context.Context, _ map[string]any) (any, error) {
		identity, err := svc.Identity(ctx)
		if err != nil {
			return nil, err
		}
		return identity, validIdentity(identity)
	})
	d.RegisterExclusive(MethodEnroll, func(ctx context.Context, p map[string]any) (any, error) {
		var request EnrollRequest
		if err := decodeParams(p, &request); err != nil {
			return nil, err
		}
		if err := request.Validate(); err != nil {
			return nil, err
		}
		return nil, svc.Enroll(ctx, request)
	})
}

// Identity asks the consumer for its identity document.
func (c *Client) Identity(ctx context.Context) ([]byte, error) {
	var out []byte
	if err := c.call(ctx, &rpc.Request{Method: MethodIdentity}, &out); err != nil {
		return nil, err
	}
	return out, validIdentity(out)
}

// Enroll asks the consumer to pin request.Identity for request.Host.
func (c *Client) Enroll(ctx context.Context, request EnrollRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	params, err := structParams(request)
	if err != nil {
		return err
	}
	return c.call(ctx, &rpc.Request{Method: MethodEnroll, Params: params}, nil)
}
