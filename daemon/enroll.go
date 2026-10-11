package daemon

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func enrollHost(ctx context.Context, scope processScope, manifests []manifest.Manifest, target, self string, step func(string)) {
	for _, m := range manifests {
		enrolled, err := enrollConsumer(ctx, scope, m, target, self)
		switch {
		case err != nil:
			step(fmt.Sprintf("WARN enroll %s identity of %s: %v", m.Name, target, err))
		case enrolled:
			step("enrolled " + m.Name + " identity of " + target)
		}
	}
}

func enrollConsumer(ctx context.Context, scope processScope, m manifest.Manifest, target, self string) (bool, error) {
	local := syncservice.NewClient(dialTransport(scope, m, self, self))
	defer func() { _ = local.Close() }()
	caps, err := local.Capabilities(ctx)
	if err != nil {
		return false, err
	}
	if !slices.Contains(caps.Methods, syncservice.MethodEnroll) {
		return false, nil
	}
	remote := syncservice.NewClient(dialTransport(scope, m, target, self))
	defer func() { _ = remote.Close() }()
	identity, err := peerIdentity(ctx, remote, target)
	if err != nil {
		return false, err
	}
	return true, local.Enroll(ctx, syncservice.EnrollRequest{Host: target, Identity: identity})
}

func peerIdentity(ctx context.Context, remote *syncservice.Client, target string) ([]byte, error) {
	attempts := 1
	if fact, err := hostregistry.Mesh.Host(target); err == nil {
		attempts = max(attempts, len(fact.Addresses))
	}
	var transportErr *rpc.TransportError
	for {
		identity, err := remote.Identity(ctx)
		attempts--
		if err == nil || attempts == 0 || !errors.As(err, &transportErr) || !transportErr.Undispatched {
			return identity, err
		}
	}
}
