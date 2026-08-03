package rpc

import (
	"context"
	"errors"

	"github.com/yasyf/daemonkit"
)

// Contract is the session both ends of a synckit business lane speak. Every
// spawn conveys these limits and both ends adopt them, so a parent and its
// child can never skew on frame size.
func Contract() daemonkit.Contract {
	return daemonkit.Contract{Schema: WireBuild, MaxFrame: MaxFrame}
}

// SpawnLimits is what a parent conveys to a child spawned on the handoff
// socketpair; it states the same frame Contract does.
func SpawnLimits() daemonkit.Limits {
	return daemonkit.Limits{MaxFrame: MaxFrame}
}

// ServeSpawned claims the handoff descriptor the spawning parent placed at fd 3
// and serves exactly one synckit session on it off dispatcher. It is the child
// half of a sealed local service: no flock, no owner record, no launchd job.
func ServeSpawned(ctx context.Context, dispatcher *Dispatcher) error {
	if dispatcher == nil {
		return errors.New("rpc: dispatcher is required")
	}
	return daemonkit.ServeSpawned(ctx, Contract(), dispatcher.Handle)
}
