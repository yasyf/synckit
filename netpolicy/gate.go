package netpolicy

import (
	"context"
	"fmt"
	"time"
)

// RemoteFunc fetches the peer's current State over the policy control channel.
type RemoteFunc func(ctx context.Context) (State, error)

// PausedError reports that the cost policy paused bulk transfer; Reason is the
// blocking Verdict's Reason.
type PausedError struct {
	Reason string
}

func (e *PausedError) Error() string {
	return "netpolicy: bulk transfer paused: " + e.Reason
}

// Gate enforces the cost policy at the chunk boundaries of a bulk transfer
// loop, with no override: a busy or urgent transfer pauses like any other. An
// unmarked cellular backhaul (a hotspot the OS flags neither cellular nor
// expensive) passes as unrestricted, and an in-flight chunk (≤1 MiB) finishes.
type Gate struct {
	monitor Monitor
	poll    time.Duration
}

// NewGate returns a Gate over monitor whose Wait re-polls the remote State
// every poll.
func NewGate(monitor Monitor, poll time.Duration) *Gate {
	return &Gate{monitor: monitor, poll: poll}
}

// Check runs before each chunk: it returns ctx's error once cancelled, a
// *PausedError when the policy blocks local against remote, and nil otherwise.
func (g *Gate) Check(ctx context.Context, remote State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	local, _ := g.monitor.Current()
	if v := Evaluate(local, remote); !v.Allowed {
		return &PausedError{Reason: v.Reason}
	}
	return nil
}

// Wait blocks until the policy allows bulk transfer, re-evaluating on every
// local path change, manual setting edit, and poll, and returns the remote
// State that allowed it. A remote fetch error ends the wait.
func (g *Gate) Wait(ctx context.Context, remote RemoteFunc) (State, error) {
	ticker := time.NewTicker(g.poll)
	defer ticker.Stop()
	for {
		local, changed := g.monitor.Current()
		peer, err := remote(ctx)
		if err != nil {
			return State{}, fmt.Errorf("fetch remote network state: %w", err)
		}
		if Evaluate(local, peer).Allowed {
			return peer, nil
		}
		select {
		case <-ctx.Done():
			return State{}, ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}
