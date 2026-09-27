package netpolicy

import (
	"context"
	"fmt"
	"time"
)

// RemoteFunc fetches the peer's current State over the policy control channel.
type RemoteFunc func(ctx context.Context) (State, error)

const restrictedMidTransfer = "local: restricted mid-transfer"

// PausedError reports that the cost policy paused bulk transfer; Reason is the
// blocking Verdict's Reason, or "local: restricted mid-transfer" when this
// host's State was restricted at some point after Wait admitted the transfer.
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

// Check runs before each chunk of a transfer Wait admitted, with admitted the
// local State Wait returned: it returns ctx's error once cancelled, a
// *PausedError when the policy blocks local against remote or local was
// restricted at any point since admitted, and nil otherwise.
func (g *Gate) Check(ctx context.Context, admitted, remote State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	local, _ := g.monitor.Current()
	if local.Unrestricted() && local.RestrictedEpoch != admitted.RestrictedEpoch {
		return &PausedError{Reason: restrictedMidTransfer}
	}
	if v := Evaluate(local, remote); !v.Allowed {
		return &PausedError{Reason: v.Reason}
	}
	return nil
}

// Wait blocks until the policy allows bulk transfer, re-evaluating on every
// local path change, manual setting edit, and poll, and returns the local and
// remote States that allowed it; the transfer passes local to every Check. A
// remote fetch error ends the wait.
func (g *Gate) Wait(ctx context.Context, remote RemoteFunc) (local, peer State, err error) {
	ticker := time.NewTicker(g.poll)
	defer ticker.Stop()
	for {
		var changed <-chan struct{}
		local, changed = g.monitor.Current()
		if peer, err = remote(ctx); err != nil {
			return State{}, State{}, fmt.Errorf("fetch remote network state: %w", err)
		}
		if Evaluate(local, peer).Allowed {
			return local, peer, nil
		}
		select {
		case <-ctx.Done():
			return State{}, State{}, ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}
