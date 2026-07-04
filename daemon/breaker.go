package daemon

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/watch"
)

const (
	// breakerInitialCooldown is the first retry delay after a peer goes unreachable.
	breakerInitialCooldown = 30 * time.Second
	// breakerMaxCooldown caps the doubling retry delay, so a down peer is probed at
	// least every five minutes — inside reposync's 15-minute reconcile backstop.
	breakerMaxCooldown = 5 * time.Minute
	// snapshotTimeout bounds a tailscale snapshot so a hung tailscale never wedges
	// the retry goroutine.
	snapshotTimeout = 15 * time.Second
)

// breakerNotifier wraps a manifest's notifier with a per-peer circuit breaker:
// after a failed notify it suppresses further notifies to that peer, retries on a
// doubling cooldown, and logs only the down and recovery transitions. The daemon's
// manifest notifier ignores the item id, so every notify runs a full-manifest sync;
// a successful retry therefore doubles as the missed-changes catch-up.
// Notify always returns nil, so the engine's per-notify error log never fires and
// the breaker owns every notify-path log line for daemon engines.
type breakerNotifier struct {
	inner watch.Notifier[string]
	name  string          // manifest name, for log attribution
	self  string          // the local host takes no tailscale snapshot
	ctx   context.Context // generation context: retries outlive any single event

	mu   sync.Mutex
	open map[string]*openPeer

	now       func() time.Time
	afterFunc func(d time.Duration, fn func())
	snapshot  func(ctx context.Context, peer string)
}

// openPeer records an open breaker: when the outage began and the current retry
// cooldown.
type openPeer struct {
	openedAt time.Time
	cooldown time.Duration
}

// newBreakerNotifier wraps inner with a per-peer breaker for manifest name, taking
// self so the local host is never snapshotted and ctx as the generation context its
// retry timers run under.
func newBreakerNotifier(ctx context.Context, inner watch.Notifier[string], name, self string) *breakerNotifier {
	return &breakerNotifier{
		inner:     inner,
		name:      name,
		self:      self,
		ctx:       ctx,
		open:      make(map[string]*openPeer),
		now:       time.Now,
		afterFunc: func(d time.Duration, fn func()) { time.AfterFunc(d, fn) },
		snapshot:  tailscaleSnapshot,
	}
}

// Notify passes a closed peer through to the inner notifier and opens the breaker on
// its first failure. While open it suppresses notifies — the retry timer owns the
// next attempt. It always returns nil so the engine never logs a notify failure.
func (b *breakerNotifier) Notify(ctx context.Context, peer, id string) error {
	if b.isOpen(peer) {
		return nil
	}
	if err := b.inner.Notify(ctx, peer, id); err != nil {
		b.opened(peer, err)
	}
	return nil
}

func (b *breakerNotifier) isOpen(peer string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.open[peer]
	return ok
}

// opened records the outage, arms the first retry, and logs the single down
// transition under the generation context. It is idempotent under mu: a concurrent
// per-peer fan-out that also failed finds the entry already present and returns
// without logging or re-arming.
func (b *breakerNotifier) opened(peer string, cause error) {
	b.mu.Lock()
	if _, ok := b.open[peer]; ok {
		b.mu.Unlock()
		return
	}
	op := &openPeer{openedAt: b.now(), cooldown: breakerInitialCooldown}
	b.open[peer] = op
	b.afterFunc(op.cooldown, func() { b.retry(peer) })
	b.mu.Unlock()

	slog.WarnContext(b.ctx, "watch: peer unreachable; suppressing notifies until recovery",
		"manifest", b.name, "peer", peer, "err", cause, "retry_in", op.cooldown)
	if peer != b.self {
		go b.snapshot(b.ctx, peer)
	}
}

// retry probes an open peer under the generation context. A stale timer that fires
// after the generation is torn down no-ops. A failure doubles the cooldown (capped)
// and re-arms silently; a success closes the breaker.
func (b *breakerNotifier) retry(peer string) {
	if b.ctx.Err() != nil {
		return
	}
	if err := b.inner.Notify(b.ctx, peer, ""); err != nil {
		b.mu.Lock()
		op, ok := b.open[peer]
		if !ok {
			b.mu.Unlock()
			return
		}
		op.cooldown = min(op.cooldown*2, breakerMaxCooldown)
		b.afterFunc(op.cooldown, func() { b.retry(peer) })
		b.mu.Unlock()
		return
	}
	b.closed(peer)
}

// closed clears an open peer and logs the single recovery transition with the
// outage duration. It no-ops if the peer was not open.
func (b *breakerNotifier) closed(peer string) {
	b.mu.Lock()
	op, ok := b.open[peer]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.open, peer)
	downFor := b.now().Sub(op.openedAt)
	b.mu.Unlock()

	slog.InfoContext(b.ctx, "watch: peer recovered", "manifest", b.name, "peer", peer, "down_for", downFor)
}

// tailscaleSnapshot logs a one-line tailnet view of peer the first time its breaker
// opens, to make the next unreachable-peer storm diagnosable. It never affects sync
// behavior: an unreachable or unparseable tailscale degrades to a single info line.
func tailscaleSnapshot(ctx context.Context, peer string) {
	ctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
	defer cancel()
	line, err := hostregistry.TailscalePeerStatus(ctx, hostregistry.NewExecRunner(), peer)
	if err != nil {
		slog.InfoContext(ctx, "watch: tailscale snapshot unavailable", "peer", peer, "err", err)
		return
	}
	slog.WarnContext(ctx, "watch: tailscale snapshot", "peer", peer, "status", line)
}
