// Package watch is the generic, domain-agnostic anti-echo watch engine shared
// across synckit tools: a debounce/settle loop, dedupe by a resolved fingerprint,
// record-before-notify so a self-induced echo terminates the loop, and concurrent
// peer fan-out. It is parameterized over an opaque identity type T; the consumer
// supplies a Resolver (the current "truth" fingerprint for an id), a Notifier (how
// to tell one peer about an id), and a DigestFunc (a stable map key for an id). The
// engine owns no I/O wiring of its own — no filesystem watcher, no transport — so
// it is driven directly in tests through the same boundaries production uses.
package watch

import (
	"context"
	"log"
	"sync"
	"time"
)

// Resolver resolves the current "truth" fingerprint for id. A change in the
// returned string is what the engine acts on; an error is logged and skipped
// (never notified, never recorded). Implementations must be safe for concurrent
// use.
type Resolver[T any] interface {
	Resolve(ctx context.Context, id T) (string, error)
}

// Notifier notifies one peer host about id. A failure to reach one peer must not
// block the others, so the engine fans out concurrently and isolates each error.
type Notifier[T any] interface {
	Notify(ctx context.Context, peer string, id T) error
}

// Gate reports whether id is busy — mid-operation in a way that makes acting on
// it right now unwelcome. A busy id's evaluation is deferred and retried rather
// than acted on, so the pending change fires shortly after the id goes idle
// instead of parking until some external tick. reason is logged with each
// deferral. The gate is politeness and latency only, never a correctness
// backstop: gate-to-act is a TOCTOU window, and the consumer's own guards stay
// authoritative. Implementations must be safe for concurrent use.
type Gate[T any] interface {
	Busy(ctx context.Context, id T) (busy bool, reason string, err error)
}

// DigestFunc returns a stable map key for id — the key under which the engine
// tracks the per-id debounce timer and the last-acted-on fingerprint. It MUST be
// stable: the same id must always yield the same key, or debounce coalescing and
// dedupe silently break (each call lands its own timer and fingerprint slot).
type DigestFunc[T any] func(id T) string

// timer is the slice of *time.Timer the engine depends on, extracted so tests can
// drive debounce deterministically instead of sleeping on a wall clock.
type timer interface {
	Reset(d time.Duration) bool
	Stop() bool
}

// newTimerFunc builds a debounce timer that fires fn after d. The production
// implementation is a real *time.Timer; tests swap in a fake.
type newTimerFunc func(d time.Duration, fn func()) timer

// realTimer adapts *time.Timer to the timer interface.
type realTimer struct{ t *time.Timer }

func (rt realTimer) Reset(d time.Duration) bool { return rt.t.Reset(d) }
func (rt realTimer) Stop() bool                 { return rt.t.Stop() }

func realNewTimer(d time.Duration, fn func()) timer {
	return realTimer{t: time.AfterFunc(d, fn)}
}

// Engine is the pure debounce + dedupe + notify core, free of any filesystem or
// transport wiring. OnEvent coalesces a burst of events per id into a single
// evaluate; evaluate resolves the id's fingerprint, dedupes by digest key, and
// fans out to every peer. The boundaries (Resolver, Notifier, DigestFunc, and the
// timer seam) are injected so the whole core is driven directly in tests.
type Engine[T any] struct {
	resolver Resolver[T]
	notifier Notifier[T]
	digest   DigestFunc[T]
	debounce time.Duration
	hosts    []string
	newTimer newTimerFunc
	now      func() time.Time

	gate         Gate[T]
	gateRetry    time.Duration
	gateMaxDefer time.Duration

	mu            sync.Mutex
	timers        map[string]timer     // per-id debounce timer, keyed by digest
	lastDigest    map[string]string    // per-id last-acted-on fingerprint, keyed by digest
	deferredSince map[string]time.Time // per-id first busy deferral, keyed by digest
}

// Option customizes an Engine beyond NewEngine's required boundaries.
type Option[T any] func(*Engine[T])

// WithGate installs g as the engine's busy gate. Before an evaluation compares
// and records the fingerprint, the engine asks g whether the id is busy; while
// it is, the evaluation defers — nothing is recorded or notified, and the
// per-id timer re-arms at the retry cadence (the debounce window when retry is
// zero) so the pending change stays live until the id goes idle. An id deferred
// for longer than maxDefer fires through anyway, so a persistently busy gate
// can only delay a change, never park it; the deferral clock resets whenever an
// evaluation proceeds. A gate error is logged and treated as not busy.
func WithGate[T any](g Gate[T], retry, maxDefer time.Duration) Option[T] {
	return func(e *Engine[T]) {
		e.gate = g
		e.gateRetry = retry
		e.gateMaxDefer = maxDefer
	}
}

// NewEngine builds an engine over the given boundaries. resolver resolves an id's
// current fingerprint, notifier tells one peer about an id, digest maps an id to
// its stable key, debounce is the settle window a burst must be quiet for before
// one evaluate runs, and hosts are the peers fanned out to on a real change.
func NewEngine[T any](resolver Resolver[T], notifier Notifier[T], digest DigestFunc[T], debounce time.Duration, hosts []string, opts ...Option[T]) *Engine[T] {
	e := &Engine[T]{
		resolver:      resolver,
		notifier:      notifier,
		digest:        digest,
		debounce:      debounce,
		hosts:         hosts,
		newTimer:      realNewTimer,
		now:           time.Now,
		timers:        make(map[string]timer),
		lastDigest:    make(map[string]string),
		deferredSince: make(map[string]time.Time),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// OnEvent records an event for id. It (re)arms a single per-id debounce timer so a
// burst of events — one fetch writes a loose ref, FETCH_HEAD, and an op head —
// collapses into exactly one evaluate once the id has been quiet for the debounce
// window.
func (e *Engine[T]) OnEvent(ctx context.Context, id T) {
	key := e.digest(id)
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.timers[key]; ok {
		t.Reset(e.debounce)
		return
	}
	e.timers[key] = e.newTimer(e.debounce, func() {
		e.fire(ctx, id)
	})
}

// Seed records fingerprint as id's last-acted-on fingerprint without notifying, so
// the next evaluate of id dedupes against it. It is the seam for a write the
// consumer induces out of band — one a peer triggers over RPC rather than the local
// watcher — where the consumer's Resolver reads the very file being written (so a
// self-induced write would otherwise resolve a fresh fingerprint and notify, an echo
// storm). The consumer seeds the fingerprint of the set it is about to write
// immediately before writing it; the filesystem event that write produces then
// resolves to the seeded fingerprint and is suppressed as the engine's own echo. A
// consumer whose Resolver reads upstream truth (a hash a self-write does not move)
// never needs this. Safe for concurrent use.
func (e *Engine[T]) Seed(id T, fingerprint string) {
	key := e.digest(id)
	e.mu.Lock()
	e.lastDigest[key] = fingerprint
	e.mu.Unlock()
}

// fire drops the spent timer and runs the evaluation for id.
func (e *Engine[T]) fire(ctx context.Context, id T) {
	key := e.digest(id)
	e.mu.Lock()
	delete(e.timers, key)
	e.mu.Unlock()
	e.evaluate(ctx, id)
}

// evaluate resolves the id's fingerprint, dedupes against the last acted-on
// fingerprint, and on a real change records the new fingerprint *before* notifying
// peers (so the self-induced event that follows is recognized as a no-op) then
// notifies every peer concurrently. A resolver error is logged and skipped
// silently; it never crashes or notifies. A gated engine consults the gate first:
// a busy id defers — no resolve, no record, no notify — and re-arms itself, so
// the change stays pending rather than parked.
func (e *Engine[T]) evaluate(ctx context.Context, id T) {
	key := e.digest(id)
	if e.gate != nil && e.deferBusy(ctx, id, key) {
		return
	}

	fingerprint, err := e.resolver.Resolve(ctx, id)
	if err != nil {
		log.Printf("watch: %s: resolve: %v", key, err)
		return
	}

	e.mu.Lock()
	if e.lastDigest[key] == fingerprint {
		e.mu.Unlock()
		return
	}
	e.lastDigest[key] = fingerprint
	e.mu.Unlock()

	e.notifyPeers(ctx, id)
}

// deferBusy decides whether this evaluation of id defers. A busy id inside the
// maxDefer window logs the reason, re-arms the per-id timer at the retry cadence,
// and reports true; the caller returns without recording or notifying. A gate
// error is logged and treated as not busy; an id deferred longer than maxDefer
// fires through. Whenever the evaluation proceeds, the id's deferral clock
// resets, so a once-deferred id earns a fresh window the next time it is busy.
func (e *Engine[T]) deferBusy(ctx context.Context, id T, key string) bool {
	busy, reason, err := e.gate.Busy(ctx, id)
	if err != nil {
		log.Printf("watch: %s: gate: %v", key, err)
		busy = false
	}

	e.mu.Lock()
	if !busy {
		delete(e.deferredSince, key)
		e.mu.Unlock()
		return false
	}
	since, ok := e.deferredSince[key]
	if !ok {
		since = e.now()
		e.deferredSince[key] = since
	}
	if e.now().Sub(since) > e.gateMaxDefer {
		delete(e.deferredSince, key)
		e.mu.Unlock()
		return false
	}
	e.mu.Unlock()

	log.Printf("watch: %s: deferred: %s", key, reason)
	e.rearm(ctx, id, key)
	return true
}

// rearm schedules the deferred id's next evaluation at the retry cadence,
// falling back to the debounce window when no retry cadence was configured.
func (e *Engine[T]) rearm(ctx context.Context, id T, key string) {
	d := e.gateRetry
	if d == 0 {
		d = e.debounce
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.timers[key]; ok {
		t.Reset(d)
		return
	}
	e.timers[key] = e.newTimer(d, func() {
		e.fire(ctx, id)
	})
}

// notifyPeers fans the notification out to every peer concurrently. A down or
// failing peer is logged and isolated — the others are still notified.
func (e *Engine[T]) notifyPeers(ctx context.Context, id T) {
	key := e.digest(id)
	var wg sync.WaitGroup
	for _, peer := range e.hosts {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			if err := e.notifier.Notify(ctx, peer, id); err != nil {
				log.Printf("watch: %s: notify %s: %v", key, peer, err)
			}
		}(peer)
	}
	wg.Wait()
}
