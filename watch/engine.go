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

	mu         sync.Mutex
	timers     map[string]timer  // per-id debounce timer, keyed by digest
	lastDigest map[string]string // per-id last-acted-on fingerprint, keyed by digest
}

// NewEngine builds an engine over the given boundaries. resolver resolves an id's
// current fingerprint, notifier tells one peer about an id, digest maps an id to
// its stable key, debounce is the settle window a burst must be quiet for before
// one evaluate runs, and hosts are the peers fanned out to on a real change.
func NewEngine[T any](resolver Resolver[T], notifier Notifier[T], digest DigestFunc[T], debounce time.Duration, hosts []string) *Engine[T] {
	return &Engine[T]{
		resolver:   resolver,
		notifier:   notifier,
		digest:     digest,
		debounce:   debounce,
		hosts:      hosts,
		newTimer:   realNewTimer,
		timers:     make(map[string]timer),
		lastDigest: make(map[string]string),
	}
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
// silently; it never crashes or notifies.
func (e *Engine[T]) evaluate(ctx context.Context, id T) {
	key := e.digest(id)
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
