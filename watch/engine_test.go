package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// item is a test identity type standing in for a consumer's domain object. Its key
// is the stable digest the engine tracks under.
type item struct {
	key string
}

const testKey = "alpha"

func testItem() item { return item{key: testKey} }

func itemDigest(it item) string { return it.key }

func newTestEngine(resolver Resolver[item], notifier Notifier[item], debounce time.Duration, hosts []string) *Engine[item] {
	return NewEngine[item](resolver, notifier, itemDigest, debounce, hosts)
}

// fakeResolver returns scripted fingerprints per call, advancing through the script
// so successive evaluations can observe a changed or unchanged fingerprint.
type fakeResolver struct {
	mu           sync.Mutex
	fingerprints []string
	err          error
	calls        int
}

func (r *fakeResolver) Resolve(_ context.Context, _ item) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return "", r.err
	}
	f := r.fingerprints[r.calls]
	if r.calls < len(r.fingerprints)-1 {
		r.calls++
	}
	return f, nil
}

// notifyCall records one (peer, key) notification.
type notifyCall struct {
	peer string
	key  string
}

// fakeNotifier records every notification and can be told to fail for one peer.
type fakeNotifier struct {
	mu       sync.Mutex
	calls    []notifyCall
	failPeer string
}

func (n *fakeNotifier) Notify(_ context.Context, peer string, it item) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, notifyCall{peer: peer, key: it.key})
	if peer == n.failPeer {
		return errors.New("peer unreachable")
	}
	return nil
}

func (n *fakeNotifier) snapshot() []notifyCall {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]notifyCall, len(n.calls))
	copy(out, n.calls)
	return out
}

// fakeTimer is a debounce timer whose fire is triggered by the test, never by
// wall-clock time, so debounce coalescing is deterministic. resets counts how many
// times the timer was re-armed (one per coalesced event after the first).
type fakeTimer struct {
	mu     sync.Mutex
	fn     func()
	resets int
	popped bool
}

func (t *fakeTimer) Reset(time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resets++
	return true
}

func (t *fakeTimer) Stop() bool { return true }

// fire invokes the debounce callback, mimicking the timer expiring after the
// debounce window of quiescence.
func (t *fakeTimer) fire() {
	t.mu.Lock()
	if t.popped {
		t.mu.Unlock()
		return
	}
	t.popped = true
	fn := t.fn
	t.mu.Unlock()
	fn()
}

func (t *fakeTimer) resetCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resets
}

func TestDebounceCoalescesBurstIntoOneEvaluation(t *testing.T) {
	resolver := &fakeResolver{fingerprints: []string{"fpA"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1"})

	var ft *fakeTimer
	eng.newTimer = func(_ time.Duration, fn func()) timer {
		ft = &fakeTimer{fn: fn}
		return ft
	}

	it := testItem()
	ctx := context.Background()
	eng.OnEvent(ctx, it)
	eng.OnEvent(ctx, it)
	eng.OnEvent(ctx, it)

	if ft == nil {
		t.Fatal("no timer was armed")
	}
	if got := ft.resetCount(); got != 2 {
		t.Errorf("timer resets = %d, want 2 (3 events, 1 arm + 2 resets)", got)
	}

	resolver.mu.Lock()
	calls := resolver.calls
	resolver.mu.Unlock()
	if calls != 0 || len(notifier.snapshot()) != 0 {
		t.Fatalf("evaluation ran before debounce fired: resolver.calls=%d notifies=%d", calls, len(notifier.snapshot()))
	}

	ft.fire()

	if got := len(notifier.snapshot()); got != 1 {
		t.Fatalf("notifies = %d, want exactly 1 (burst coalesced)", got)
	}
	if got := notifier.snapshot()[0]; got.peer != "peer1" || got.key != testKey {
		t.Errorf("notify = %+v, want {peer1 %s}", got, testKey)
	}
}

func TestDedupeUnchangedFingerprintNoNotifyOnSecondEvaluation(t *testing.T) {
	resolver := &fakeResolver{fingerprints: []string{"fpA", "fpA"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1"})
	ctx := context.Background()
	it := testItem()

	eng.evaluate(ctx, it)
	eng.evaluate(ctx, it)

	if got := len(notifier.snapshot()); got != 1 {
		t.Fatalf("notifies = %d, want 1 (second evaluation deduped)", got)
	}
}

func TestFingerprintChangeNotifiesAllPeersOnceAndUpdatesLastDigest(t *testing.T) {
	resolver := &fakeResolver{fingerprints: []string{"fpA", "fpB"}}
	notifier := &fakeNotifier{}
	hosts := []string{"peer1", "peer2", "peer3"}
	eng := newTestEngine(resolver, notifier, time.Hour, hosts)
	ctx := context.Background()
	it := testItem()

	eng.evaluate(ctx, it) // fpA -> notify all
	eng.evaluate(ctx, it) // fpB -> notify all again

	calls := notifier.snapshot()
	if len(calls) != 6 {
		t.Fatalf("notifies = %d, want 6 (3 peers x 2 changes)", len(calls))
	}
	perPeer := map[string]int{}
	for _, c := range calls {
		if c.key != testKey {
			t.Errorf("notify key = %q, want %q", c.key, testKey)
		}
		perPeer[c.peer]++
	}
	for _, peer := range hosts {
		if perPeer[peer] != 2 {
			t.Errorf("peer %s notified %d times, want 2", peer, perPeer[peer])
		}
	}

	eng.mu.Lock()
	got := eng.lastDigest[testKey]
	eng.mu.Unlock()
	if got != "fpB" {
		t.Errorf("lastDigest = %q, want fpB", got)
	}
}

func TestResolverErrorNoNotifyNoCrash(t *testing.T) {
	resolver := &fakeResolver{err: errors.New("no truth")}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1"})

	eng.evaluate(context.Background(), testItem())

	if got := len(notifier.snapshot()); got != 0 {
		t.Errorf("notifies = %d, want 0 on resolver error", got)
	}
	eng.mu.Lock()
	_, recorded := eng.lastDigest[testKey]
	eng.mu.Unlock()
	if recorded {
		t.Error("lastDigest recorded despite resolver error")
	}
}

func TestOnePeerFailureDoesNotBlockOthers(t *testing.T) {
	resolver := &fakeResolver{fingerprints: []string{"fpA"}}
	notifier := &fakeNotifier{failPeer: "peer2"}
	hosts := []string{"peer1", "peer2", "peer3"}
	eng := newTestEngine(resolver, notifier, time.Hour, hosts)

	eng.evaluate(context.Background(), testItem())

	calls := notifier.snapshot()
	if len(calls) != 3 {
		t.Fatalf("notifies = %d, want 3 (all peers attempted despite one failure)", len(calls))
	}
	notified := map[string]bool{}
	for _, c := range calls {
		notified[c.peer] = true
	}
	for _, peer := range hosts {
		if !notified[peer] {
			t.Errorf("peer %s was not notified (failure isolation broken)", peer)
		}
	}
}

func TestAntiEchoSameHashTerminatesLoop(t *testing.T) {
	// First event resolves X and notifies; a second event (the self-induced echo)
	// resolves the SAME X and must produce no further notify. This proves the
	// record-before-notify ordering in evaluate: lastDigest is written under mu
	// before the unlock+notify, so the echo is deduped and the loop terminates.
	resolver := &fakeResolver{fingerprints: []string{"fpX", "fpX"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1", "peer2"})
	ctx := context.Background()
	it := testItem()

	eng.evaluate(ctx, it) // resolves X, notifies both peers
	if got := len(notifier.snapshot()); got != 2 {
		t.Fatalf("first evaluate notifies = %d, want 2", got)
	}

	eng.evaluate(ctx, it) // echo: same X, no notify
	if got := len(notifier.snapshot()); got != 2 {
		t.Fatalf("after echo notifies = %d, want 2 (loop terminated, no new notify)", got)
	}
}

func TestSeedSuppressesNextEvaluateAsOwnEcho(t *testing.T) {
	// A write the consumer induces out of band (e.g. a peer-driven apply) seeds the
	// fingerprint of the set it is about to write, then the filesystem event that
	// write produces resolves to that same fingerprint. Seeding before the resolve
	// must make evaluate recognize it as the engine's own echo and not notify — the
	// anti-echo path for a Resolver that reads the very file being written.
	resolver := &fakeResolver{fingerprints: []string{"fpWritten"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1", "peer2"})
	ctx := context.Background()
	it := testItem()

	eng.Seed(it, "fpWritten") // consumer records the set it is about to write
	eng.evaluate(ctx, it)     // the induced fs event resolves the same fingerprint

	if got := len(notifier.snapshot()); got != 0 {
		t.Fatalf("notifies = %d, want 0 (seeded write recognized as own echo)", got)
	}
}

func TestSeedThenGenuineChangeStillNotifies(t *testing.T) {
	// Seeding the prior write must not deafen the engine to a later genuine change:
	// once a different fingerprint resolves, the engine notifies exactly once.
	resolver := &fakeResolver{fingerprints: []string{"fpOther"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, []string{"peer1"})
	ctx := context.Background()
	it := testItem()

	eng.Seed(it, "fpWritten") // a prior self-induced write
	eng.evaluate(ctx, it)     // a genuinely different fingerprint now

	if got := len(notifier.snapshot()); got != 1 {
		t.Fatalf("notifies = %d, want 1 (genuine change after a seed still notifies)", got)
	}
	eng.mu.Lock()
	got := eng.lastDigest[testKey]
	eng.mu.Unlock()
	if got != "fpOther" {
		t.Errorf("lastDigest = %q, want fpOther", got)
	}
}

func TestNoPeersNoNotifyButLastDigestTracked(t *testing.T) {
	resolver := &fakeResolver{fingerprints: []string{"fpA"}}
	notifier := &fakeNotifier{}
	eng := newTestEngine(resolver, notifier, time.Hour, nil)

	eng.evaluate(context.Background(), testItem())

	if got := len(notifier.snapshot()); got != 0 {
		t.Errorf("notifies = %d, want 0 with no peers", got)
	}
	eng.mu.Lock()
	got := eng.lastDigest[testKey]
	eng.mu.Unlock()
	if got != "fpA" {
		t.Errorf("lastDigest = %q, want fpA tracked even with no peers", got)
	}
}
