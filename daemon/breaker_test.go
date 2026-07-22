package daemon

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/watch"
)

// captureSlog swaps the default slog logger for a buffered text handler at debug
// level so a test can count transition log lines, restoring it on cleanup.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// notifyRec is one recorded inner notify.
type notifyRec struct{ peer, id string }

// fakeInner is a scriptable watch.Notifier[string]: it records every notify and
// fails while fail is set, so a test drives the breaker's open/retry/close paths.
type fakeInner struct {
	mu    sync.Mutex
	calls []notifyRec
	fail  bool
}

func (f *fakeInner) Notify(_ context.Context, peer, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, notifyRec{peer, id})
	if f.fail {
		return errFakeDown
	}
	return nil
}

func (f *fakeInner) setFail(v bool) {
	f.mu.Lock()
	f.fail = v
	f.mu.Unlock()
}

func (f *fakeInner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeInner) lastCall() notifyRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

// breakerHarness captures the breaker's timer, clock, and snapshot seams so a test
// drives cooldowns deterministically instead of sleeping, and observes each armed
// cooldown and every tailscale snapshot request.
type breakerHarness struct {
	mu    sync.Mutex
	clock time.Time
	armed []time.Duration
	fns   []func()
	fired int
	snaps chan string
}

func (h *breakerHarness) nowFn() func() time.Time {
	return func() time.Time {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.clock
	}
}

func (h *breakerHarness) setClock(ts time.Time) {
	h.mu.Lock()
	h.clock = ts
	h.mu.Unlock()
}

func (h *breakerHarness) advance(d time.Duration) {
	h.mu.Lock()
	h.clock = h.clock.Add(d)
	h.mu.Unlock()
}

func (h *breakerHarness) armFn() func(time.Duration, func()) {
	return func(d time.Duration, fn func()) {
		h.mu.Lock()
		h.armed = append(h.armed, d)
		h.fns = append(h.fns, fn)
		h.mu.Unlock()
	}
}

func (h *breakerHarness) snapFn() func(context.Context, string) {
	return func(_ context.Context, peer string) { h.snaps <- peer }
}

func (h *breakerHarness) cooldowns() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.armed)
}

// fire invokes the next un-fired armed timer, mimicking its cooldown lapsing.
func (h *breakerHarness) fire() {
	h.mu.Lock()
	if h.fired >= len(h.fns) {
		h.mu.Unlock()
		panic("breakerHarness.fire: no armed timer pending")
	}
	fn := h.fns[h.fired]
	h.fired++
	h.mu.Unlock()
	fn()
}

func (h *breakerHarness) waitSnapshot(t *testing.T) string {
	t.Helper()
	select {
	case p := <-h.snaps:
		return p
	case <-time.After(time.Second):
		t.Fatal("no tailscale snapshot within 1s")
		return ""
	}
}

func (h *breakerHarness) noSnapshot(t *testing.T) {
	t.Helper()
	select {
	case p := <-h.snaps:
		t.Fatalf("unexpected tailscale snapshot for %q", p)
	case <-time.After(50 * time.Millisecond):
	}
}

func newTestBreaker(ctx context.Context, t *testing.T, inner watch.Notifier[string], self string) (*breakerNotifier, *breakerHarness) {
	t.Helper()
	h := &breakerHarness{clock: time.Unix(0, 0), snaps: make(chan string, 16)}
	b := newBreakerNotifier(ctx, inner, "stub", self, testDaemonPool(ctx, t))
	b.now = h.nowFn()
	b.afterFunc = h.armFn()
	b.snapshot = h.snapFn()
	return b, h
}

func TestBreakerPassthroughWhenClosed(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")

	for i := 0; i < 3; i++ {
		if err := b.Notify(context.Background(), "peer@node", "id"); err != nil {
			t.Fatalf("Notify: %v", err)
		}
	}
	if got := inner.count(); got != 3 {
		t.Fatalf("inner notify calls = %d, want 3 (all pass through while closed)", got)
	}
	if got := len(h.cooldowns()); got != 0 {
		t.Fatalf("armed timers = %d, want 0 while closed", got)
	}
	if s := buf.String(); strings.Contains(s, "peer unreachable") || strings.Contains(s, "peer recovered") {
		t.Fatalf("unexpected transition log:\n%s", s)
	}
}

func TestBreakerOpensOnFirstFailure(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")

	if err := b.Notify(context.Background(), "peer@node", "id"); err != nil {
		t.Fatalf("Notify = %v, want nil (breaker swallows the failure so engine.go stays silent)", err)
	}
	if got := inner.count(); got != 1 {
		t.Fatalf("inner calls = %d, want 1 (attempted once)", got)
	}
	if got := h.cooldowns(); len(got) != 1 || got[0] != breakerInitialCooldown {
		t.Fatalf("armed cooldowns = %v, want [%s]", got, breakerInitialCooldown)
	}
	if got := strings.Count(buf.String(), "watch: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d, want 1\n%s", got, buf.String())
	}
	if p := h.waitSnapshot(t); p != "peer@node" {
		t.Fatalf("snapshot peer = %q, want peer@node", p)
	}
}

func TestBreakerConcurrentFailuresOpenOnce(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")

	// Race the open transition: many goroutines fail Notify for the SAME peer with
	// distinct ids at once. Extra inner attempts before the breaker latches are
	// fine — the invariant is that the peer opens once, arms one retry, warns once.
	const goroutines = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			<-start
			if err := b.Notify(context.Background(), "peer@node", strconv.Itoa(id)); err != nil {
				t.Errorf("Notify = %v, want nil", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	b.mu.Lock()
	openEntries := len(b.open)
	b.mu.Unlock()
	if openEntries != 1 {
		t.Fatalf("open breaker entries = %d, want 1 (concurrent failures latch the peer once)", openEntries)
	}
	if got := h.cooldowns(); len(got) != 1 || got[0] != breakerInitialCooldown {
		t.Fatalf("armed cooldowns = %v, want exactly [%s] (a single retry timer)", got, breakerInitialCooldown)
	}
	if got := strings.Count(buf.String(), "watch: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d, want 1 (only the winning open transition logs)\n%s", got, buf.String())
	}
}

func TestBreakerSuppressesWhileOpen(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")
	ctx := context.Background()

	if err := b.Notify(ctx, "peer@node", "id"); err != nil {
		t.Fatal(err)
	}
	h.waitSnapshot(t)
	base := inner.count() // the single opening attempt

	for i := 0; i < 5; i++ {
		if err := b.Notify(ctx, "peer@node", "x"); err != nil {
			t.Fatalf("suppressed Notify = %v, want nil", err)
		}
	}
	if got := inner.count(); got != base {
		t.Fatalf("inner calls = %d after 5 suppressed notifies, want %d (open breaker owns retries)", got, base)
	}
	if got := strings.Count(buf.String(), "watch: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d, want still 1 (no log per suppressed notify)", got)
	}
	h.noSnapshot(t) // no second snapshot for the same open episode
}

func TestBreakerRetryBacksOffExponentially(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")

	if err := b.Notify(context.Background(), "peer@node", "id"); err != nil {
		t.Fatal(err)
	}
	h.waitSnapshot(t)
	for i := 0; i < 5; i++ {
		h.fire() // each retry fails and re-arms at the doubled, capped cooldown
	}

	want := []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute,
	}
	if got := h.cooldowns(); !slices.Equal(got, want) {
		t.Fatalf("armed cooldowns = %v, want %v", got, want)
	}
	if got := strings.Count(buf.String(), "watch: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d, want 1 (retries log nothing)", got)
	}
	if strings.Contains(buf.String(), "recovered") {
		t.Fatalf("recovered logged while peer still down:\n%s", buf.String())
	}
}

func TestBreakerRecoveryRetryIsCatchUpSync(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")
	ctx := context.Background()

	h.setClock(time.Unix(1000, 0))
	if err := b.Notify(ctx, "peer@node", "orig-id"); err != nil {
		t.Fatal(err)
	}
	h.waitSnapshot(t)

	inner.setFail(false)        // peer heals
	h.advance(45 * time.Second) // 45s down before the retry probe
	h.fire()

	// A successful retry is itself a full-manifest catch-up sync: inner is notified
	// with an empty id, not the original event id.
	if last := inner.lastCall(); last.peer != "peer@node" || last.id != "" {
		t.Fatalf("retry notify = %+v, want {peer@node <empty id>}", last)
	}
	if got := strings.Count(buf.String(), "watch: peer recovered"); got != 1 {
		t.Fatalf("recovered records = %d, want 1", got)
	}
	if !strings.Contains(buf.String(), "down_for=45s") {
		t.Fatalf("recovery line missing down_for=45s:\n%s", buf.String())
	}

	// The breaker is closed: a subsequent real notify reaches inner again.
	before := inner.count()
	if err := b.Notify(ctx, "peer@node", "id2"); err != nil {
		t.Fatal(err)
	}
	if inner.count() != before+1 {
		t.Fatal("closed breaker did not pass a later notify through to inner")
	}
}

func TestBreakerSelfPeerSkipsSnapshot(t *testing.T) {
	buf := captureSlog(t)
	inner := &fakeInner{fail: true}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")

	if err := b.Notify(context.Background(), "me@self", "id"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(buf.String(), "watch: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d, want 1 even for the self host", got)
	}
	h.noSnapshot(t) // the local host takes no tailscale snapshot
}

func TestBreakerRetryNoopsAfterGenerationCancel(t *testing.T) {
	captureSlog(t)
	inner := &fakeInner{fail: true}
	ctx, cancel := context.WithCancel(context.Background())
	b, h := newTestBreaker(ctx, t, inner, "me@self")

	if err := b.Notify(ctx, "peer@node", "id"); err != nil {
		t.Fatal(err)
	}
	h.waitSnapshot(t)
	before := inner.count()

	cancel() // the watch generation is torn down
	h.fire() // a stale retry timer fires against the dead generation

	if got := inner.count(); got != before {
		t.Fatalf("inner called %d times after generation cancel, want %d (stale retry must no-op)", got, before)
	}
}

// perPeerInner is a watch.Notifier[string] that fails only for the peers in down, so a
// test can drive one peer's breaker open while another stays healthy.
type perPeerInner struct {
	mu    sync.Mutex
	down  map[string]bool
	calls []notifyRec
}

func (f *perPeerInner) Notify(_ context.Context, peer, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, notifyRec{peer, id})
	if f.down[peer] {
		return errFakeDown
	}
	return nil
}

func (f *perPeerInner) callsFor(peer string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.peer == peer {
			n++
		}
	}
	return n
}

// TestBreakerIsolatesPeers pins that the per-peer breaker keys entirely on peer (S5): one
// peer failing opens only its own breaker, and its open state neither opens nor
// suppresses a healthy peer. A down peer's outage must never gate notifications to the
// peers that are still reachable.
func TestBreakerIsolatesPeers(t *testing.T) {
	captureSlog(t)
	inner := &perPeerInner{down: map[string]bool{"down@node": true}}
	b, h := newTestBreaker(context.Background(), t, inner, "me@self")
	ctx := context.Background()

	if err := b.Notify(ctx, "down@node", "id"); err != nil {
		t.Fatal(err)
	}
	if err := b.Notify(ctx, "up@node", "id"); err != nil {
		t.Fatal(err)
	}
	h.waitSnapshot(t) // only the opened (down) peer snapshots

	b.mu.Lock()
	_, downOpen := b.open["down@node"]
	_, upOpen := b.open["up@node"]
	openCount := len(b.open)
	b.mu.Unlock()
	if !downOpen || upOpen || openCount != 1 {
		t.Fatalf("open breakers: count=%d down=%v up=%v, want only down@node open", openCount, downOpen, upOpen)
	}

	// Further notifies: the down peer is suppressed (inner never re-called) while the
	// healthy peer keeps flowing through — one peer's outage never gates another.
	downBase := inner.callsFor("down@node")
	upBase := inner.callsFor("up@node")
	for i := 0; i < 3; i++ {
		if err := b.Notify(ctx, "down@node", "x"); err != nil {
			t.Fatal(err)
		}
		if err := b.Notify(ctx, "up@node", "x"); err != nil {
			t.Fatal(err)
		}
	}
	if got := inner.callsFor("down@node"); got != downBase {
		t.Errorf("down@node inner calls = %d, want %d (its open breaker suppresses)", got, downBase)
	}
	if got := inner.callsFor("up@node"); got != upBase+3 {
		t.Errorf("up@node inner calls = %d, want %d (healthy peer unaffected by the other's open breaker)", got, upBase+3)
	}
}
