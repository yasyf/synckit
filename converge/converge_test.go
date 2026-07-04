package converge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/cregistry"
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

// setErr scripts peer's Fetch to fail (or, with a nil err, succeed) under lock so a
// test can flip a peer between reachable and unreachable across passes race-free.
func (f *fakeFetcher) setErr(peer string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errs == nil {
		f.errs = map[string]error{}
	}
	if err == nil {
		delete(f.errs, peer)
		return
	}
	f.errs[peer] = err
}

// meta is the per-item payload used throughout the tests: a small struct so the
// registry's value path runs against real JSON.
type meta struct {
	Tag string `json:"tag"`
}

// fakeDriver is an in-memory Driver: its registry stands in for the tool's state
// file, and Reconcile just records which ids it was asked to reconcile. saves counts
// persist calls so the idempotence test can assert the converged registry is written
// exactly as expected.
type fakeDriver struct {
	mu         sync.Mutex
	reg        cregistry.Registry[meta]
	reconciled []string
	saves      int
	saveErr    error
}

func newFakeDriver(reg cregistry.Registry[meta]) *fakeDriver {
	return &fakeDriver{reg: reg}
}

func (d *fakeDriver) LoadRegistry(context.Context) (cregistry.Registry[meta], error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps(d.reg), nil
}

func (d *fakeDriver) SaveRegistry(_ context.Context, reg cregistry.Registry[meta]) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.saveErr != nil {
		return d.saveErr
	}
	d.saves++
	d.reg = maps(reg)
	return nil
}

func (d *fakeDriver) Reconcile(_ context.Context, id string, _ cregistry.Entry[meta], _ []string, _ string) (Outcome, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reconciled = append(d.reconciled, id)
	return "reconciled", nil
}

func (d *fakeDriver) lastReconciled() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Sorted(slices.Values(d.reconciled))
}

// fakeFetcher serves a fixed per-peer registry and records every Fetch so a test can
// prove no peer was ever written to (there is no peer-write method — the type only
// reads — which is the structural loop-guard proof). A peer mapped to a nil registry
// with a non-nil error models an unreachable host.
type fakeFetcher struct {
	mu      sync.Mutex
	regs    map[string]cregistry.Registry[meta]
	errs    map[string]error
	fetched []string
}

func (f *fakeFetcher) Fetch(_ context.Context, peer string) (cregistry.Registry[meta], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetched = append(f.fetched, peer)
	if err := f.errs[peer]; err != nil {
		return nil, err
	}
	return maps(f.regs[peer]), nil
}

func (f *fakeFetcher) fetchedPeers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.fetched)
}

// noLock is the trivial lock wrapper: the tests are single-goroutine, so it just
// runs fn. It also proves Reconcile threads the lock without depending on flock.
func noLock(_ context.Context, fn func() error) error { return fn() }

// maps clones a registry so a fake never aliases internal state into a caller.
func maps(r cregistry.Registry[meta]) cregistry.Registry[meta] {
	out := make(cregistry.Registry[meta], len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

func ids(results []ItemResult) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, r.ID)
	}
	return slices.Sorted(slices.Values(out))
}

// TestPullMergeConvergesAndPersists proves a pass folds every peer's registry into
// the local one, persists the converged result, and reconciles exactly the present
// ids. local has A; peer-1 adds B; peer-2 adds C and removes A (later stamp). The
// merge must end with B and C present, A tombstoned, and the persisted registry equal
// to that merge.
func TestPullMergeConvergesAndPersists(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)

	peer1 := cregistry.New[meta]()
	peer1.Add("B", meta{Tag: "b"}, 20)

	peer2 := cregistry.New[meta]()
	peer2.Add("C", meta{Tag: "c"}, 30)
	peer2.Remove("A", 40) // later than local's add => A converges to absent

	d := newFakeDriver(local)
	f := &fakeFetcher{regs: map[string]cregistry.Registry[meta]{"peer-1": peer1, "peer-2": peer2}}

	results, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), []string{"peer-1", "peer-2"}, "")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got, want := ids(results), []string{"B", "C"}; !slices.Equal(got, want) {
		t.Fatalf("reconciled present ids = %v, want %v (A tombstoned, B+C present)", got, want)
	}
	if got, want := d.lastReconciled(), []string{"B", "C"}; !slices.Equal(got, want) {
		t.Fatalf("Driver.Reconcile called for %v, want %v", got, want)
	}

	want := cregistry.New[meta]()
	want.Add("A", meta{Tag: "a"}, 10)
	want.Remove("A", 40)
	want.Add("B", meta{Tag: "b"}, 20)
	want.Add("C", meta{Tag: "c"}, 30)
	if !reflect.DeepEqual(d.reg, want) {
		t.Fatalf("persisted registry =\n %v\nwant\n %v", d.reg, want)
	}
	if d.reg["A"].Present() {
		t.Fatal("A should be tombstoned (absent) but persisted as present")
	}
	if d.saves != 1 {
		t.Fatalf("SaveRegistry called %d times, want 1", d.saves)
	}
}

// TestOfflinePeerDoesNotAbort proves one unreachable peer is skipped, not fatal: the
// pass still converges against the reachable peer and persists. peer-down errors on
// Fetch; peer-up contributes B.
func TestOfflinePeerDoesNotAbort(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)

	peerUp := cregistry.New[meta]()
	peerUp.Add("B", meta{Tag: "b"}, 20)

	d := newFakeDriver(local)
	f := &fakeFetcher{
		regs: map[string]cregistry.Registry[meta]{"peer-up": peerUp},
		errs: map[string]error{"peer-down": errors.New("connection refused")},
	}

	results, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), []string{"peer-down", "peer-up"}, "")
	if err != nil {
		t.Fatalf("Reconcile must not abort on one unreachable peer: %v", err)
	}
	if got, want := ids(results), []string{"A", "B"}; !slices.Equal(got, want) {
		t.Fatalf("present ids = %v, want %v (reachable peer still merged)", got, want)
	}
	if _, ok := d.reg["B"]; !ok {
		t.Fatal("reachable peer's item B not merged into persisted registry")
	}
}

// TestIdempotentLoopGuard is the convergence/loop-guard proof: once converged, a
// SECOND pass with unchanged peers makes no net change to the registry, and across
// both passes no peer is ever written — the Fetcher only reads. The registry equality
// after pass 2 == after pass 1 is the "no net change"; the absence of any peer-write
// path is the structural guarantee that a reconcile on this host cannot make a peer do
// work.
func TestIdempotentLoopGuard(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	peer := cregistry.New[meta]()
	peer.Add("B", meta{Tag: "b"}, 20)

	d := newFakeDriver(local)
	f := &fakeFetcher{regs: map[string]cregistry.Registry[meta]{"peer-1": peer}}
	peers := []string{"peer-1"}

	if _, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), peers, ""); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	afterFirst := maps(d.reg)

	if _, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), peers, ""); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if !reflect.DeepEqual(d.reg, afterFirst) {
		t.Fatalf("second pass changed the registry:\n after1=%v\n after2=%v", afterFirst, d.reg)
	}

	// The fetched log only ever names peers — there is no peer-write method to call.
	for _, p := range f.fetchedPeers() {
		if p != "peer-1" {
			t.Fatalf("fetched an unexpected target %q; pull-merge must only read peer-1", p)
		}
	}
	if len(f.fetchedPeers()) != 2 {
		t.Fatalf("expected exactly 2 read-only fetches across 2 passes, got %d", len(f.fetchedPeers()))
	}
}

// TestOriginPeerSkipped proves the anti-echo provenance is honored: the origin peer is
// never fetched, so a pass triggered by peer-1's notification does not turn around and
// pull from peer-1.
func TestOriginPeerSkipped(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	peer1 := cregistry.New[meta]()
	peer1.Add("B", meta{Tag: "b"}, 20)
	peer2 := cregistry.New[meta]()
	peer2.Add("C", meta{Tag: "c"}, 30)

	d := newFakeDriver(local)
	f := &fakeFetcher{regs: map[string]cregistry.Registry[meta]{"peer-1": peer1, "peer-2": peer2}}

	if _, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), []string{"peer-1", "peer-2"}, "peer-1"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.fetchedPeers(); !slices.Equal(got, []string{"peer-2"}) {
		t.Fatalf("fetched %v, want only [peer-2] (origin peer-1 skipped)", got)
	}
	if _, ok := d.reg["B"]; ok {
		t.Fatal("origin peer-1's item B was merged; the origin must be skipped")
	}
}

// TestTombstonePersistedNotReconciled proves a removed item still propagates through a
// pass — it is persisted (the tombstone rides along) — but is never reconciled, since
// only Present() items reach Driver.Reconcile. A peer tombstones A; the local registry
// must end with A absent-but-present-in-the-map, and Reconcile must skip it.
func TestTombstonePersistedNotReconciled(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	local.Add("B", meta{Tag: "b"}, 10)

	peer := cregistry.New[meta]()
	peer.Add("A", meta{Tag: "a"}, 10)
	peer.Remove("A", 50) // tombstone A

	d := newFakeDriver(local)
	f := &fakeFetcher{regs: map[string]cregistry.Registry[meta]{"peer-1": peer}}

	results, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), []string{"peer-1"}, "")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// A is tombstoned: skipped by Reconcile, but its entry (with the tombstone) is
	// persisted so the removal propagates onward.
	if got, want := ids(results), []string{"B"}; !slices.Equal(got, want) {
		t.Fatalf("reconciled %v, want %v (A tombstoned must be skipped)", got, want)
	}
	entry, ok := d.reg["A"]
	if !ok {
		t.Fatal("tombstoned A dropped from persisted registry: removal would not propagate")
	}
	if entry.Present() {
		t.Fatal("A persisted as present, want tombstoned")
	}
	if entry.Removed != 50 {
		t.Fatalf("persisted A.Removed = %d, want 50 (tombstone stamp preserved)", entry.Removed)
	}
}

// TestSaveErrorAbortsPass proves a persistence failure is fatal to the pass and no
// item is reconciled (the converged state was never durably written).
func TestSaveErrorAbortsPass(t *testing.T) {
	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	d := newFakeDriver(local)
	d.saveErr = errors.New("disk full")
	f := &fakeFetcher{regs: map[string]cregistry.Registry[meta]{}}

	results, err := Reconcile(context.Background(), noLock, d, f, NewPeerStatus(), nil, "")
	if err == nil {
		t.Fatal("expected SaveRegistry failure to abort the pass")
	}
	if results != nil {
		t.Fatalf("results = %v, want nil when persist fails", results)
	}
	if got := d.lastReconciled(); len(got) != 0 {
		t.Fatalf("reconciled %v after a persist failure, want none", got)
	}
}

// TestUnreachablePeerLogsOncePerOutage proves the transition tracker collapses a
// sustained outage into a single warn: two passes against the same down peer,
// sharing one PeerStatus, log the unreachable line exactly once — not once per pass.
func TestUnreachablePeerLogsOncePerOutage(t *testing.T) {
	buf := captureSlog(t)

	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	d := newFakeDriver(local)
	f := &fakeFetcher{
		regs: map[string]cregistry.Registry[meta]{},
		errs: map[string]error{"peer-down": errors.New("connection refused")},
	}
	status := NewPeerStatus()
	peers := []string{"peer-down"}

	for pass := 0; pass < 2; pass++ {
		if _, err := Reconcile(context.Background(), noLock, d, f, status, peers, ""); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}

	if got := strings.Count(buf.String(), "converge: peer unreachable"); got != 1 {
		t.Fatalf("unreachable warns = %d across two passes, want exactly 1\nlog:\n%s", got, buf.String())
	}
}

// TestPeerRecoveryLogsOnceWithDuration proves recovery logs one line carrying the
// outage duration: a down pass then two up passes emit exactly one "peer recovered"
// record, stamped with the down_for the injected clock makes deterministic, and no
// recovered line while the peer stays reachable.
func TestPeerRecoveryLogsOnceWithDuration(t *testing.T) {
	buf := captureSlog(t)

	local := cregistry.New[meta]()
	local.Add("A", meta{Tag: "a"}, 10)
	d := newFakeDriver(local)
	f := &fakeFetcher{
		regs: map[string]cregistry.Registry[meta]{"peer": cregistry.New[meta]()},
		errs: map[string]error{"peer": errors.New("connection refused")},
	}
	status := NewPeerStatus()
	clock := time.Unix(1000, 0)
	status.now = func() time.Time { return clock }
	peers := []string{"peer"}

	// Down pass: peer unreachable, outage begins at t0.
	if _, err := Reconcile(context.Background(), noLock, d, f, status, peers, ""); err != nil {
		t.Fatalf("down pass: %v", err)
	}
	// Peer heals; 90s elapse before the recovery pass.
	f.setErr("peer", nil)
	clock = clock.Add(90 * time.Second)
	for pass := 0; pass < 2; pass++ {
		if _, err := Reconcile(context.Background(), noLock, d, f, status, peers, ""); err != nil {
			t.Fatalf("up pass %d: %v", pass, err)
		}
	}

	if got := strings.Count(buf.String(), "converge: peer recovered"); got != 1 {
		t.Fatalf("recovered records = %d, want exactly 1\nlog:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "down_for=1m30s") {
		t.Fatalf("recovery line missing down_for=1m30s\nlog:\n%s", buf.String())
	}
}
