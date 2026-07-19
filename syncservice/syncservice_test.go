package syncservice

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

// stateJSON is a registry literal with a 16-digit int64 added_at stamp. A correct
// round trip preserves these bytes exactly; decoding through any would render the
// stamp as the float64 "1.7192736e+15".
const stateJSON = `{"x":{"added_at":1719273600000000,"removed_at":0,"value":{"k":"v"}}}`

// fakeConsumer is a deterministic SyncConsumer for the round-trip test. Reconcile and
// Sync stash the origin they saw so the test can assert the client propagated it.
type fakeConsumer struct {
	reconcileOrigin string
	syncOrigin      string
}

func (f *fakeConsumer) Capabilities(_ context.Context) (Capabilities, error) {
	return DefaultCapabilities("fake"), nil
}

func (f *fakeConsumer) List(_ context.Context) ([]WatchItem, error) {
	return []WatchItem{
		{ID: "alpha", WatchDirs: []string{"/a", "/b"}, Fingerprint: "fa"},
		{ID: "beta", WatchDirs: []string{"/c"}, Fingerprint: "fb"},
	}, nil
}

func (f *fakeConsumer) Reconcile(_ context.Context, origin string) (ReconcileResult, error) {
	f.reconcileOrigin = origin
	return ReconcileResult{Converged: len(origin)}, nil
}

func (f *fakeConsumer) Sync(_ context.Context, origin string) (SyncResult, error) {
	f.syncOrigin = origin
	return SyncResult{Converged: len(origin)}, nil
}

func (f *fakeConsumer) GetState(_ context.Context) (RawRegistry, error) {
	return RawRegistry(stateJSON), nil
}

func serveSocket(t *testing.T, consumer SyncConsumer) Transport {
	t.Helper()
	dir, err := os.MkdirTemp("", "syncsvc")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "rpc.sock")
	listener, err := rpc.Listen(context.Background(), sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dispatcher := rpc.NewDispatcher()
	RegisterConsumer(dispatcher, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rpc.NewServer(dispatcher).Serve(ctx, listener) }()
	transport := Socket(sock)
	t.Cleanup(func() {
		_ = transport.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return transport
}

func TestClientPersistentSessionRoundTrip(t *testing.T) {
	fake := &fakeConsumer{}
	c := NewClient(serveSocket(t, fake))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.ProtocolVersion != ProtocolVersion {
		t.Errorf("protocol version = %d, want %d", caps.ProtocolVersion, ProtocolVersion)
	}
	if caps.Name != "fake" {
		t.Errorf("name = %q, want fake", caps.Name)
	}
	if strings.Join(caps.Methods, ",") != strings.Join(AllMethods, ",") {
		t.Errorf("methods = %v, want %v", caps.Methods, AllMethods)
	}

	items, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []WatchItem{
		{ID: "alpha", WatchDirs: []string{"/a", "/b"}, Fingerprint: "fa"},
		{ID: "beta", WatchDirs: []string{"/c"}, Fingerprint: "fb"},
	}
	if len(items) != len(want) {
		t.Fatalf("list len = %d, want %d", len(items), len(want))
	}
	for i, w := range want {
		got := items[i]
		if got.ID != w.ID || got.Fingerprint != w.Fingerprint || strings.Join(got.WatchDirs, ",") != strings.Join(w.WatchDirs, ",") {
			t.Errorf("item %d = %+v, want %+v", i, got, w)
		}
	}

	rec, err := c.Reconcile(ctx, "h1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fake.reconcileOrigin != "h1" {
		t.Errorf("reconcile origin = %q, want h1", fake.reconcileOrigin)
	}
	if rec.Converged != len("h1") {
		t.Errorf("reconcile converged = %d, want %d", rec.Converged, len("h1"))
	}

	syn, err := c.Sync(ctx, "h2")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if fake.syncOrigin != "h2" {
		t.Errorf("sync origin = %q, want h2", fake.syncOrigin)
	}
	if syn.Converged != len("h2") {
		t.Errorf("sync converged = %d, want %d", syn.Converged, len("h2"))
	}

	state, err := c.GetState(ctx)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if string(state) != stateJSON {
		t.Errorf("get_state = %s, want byte-identical %s", state, stateJSON)
	}
	if !strings.Contains(string(state), "1719273600000000") {
		t.Errorf("get_state %s lost the int64 stamp", state)
	}
	if strings.Contains(string(state), "1.7192736e+15") {
		t.Errorf("get_state %s corrupted the int64 stamp to float64", state)
	}
}

// erroringConsumer is a SyncConsumer whose Reconcile fails, to assert a handler error
// surfaces as a non-nil client error.
type erroringConsumer struct{ fakeConsumer }

func (*erroringConsumer) Reconcile(_ context.Context, _ string) (ReconcileResult, error) {
	return ReconcileResult{}, errBoom
}

var errBoom = boomError("boom")

type boomError string

func (e boomError) Error() string { return string(e) }

func TestHandlerErrorSurfaces(t *testing.T) {
	c := NewClient(serveSocket(t, &erroringConsumer{}))
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Reconcile(context.Background(), "h1")
	if err == nil {
		t.Fatal("want a non-nil error from a failing handler, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %v, want it to mention boom", err)
	}
}

func TestStdioTransportClosedRejectsDo(t *testing.T) {
	// A bogus binary so a Do that slipped past the closed guard would visibly
	// fail at spawn instead; the guard must short-circuit before start().
	tx := Stdio(filepath.Join(t.TempDir(), "never-spawned"))
	if err := tx.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := tx.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
	if err == nil {
		t.Fatal("Do after Close returned nil, want a closed error")
	}
	if !strings.Contains(err.Error(), "transport closed") {
		t.Errorf("error = %v, want it to mention transport closed", err)
	}
}

func TestStdioTransportRealSubprocess(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}

	bin := filepath.Join(t.TempDir(), "rpcstub")
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/rpcstub") //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build rpcstub: %v\n%s", err, out)
	}

	tx := Stdio(bin)
	c := NewClient(tx)
	ctx := context.Background()

	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Name != "stub" {
		t.Errorf("name = %q, want stub", caps.Name)
	}
	if caps.ProtocolVersion != ProtocolVersion {
		t.Errorf("protocol version = %d, want %d", caps.ProtocolVersion, ProtocolVersion)
	}

	items, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(items) != 1 || items[0].ID != "only" || items[0].Fingerprint != "fp" {
		t.Fatalf("list = %+v, want one item id=only fp=fp", items)
	}

	state, err := c.GetState(ctx)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !strings.Contains(string(state), "1719273600000000") {
		t.Errorf("get_state %s lost the int64 stamp byte-exact", state)
	}
	if strings.Contains(string(state), "1.7192736e+15") {
		t.Errorf("get_state %s corrupted the int64 stamp to float64", state)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities}); err == nil {
		t.Fatal("Do after Close returned nil, want a closed error")
	}
}

// TestStdioTransportCtxTimeout drives Do against a child that never answers, so ctx
// expires and reset() tears the child down while the exchange goroutine is still in
// flight; each following Do re-spawns a fresh child. Unfixed, the abandoned goroutine
// read receiver pipe fields reset() had already cleared and crashed the daemon on a
// nil dereference in the field.
func TestStdioTransportCtxTimeout(t *testing.T) {
	noBackoff(t)
	tx := Stdio("sleep", "9999")
	t.Cleanup(func() { _ = tx.Close() })

	for range 25 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
		}
	}
}

// TestStdioTransportResetRaceSoak is a canary for the reset()-vs-exchange race: an
// already-expired ctx plus spinner goroutines starving the scheduler delay exchange
// past reset()'s teardown, which unfixed panicked on a nil pipe field and tripped
// the race detector. The starved interleaving is probabilistic per iteration —
// deterministically green on correct code, while a reintroduced race surfaces
// within a few CI runs rather than every run.
func TestStdioTransportResetRaceSoak(t *testing.T) {
	stop := make(chan struct{})
	for range runtime.GOMAXPROCS(0) * 2 {
		go func() {
			for {
				select {
				case <-stop:
					return
				default: //nolint:staticcheck // SA5004: spinning is the point — starve the scheduler.
				}
			}
		}()
	}
	defer close(stop)

	noBackoff(t)
	tx := Stdio("sleep", "9999")
	t.Cleanup(func() { _ = tx.Close() })

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for range 200 {
		if _, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
		}
	}
}

// noBackoff disables the transport respawn backoff for a test that intentionally
// respawns on every Do (a reset-race soak), restoring it on cleanup.
func noBackoff(t *testing.T) {
	t.Helper()
	prev := transportBackoffBase
	transportBackoffBase = 0
	t.Cleanup(func() { transportBackoffBase = prev })
}

// TestCmdTransportBackoffAfterConsecutiveResets drives a transport whose child exits
// immediately (a framing EOF every Do): the first failure self-heals, a second
// consecutive failure arms the backoff so the third Do fails fast without respawning,
// and a Do after the window spawns again.
func TestCmdTransportBackoffAfterConsecutiveResets(t *testing.T) {
	prevBase, prevMax := transportBackoffBase, transportBackoffMax
	transportBackoffBase = 100 * time.Millisecond
	transportBackoffMax = 400 * time.Millisecond
	t.Cleanup(func() { transportBackoffBase, transportBackoffMax = prevBase, prevMax })

	tr := &cmdTransport{candidates: [][]string{{"false"}}}
	t.Cleanup(func() { _ = tr.Close() })
	ctx := context.Background()
	req := &rpc.Request{Method: MethodCapabilities}

	for i := 1; i <= 2; i++ {
		_, err := tr.Do(ctx, req)
		if err == nil || strings.Contains(err.Error(), "backing off") {
			t.Fatalf("Do #%d = %v, want a framing error, not backoff", i, err)
		}
	}

	_, err := tr.Do(ctx, req)
	if err == nil || !strings.Contains(err.Error(), "backing off") {
		t.Fatalf("Do during backoff = %v, want a fast-fail backoff error", err)
	}

	time.Sleep(transportBackoffMax + 50*time.Millisecond)
	if _, err := tr.Do(ctx, req); err == nil || strings.Contains(err.Error(), "backing off") {
		t.Fatalf("Do after the backoff window = %v, want a fresh spawn's framing error", err)
	}
}

// TestCmdTransportReconnectsNextOperationToWorkingCandidate proves a failed operation is
// never replayed, while the next operation may reconnect through the next address.
func TestCmdTransportReconnectsNextOperationToWorkingCandidate(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), "rpcstub")
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/rpcstub") //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build rpcstub: %v\n%s", err, out)
	}

	tr := &cmdTransport{candidates: [][]string{{"false"}, {bin}}}
	t.Cleanup(func() { _ = tr.Close() })

	c := NewClient(tr)
	if _, err := c.Capabilities(context.Background()); err == nil {
		t.Fatal("Capabilities on dead candidate succeeded, want the first operation returned exactly once")
	}
	if tr.idx != 1 || tr.cmd != nil {
		t.Fatalf("after first operation idx=%d cmd==nil:%v, want candidate 1 selected but not spawned", tr.idx, tr.cmd == nil)
	}

	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities on next operation: %v", err)
	}
	if caps.Name != "stub" {
		t.Fatalf("name = %q, want stub (the second candidate answered)", caps.Name)
	}
	if tr.idx != 1 || tr.cmd == nil {
		t.Fatalf("idx=%d cmd==nil:%v, want a live child pinned to candidate 1", tr.idx, tr.cmd == nil)
	}
}

// buildStub compiles a testdata command and returns the binary path, skipping when go
// is unavailable.
func buildStub(t *testing.T, pkg string) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), filepath.Base(pkg))
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/"+pkg) //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	return bin
}

// TestCmdTransportPreSendFailureAdvancesOnlyNextOperation proves a broken handshake does
// not start another candidate during the failed operation. It only selects that candidate
// for a later operation.
func TestCmdTransportPreSendFailureAdvancesOnlyNextOperation(t *testing.T) {
	partial := buildStub(t, "partialstub")
	canary, spawns := failingSpawnCandidate(t)

	tr := &cmdTransport{candidates: [][]string{{partial}, canary}}
	t.Cleanup(func() { _ = tr.Close() })

	_, err := tr.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
	if err == nil {
		t.Fatal("Do succeeded, want the partial-handshake read error")
	}
	if got := spawns(); got != 0 {
		t.Fatalf("second candidate spawned %d times during the failed operation, want 0", got)
	}
	if tr.idx != 1 {
		t.Fatalf("idx = %d, want 1 selected for the next operation", tr.idx)
	}
}

// TestSSHStdioResolvesAddrsAtSpawnNotConstruction proves SSHStdio defers DialAddrs to
// the first spawn: an address recorded after construction but before the first spawn is
// used, and the tailnet FQDN stays last. Resolved at construction, candidate 0 would be
// the bare peer.
func TestSSHStdioResolvesAddrsAtSpawnNotConstruction(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	peer := "me@node.tail.ts.net"

	tx := SSHStdio(peer, "cookiesync rpc whoami")
	if err := hostregistry.Mesh.AddAddr(context.Background(), peer, "me@node.local"); err != nil {
		t.Fatalf("AddAddr: %v", err)
	}

	tr := tx.(*cmdTransport)
	if err := tr.ensureCandidates(); err != nil {
		t.Fatalf("ensureCandidates: %v", err)
	}
	addrOf := func(argv []string) string { return argv[len(argv)-2] }
	if len(tr.candidates) != 2 {
		t.Fatalf("candidates = %d, want 2 (post-construction LAN addr + tailnet FQDN)", len(tr.candidates))
	}
	if got := addrOf(tr.candidates[0]); got != "me@node.local" {
		t.Fatalf("first candidate dials %q, want the post-construction LAN addr me@node.local", got)
	}
	if got := addrOf(tr.candidates[1]); got != peer {
		t.Fatalf("last candidate dials %q, want the tailnet FQDN %q", got, peer)
	}
}

// TestSSHStdioPoisonedAddrsSurfacesErrorFromDo proves a DialAddrs resolution failure is
// returned by Do rather than masked by a silent [peer] fallback.
func TestSSHStdioPoisonedAddrsSurfacesErrorFromDo(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.UpdateRaw(context.Background(), func(raw map[string]json.RawMessage) error {
		raw["addrs"] = json.RawMessage(`"not-a-map"`)
		return nil
	}); err != nil {
		t.Fatalf("poison addrs: %v", err)
	}

	tx := SSHStdio("me@node.tail.ts.net", "cookiesync rpc whoami")
	t.Cleanup(func() { _ = tx.Close() })

	_, err := tx.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
	if err == nil {
		t.Fatal("Do succeeded, want the DialAddrs decode failure surfaced, not a silent [peer] fallback")
	}
	if !strings.Contains(err.Error(), "dial addresses") {
		t.Fatalf("error = %v, want it to name the dial-address resolution failure", err)
	}
}

// failingSpawnCandidate returns an argv for a child that records each spawn to a log and
// exits non-zero (a framing EOF), plus a counter over that log. Every start() that
// actually spawns appends a line, so the count is the spawn count and a backed-off Do
// (which never spawns) leaves it unchanged.
func failingSpawnCandidate(t *testing.T) ([]string, func() int) {
	t.Helper()
	spawnLog := filepath.Join(t.TempDir(), "spawns.log")
	argv := []string{"sh", "-c", "echo spawn >> " + spawnLog + "; exit 1"}
	count := func() int {
		data, _ := os.ReadFile(spawnLog) //nolint:gosec // G304: test reads its own temp file.
		return strings.Count(string(data), "spawn")
	}
	return argv, count
}

// TestCmdTransportBackoffResetsAfterSuccess proves a successful Do zeroes the
// consecutive-failure counter and clears the armed backoff window, asserting child-spawn
// counts: the working child never runs the failing script.
func TestCmdTransportBackoffResetsAfterSuccess(t *testing.T) {
	prevBase, prevMax := transportBackoffBase, transportBackoffMax
	transportBackoffBase = 50 * time.Millisecond
	transportBackoffMax = 200 * time.Millisecond
	t.Cleanup(func() { transportBackoffBase, transportBackoffMax = prevBase, prevMax })

	failing, spawns := failingSpawnCandidate(t)
	working := buildStub(t, "rpcstub")

	tr := &cmdTransport{candidates: [][]string{failing}}
	t.Cleanup(func() { _ = tr.Close() })
	ctx := context.Background()
	req := &rpc.Request{Method: MethodCapabilities}

	for i := 1; i <= 2; i++ {
		if _, err := tr.Do(ctx, req); err == nil || strings.Contains(err.Error(), "backing off") {
			t.Fatalf("failing Do #%d = %v, want a spawned framing failure", i, err)
		}
	}
	if got := spawns(); got != 2 {
		t.Fatalf("spawns after two failures = %d, want 2", got)
	}
	if tr.resetCount != 2 {
		t.Fatalf("resetCount = %d, want 2 after two consecutive failures", tr.resetCount)
	}

	// Clear the armed window without sleeping, then a success against a working child
	// zeroes the counter and the window.
	tr.resetAt = time.Now().Add(-transportBackoffMax)
	tr.candidates = [][]string{{working}}
	if _, err := tr.Do(ctx, req); err != nil {
		t.Fatalf("Do against working child: %v", err)
	}
	if got := spawns(); got != 2 {
		t.Fatalf("the working child spawned the failing script: spawns = %d, want still 2", got)
	}
	if tr.resetCount != 0 {
		t.Fatalf("resetCount after a success = %d, want 0 (success clears the failure counter)", tr.resetCount)
	}
	if wait := tr.backoffRemaining(); wait != 0 {
		t.Fatalf("backoffRemaining after a success = %s, want 0 (reset clears the window)", wait)
	}
}

// TestCmdTransportBackoffGrowsAndCaps proves the backoff window doubles across
// consecutive failures and clamps at max, asserting child-spawn counts: a Do inside the
// current window spawns nothing, and a spawn returns only once the (growing, capped)
// window has elapsed. resetAt is backdated to advance the window deterministically
// without sleeping.
func TestCmdTransportBackoffGrowsAndCaps(t *testing.T) {
	prevBase, prevMax := transportBackoffBase, transportBackoffMax
	transportBackoffBase = 50 * time.Millisecond
	transportBackoffMax = 120 * time.Millisecond
	t.Cleanup(func() { transportBackoffBase, transportBackoffMax = prevBase, prevMax })

	failing, spawns := failingSpawnCandidate(t)
	tr := &cmdTransport{candidates: [][]string{failing}}
	t.Cleanup(func() { _ = tr.Close() })
	ctx := context.Background()
	req := &rpc.Request{Method: MethodCapabilities}

	fail := func(wantSpawns int) {
		t.Helper()
		if _, err := tr.Do(ctx, req); err == nil || strings.Contains(err.Error(), "backing off") {
			t.Fatalf("Do = %v, want a spawned framing failure", err)
		}
		if got := spawns(); got != wantSpawns {
			t.Fatalf("spawns = %d, want %d after a spawned failure", got, wantSpawns)
		}
	}
	backoff := func(wantSpawns int) {
		t.Helper()
		if _, err := tr.Do(ctx, req); err == nil || !strings.Contains(err.Error(), "backing off") {
			t.Fatalf("Do = %v, want a backoff fast-fail", err)
		}
		if got := spawns(); got != wantSpawns {
			t.Fatalf("a backed-off Do spawned a child: spawns = %d, want %d", got, wantSpawns)
		}
	}
	backdate := func(d time.Duration) { tr.resetAt = time.Now().Add(-d) }

	fail(1) // resetCount 1, window 0
	fail(2) // resetCount 2, window = base (50ms)

	backoff(2) // inside the base window
	backdate(transportBackoffBase + 5*time.Millisecond)
	fail(3) // window elapsed → spawn; resetCount 3, window = 2*base (100ms)

	backdate(transportBackoffBase + 5*time.Millisecond) // 55ms < 100ms
	backoff(3)                                          // still backing off proves the window doubled

	backdate(2*transportBackoffBase + 5*time.Millisecond) // > 100ms
	fail(4)                                               // resetCount 4, window = base*4 clamped to max (120ms)

	// 125ms is past the 120ms cap but short of the uncapped base*4 (200ms), so a spawn
	// here proves the window was clamped rather than still doubling.
	backdate(transportBackoffMax + 5*time.Millisecond)
	fail(5)
}

// TestCmdTransportCtxTimeoutAfterFirstByteNoFailover proves a ctx timeout mid-response is
// terminal for the Do even after the first response byte: the candidate emits one byte
// then hangs, the short ctx expires, and Do returns the ctx error without ever spawning
// the second candidate.
func TestCmdTransportCtxTimeoutAfterFirstByteNoFailover(t *testing.T) {
	noBackoff(t)
	hang := buildStub(t, "hangstub")
	canary, spawns := failingSpawnCandidate(t)

	tr := &cmdTransport{candidates: [][]string{{hang}, canary}}
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := tr.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
	}
	if got := spawns(); got != 0 {
		t.Fatalf("second candidate spawned %d times; a mid-Do ctx timeout must not fail over", got)
	}
	if tr.idx != 0 {
		t.Fatalf("idx = %d, want 0 (no failover on timeout)", tr.idx)
	}
}

// TestCmdTransportDoneCtxTerminalNoSecondCandidate proves an already-cancelled ctx makes
// every Do terminal before spawning either candidate: Do returns the ctx error without
// starting candidate 0 or failing over to the canary candidate 1.
func TestCmdTransportDoneCtxTerminalNoSecondCandidate(t *testing.T) {
	noBackoff(t)
	first, firstSpawns := failingSpawnCandidate(t)
	canary, canarySpawns := failingSpawnCandidate(t)
	tr := &cmdTransport{candidates: [][]string{first, canary}}
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 200 {
		if _, err := tr.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do = %v, want context.Canceled", err)
		}
	}
	if got := firstSpawns(); got != 0 {
		t.Fatalf("first candidate spawned %d times; a done ctx must not start any candidate", got)
	}
	if got := canarySpawns(); got != 0 {
		t.Fatalf("canary spawned %d times; failover past a done ctx must not spawn the next candidate", got)
	}
}

// TestCmdTransportSpawnFailureArmsBackoff proves a spawn failure counts toward the
// consecutive-failure backoff: a candidate whose binary does not exist fails at start()
// every Do, and two such failures arm the window so the third Do fast-fails rather than
// storming respawns.
func TestCmdTransportSpawnFailureArmsBackoff(t *testing.T) {
	prevBase, prevMax := transportBackoffBase, transportBackoffMax
	transportBackoffBase = 100 * time.Millisecond
	transportBackoffMax = 400 * time.Millisecond
	t.Cleanup(func() { transportBackoffBase, transportBackoffMax = prevBase, prevMax })

	missing := filepath.Join(t.TempDir(), "does-not-exist")
	tr := &cmdTransport{candidates: [][]string{{missing}}}
	t.Cleanup(func() { _ = tr.Close() })
	ctx := context.Background()
	req := &rpc.Request{Method: MethodCapabilities}

	for i := 1; i <= 2; i++ {
		_, err := tr.Do(ctx, req)
		if err == nil || strings.Contains(err.Error(), "backing off") {
			t.Fatalf("Do #%d = %v, want a spawn failure, not backoff", i, err)
		}
	}
	if tr.resetCount != 2 {
		t.Fatalf("resetCount = %d, want 2 (two spawn failures armed the backoff)", tr.resetCount)
	}
	if _, err := tr.Do(ctx, req); err == nil || !strings.Contains(err.Error(), "backing off") {
		t.Fatalf("Do during backoff = %v, want a fast-fail backoff error (spawn failures throttle)", err)
	}
}

func TestUnknownMethodReturnsErrorResponse(t *testing.T) {
	tx := serveSocket(t, &fakeConsumer{})
	t.Cleanup(func() { _ = tx.Close() })

	resp, err := tx.Do(context.Background(), &rpc.Request{Method: "svc.bogus"})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.OK {
		t.Fatal("ok = true, want an error response for an unknown method")
	}
	if !strings.Contains(resp.Error, "unknown method") {
		t.Errorf("error = %q, want it to mention unknown method", resp.Error)
	}
}

// TestCmdTransportResetKillsBackgroundedDescendant reproduces the orphan leak: a tunnel
// child backgrounds a descendant that inherits the framing pipes and records its pid, then
// both wedge. A Do timeout drives reset(), which must SIGKILL the whole process group so the
// descendant dies too — a leader-only kill would leave it orphaned.
func TestCmdTransportResetKillsBackgroundedDescendant(t *testing.T) {
	noBackoff(t)
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	script := "sh -c 'echo $$ > " + pidFile + "; exec sleep 30' &\nexec sleep 30\n"
	tr := &cmdTransport{candidates: [][]string{{"/bin/sh", "-c", script}}}
	t.Cleanup(func() { _ = tr.Close() })
	t.Cleanup(func() {
		if pid := readDescendantPID(pidFile); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := tr.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do = %v, want context.DeadlineExceeded (the child never answers)", err)
	}

	pid := 0
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if pid = readDescendantPID(pidFile); pid > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("tunnel child never recorded its descendant pid")
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d still alive; reset must SIGKILL the whole process group", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readDescendantPID reads the pid a tunnel child recorded, or 0 when the file is absent or
// not yet written.
func readDescendantPID(path string) int {
	data, err := os.ReadFile(path) //nolint:gosec // G304: test reads a file its own stub wrote in a temp dir.
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}
