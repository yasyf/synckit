package syncservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/rpc"
)

func TestWithTransportRunnerConcurrentTransports(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatalf("initialize mesh state: %v", err)
	}
	fact, err := hostregistry.NewSSHHostFact("me@peer", "/opt/homebrew/bin/synckitd", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := hostregistry.Mesh.RegisterHost(context.Background(), fact); err != nil {
		t.Fatal(err)
	}
	bin := buildStub(t, "rpcstub")
	fakeBin := t.TempDir()
	ssh := filepath.Join(fakeBin, "ssh")
	if err := os.WriteFile(ssh, []byte("#!/bin/sh\nexec \"$SYNCKIT_TEST_RPC_STUB\"\n"), 0o755); err != nil { //nolint:gosec // executable fixture
		t.Fatalf("write fake ssh: %v", err)
	}
	t.Setenv("SYNCKIT_TEST_RPC_STUB", bin)
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	const count = 8
	err = WithTransportRunner(context.Background(), func(runner TransportRunner) error {
		var wg sync.WaitGroup
		errs := make(chan error, count)
		for i := range count {
			wg.Add(1)
			go func() {
				defer wg.Done()
				transport := runner.Stdio(bin)
				if i%2 == 1 {
					transport = runner.SSHStdio("me@peer", "ignored")
				}
				client := NewClient(transport)
				defer func() { _ = client.Close() }()
				caps, err := client.Capabilities(context.Background())
				if err != nil {
					errs <- err
					return
				}
				if caps.Name != "stub" {
					errs <- fmt.Errorf("worker %d name = %q", i, caps.Name)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTransportRunner: %v", err)
	}
}

func TestWithTransportRunnerKeepsLongGetStateSessionAlive(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("SYNCKIT_TEST_GET_STATE_DELAY", "10s")
	bin := buildStub(t, "rpcstub")
	started := time.Now()
	err := WithTransportRunner(context.Background(), func(runner TransportRunner) error {
		client := NewClient(runner.Stdio(bin))
		defer func() { _ = client.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		state, err := client.GetState(ctx)
		if err != nil {
			return err
		}
		if string(state) != stateJSON {
			return fmt.Errorf("state = %s, want %s", state, stateJSON)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTransportRunner: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Second {
		t.Fatalf("long get_state returned after %s, want at least 10s", elapsed)
	}
}

func TestWithTransportRunnerInvalidatesEscapes(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var runner TransportRunner
	var transport Transport
	if err := WithTransportRunner(context.Background(), func(value TransportRunner) error {
		runner = value
		transport = value.Stdio("true")
		return nil
	}); err != nil {
		t.Fatalf("WithTransportRunner: %v", err)
	}
	if _, err := transport.Do(context.Background(), &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, ErrTransportRunnerClosed) {
		t.Fatalf("escaped transport Do = %v, want ErrTransportRunnerClosed", err)
	}
	escaped := runner.Stdio("true")
	if _, err := escaped.Do(context.Background(), &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, ErrTransportRunnerClosed) {
		t.Fatalf("escaped runner transport Do = %v, want ErrTransportRunnerClosed", err)
	}
}

func TestWithTransportRunnerSettlesInFlightSessionOnError(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	started := make(chan struct{})
	done := make(chan error, 1)
	sentinel := errors.New("callback failed")
	err := WithTransportRunner(context.Background(), func(runner TransportRunner) error {
		transport := runner.Stdio("sh", "-c", "printf x; sleep 9999")
		go func() {
			close(started)
			_, err := transport.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
			done <- err
		}()
		<-started
		time.Sleep(50 * time.Millisecond)
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTransportRunner = %v, want callback error", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("in-flight Do succeeded after scope settlement")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight transport did not settle before scope returned")
	}
}

func TestWithTransportRunnerReleasesOwnerAfterPanic(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("callback panic was not propagated")
			}
		}()
		_ = WithTransportRunner(context.Background(), func(TransportRunner) error {
			panic("boom")
		})
	}()
	if err := WithTransportRunner(context.Background(), func(TransportRunner) error { return nil }); err != nil {
		t.Fatalf("owner remained locked after panic: %v", err)
	}
}

func testSessionPool(t *testing.T) *supervise.Pool {
	t.Helper()
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")},
		Generation: t.Name(),
	}
	pool, err := supervise.NewPool(4, reaper)
	if err != nil {
		t.Fatalf("new process pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		pool.Cancel()
		if err := pool.Wait(context.Background()); err != nil {
			t.Errorf("wait for process pool: %v", err)
		}
	})
	return pool
}

func testCmdTransport(t *testing.T, candidates [][]string) *synctransport.CommandTransport {
	t.Helper()
	return synctransport.NewCandidates(testSessionPool(t), candidates)
}

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
	tx := synctransport.NewStdio(testSessionPool(t), filepath.Join(t.TempDir(), "never-spawned"))
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

	tx := synctransport.NewStdio(testSessionPool(t), bin)
	c := NewClient(tx)
	ctx := context.Background()

	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Name != "stub" {
		t.Errorf("name = %q, want stub", caps.Name)
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
	tx := synctransport.NewStdio(testSessionPool(t), "sleep", "9999")
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
	tx := synctransport.NewStdio(testSessionPool(t), "sleep", "9999")
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
	t.Cleanup(synctransport.SetBackoff(0, 0))
}

// TestCmdTransportBackoffAfterConsecutiveResets drives a transport whose child exits
// immediately (a framing EOF every Do): the first failure self-heals, a second
// consecutive failure arms the backoff so the third Do fails fast without respawning,
// and a Do after the window spawns again.
func TestCmdTransportBackoffAfterConsecutiveResets(t *testing.T) {
	const backoffMax = 400 * time.Millisecond
	t.Cleanup(synctransport.SetBackoff(100*time.Millisecond, backoffMax))

	tr := testCmdTransport(t, [][]string{{"false"}})
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

	time.Sleep(backoffMax + 50*time.Millisecond)
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

	tr := testCmdTransport(t, [][]string{{"false"}, {bin}})
	t.Cleanup(func() { _ = tr.Close() })

	c := NewClient(tr)
	if _, err := c.Capabilities(context.Background()); err == nil {
		t.Fatal("Capabilities on dead candidate succeeded, want the first operation returned exactly once")
	}
	if idx, active := tr.State(); idx != 1 || active {
		t.Fatalf("after first operation idx=%d active=%v, want candidate 1 selected but not spawned", idx, active)
	}

	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities on next operation: %v", err)
	}
	if caps.Name != "stub" {
		t.Fatalf("name = %q, want stub (the second candidate answered)", caps.Name)
	}
	if idx, active := tr.State(); idx != 1 || !active {
		t.Fatalf("idx=%d active=%v, want a live child pinned to candidate 1", idx, active)
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

	tr := testCmdTransport(t, [][]string{{partial}, canary})
	t.Cleanup(func() { _ = tr.Close() })

	_, err := tr.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
	if err == nil {
		t.Fatal("Do succeeded, want the partial-handshake read error")
	}
	if got := spawns(); got != 0 {
		t.Fatalf("second candidate spawned %d times during the failed operation, want 0", got)
	}
	if idx, _ := tr.State(); idx != 1 {
		t.Fatalf("idx = %d, want 1 selected for the next operation", idx)
	}
}

// TestSSHStdioResolvesAddrsAtSpawnNotConstruction proves SSHStdio defers DialAddrs to
// the first spawn: an address recorded after construction but before the first spawn is
// used, and the tailnet FQDN stays last. Resolved at construction, candidate 0 would be
// the bare peer.
func TestSSHStdioResolvesAddrsAtSpawnNotConstruction(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := "me@node.tail.ts.net"

	tx := synctransport.NewSSHStdio(testSessionPool(t), peer, "cookiesync rpc whoami")
	fact, err := hostregistry.NewSSHHostFact(peer, "/opt/homebrew/bin/synckitd", []string{"me@node.local"})
	if err != nil {
		t.Fatal(err)
	}
	if err := hostregistry.Mesh.RegisterHost(context.Background(), fact); err != nil {
		t.Fatalf("RegisterHost: %v", err)
	}

	tr := tx.(*synctransport.CommandTransport)
	if err := tr.EnsureCandidates(); err != nil {
		t.Fatalf("ensureCandidates: %v", err)
	}
	candidates := tr.Candidates()
	addrOf := func(argv []string) string { return argv[len(argv)-2] }
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want 2 (post-construction LAN addr + tailnet FQDN)", len(candidates))
	}
	if got := addrOf(candidates[0]); got != "me@node.local" {
		t.Fatalf("first candidate dials %q, want the post-construction LAN addr me@node.local", got)
	}
	if got := addrOf(candidates[1]); got != peer {
		t.Fatalf("last candidate dials %q, want the tailnet FQDN %q", got, peer)
	}
}

// TestSSHStdioPoisonedAddrsSurfacesErrorFromDo proves a DialAddrs resolution failure is
// returned by Do rather than masked by a silent [peer] fallback.
func TestSSHStdioPoisonedAddrsSurfacesErrorFromDo(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := hostregistry.Mesh.InitializeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, err := hostregistry.Mesh.Path()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // temp state path from the fixed Mesh Config
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"hosts":[]`), []byte(`"hosts":"not-an-array"`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil { //nolint:gosec // temp state path from the fixed Mesh Config
		t.Fatal(err)
	}

	tx := synctransport.NewSSHStdio(testSessionPool(t), "me@node.tail.ts.net", "cookiesync rpc whoami")
	t.Cleanup(func() { _ = tx.Close() })

	_, err = tx.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
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
	const backoffMax = 200 * time.Millisecond
	t.Cleanup(synctransport.SetBackoff(50*time.Millisecond, backoffMax))

	failing, spawns := failingSpawnCandidate(t)
	working := buildStub(t, "rpcstub")

	tr := testCmdTransport(t, [][]string{failing})
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
	if count, _ := tr.FailureState(); count != 2 {
		t.Fatalf("resetCount = %d, want 2 after two consecutive failures", count)
	}

	// Clear the armed window without sleeping, then a success against a working child
	// zeroes the counter and the window.
	tr.BackdateReset(backoffMax)
	tr.ReplaceCandidates([][]string{{working}})
	if _, err := tr.Do(ctx, req); err != nil {
		t.Fatalf("Do against working child: %v", err)
	}
	if got := spawns(); got != 2 {
		t.Fatalf("the working child spawned the failing script: spawns = %d, want still 2", got)
	}
	if count, wait := tr.FailureState(); count != 0 {
		t.Fatalf("resetCount after a success = %d, want 0 (success clears the failure counter)", count)
	} else if wait != 0 {
		t.Fatalf("backoffRemaining after a success = %s, want 0 (reset clears the window)", wait)
	}
}

// TestCmdTransportBackoffGrowsAndCaps proves the backoff window doubles across
// consecutive failures and clamps at max, asserting child-spawn counts: a Do inside the
// current window spawns nothing, and a spawn returns only once the (growing, capped)
// window has elapsed. resetAt is backdated to advance the window deterministically
// without sleeping.
func TestCmdTransportBackoffGrowsAndCaps(t *testing.T) {
	const (
		backoffBase = 50 * time.Millisecond
		backoffMax  = 120 * time.Millisecond
	)
	t.Cleanup(synctransport.SetBackoff(backoffBase, backoffMax))

	failing, spawns := failingSpawnCandidate(t)
	tr := testCmdTransport(t, [][]string{failing})
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
	backdate := tr.BackdateReset

	fail(1) // resetCount 1, window 0
	fail(2) // resetCount 2, window = base (50ms)

	backoff(2) // inside the base window
	backdate(backoffBase + 5*time.Millisecond)
	fail(3) // window elapsed → spawn; resetCount 3, window = 2*base (100ms)

	backdate(backoffBase + 5*time.Millisecond) // 55ms < 100ms
	backoff(3)                                 // still backing off proves the window doubled

	backdate(2*backoffBase + 5*time.Millisecond) // > 100ms
	fail(4)                                      // resetCount 4, window = base*4 clamped to max (120ms)

	// 125ms is past the 120ms cap but short of the uncapped base*4 (200ms), so a spawn
	// here proves the window was clamped rather than still doubling.
	backdate(backoffMax + 5*time.Millisecond)
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

	tr := testCmdTransport(t, [][]string{{hang}, canary})
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := tr.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
	}
	if got := spawns(); got != 0 {
		t.Fatalf("second candidate spawned %d times; a mid-Do ctx timeout must not fail over", got)
	}
	if idx, _ := tr.State(); idx != 0 {
		t.Fatalf("idx = %d, want 0 (no failover on timeout)", idx)
	}
}

// TestCmdTransportDoneCtxTerminalNoSecondCandidate proves an already-cancelled ctx makes
// every Do terminal before spawning either candidate: Do returns the ctx error without
// starting candidate 0 or failing over to the canary candidate 1.
func TestCmdTransportDoneCtxTerminalNoSecondCandidate(t *testing.T) {
	noBackoff(t)
	first, firstSpawns := failingSpawnCandidate(t)
	canary, canarySpawns := failingSpawnCandidate(t)
	tr := testCmdTransport(t, [][]string{first, canary})
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
	t.Cleanup(synctransport.SetBackoff(100*time.Millisecond, 400*time.Millisecond))

	missing := filepath.Join(t.TempDir(), "does-not-exist")
	tr := testCmdTransport(t, [][]string{{missing}})
	t.Cleanup(func() { _ = tr.Close() })
	ctx := context.Background()
	req := &rpc.Request{Method: MethodCapabilities}

	for i := 1; i <= 2; i++ {
		_, err := tr.Do(ctx, req)
		if err == nil || strings.Contains(err.Error(), "backing off") {
			t.Fatalf("Do #%d = %v, want a spawn failure, not backoff", i, err)
		}
	}
	if count, _ := tr.FailureState(); count != 2 {
		t.Fatalf("resetCount = %d, want 2 (two spawn failures armed the backoff)", count)
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
// both wedge. A bridge failure drives reset(), which must settle the whole daemonkit process
// session so the descendant dies too — leader-only ownership would leave it orphaned.
func TestCmdTransportResetKillsBackgroundedDescendant(t *testing.T) {
	noBackoff(t)
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "descendant.pid")
	releaseFile := filepath.Join(dir, "release")
	cleanupFile := filepath.Join(dir, "cleanup")
	script := "sh -c 'trap \"\" TERM; while [ ! -e \"" + cleanupFile + "\" ]; do sleep 0.01; done' &\n" +
		"echo $! > \"" + pidFile + "\"\n" +
		"while [ ! -e \"" + releaseFile + "\" ]; do sleep 0.01; done\n" +
		"exit 1\n"
	tr := testCmdTransport(t, [][]string{{"/bin/sh", "-c", script}})
	t.Cleanup(func() { _ = tr.Close() })
	t.Cleanup(func() { _ = os.WriteFile(releaseFile, nil, 0o600) })
	t.Cleanup(func() { _ = os.WriteFile(cleanupFile, nil, 0o600) })

	done := make(chan error, 1)
	go func() {
		_, err := tr.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
		done <- err
	}()
	pid := waitDescendantPID(t, pidFile)
	identity, err := proc.Probe(pid)
	if err != nil {
		t.Fatalf("probe descendant identity: %v", err)
	}
	record := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           identity.PID,
		StartTime:     identity.StartTime,
		Boot:          identity.Boot,
		Comm:          identity.Comm,
		Generation:    t.Name(),
	}
	if err := os.WriteFile(releaseFile, nil, 0o600); err != nil {
		t.Fatalf("release bridge: %v", err)
	}
	err = nil
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Do did not settle after bridge exit")
	}
	if err == nil {
		t.Fatal("Do succeeded after bridge exit")
	}

	reaper := &proc.Reaper{}
	for deadline := time.Now().Add(10 * time.Second); ; {
		owned, ownsErr := reaper.Owns(record)
		if ownsErr != nil {
			t.Fatalf("revalidate descendant identity: %v", ownsErr)
		}
		if !owned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant identity %#v still alive; reset must settle the whole process session", record)
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

func waitDescendantPID(t *testing.T, path string) int {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if pid := readDescendantPID(path); pid > 0 {
			return pid
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("tunnel child never recorded its descendant pid")
	return 0
}
