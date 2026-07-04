package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// fakeConsumer is an in-process SyncConsumer that records what it is asked so a
// test can assert origin propagation, version handling, and the typed list path.
// It is safe for concurrent use: the engine's resolver and notifier run on
// separate goroutines.
type fakeConsumer struct {
	mu sync.Mutex

	capsVersion int                     // protocol version Capabilities reports
	capsFails   int                     // fail the first N Capabilities calls
	capsCalls   int                     // total Capabilities calls seen
	items       []syncservice.WatchItem // List result
	listCalls   int                     // total List calls seen

	lastReconcileOrigin string
	reconcileCalls      int
	lastSyncOrigin      string
	syncCalls           int
}

func newFakeConsumer(items ...syncservice.WatchItem) *fakeConsumer {
	return &fakeConsumer{capsVersion: syncservice.ProtocolVersion, items: items}
}

func (f *fakeConsumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capsCalls++
	if f.capsCalls <= f.capsFails {
		return syncservice.Capabilities{}, errFakeDown
	}
	caps := syncservice.DefaultCapabilities("fake")
	caps.ProtocolVersion = f.capsVersion
	return caps, nil
}

func (f *fakeConsumer) List(context.Context) ([]syncservice.WatchItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	return append([]syncservice.WatchItem(nil), f.items...), nil
}

func (f *fakeConsumer) Reconcile(_ context.Context, origin string) (syncservice.ReconcileResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReconcileOrigin = origin
	f.reconcileCalls++
	return syncservice.ReconcileResult{Converged: 1}, nil
}

func (f *fakeConsumer) Sync(_ context.Context, origin string) (syncservice.SyncResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSyncOrigin = origin
	f.syncCalls++
	return syncservice.SyncResult{Converged: 1}, nil
}

func (f *fakeConsumer) GetState(context.Context) (syncservice.RawRegistry, error) {
	return syncservice.RawRegistry(`{}`), nil
}

func (f *fakeConsumer) syncOrigin() (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSyncOrigin, f.syncCalls
}

func (f *fakeConsumer) reconcileOrigin() (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastReconcileOrigin, f.reconcileCalls
}

func (f *fakeConsumer) capabilityCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.capsCalls
}

func (f *fakeConsumer) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

type errString string

func (e errString) Error() string { return string(e) }

const errFakeDown = errString("fake consumer down")

// pipeTransport frames typed rpc over one end of a net.Pipe to a fakeConsumer
// served by rpc.ServeConn on the other end. It mirrors the real cmdTransport wire
// (one json request line in, one response line out) without spawning a process.
type pipeTransport struct {
	conn net.Conn
	r    *bufio.Reader
	done <-chan struct{}
}

func (t *pipeTransport) Do(_ context.Context, req *rpc.Request) (*syncservice.Response, error) {
	line, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := t.conn.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	respLine, err := rpc.ReadLine(t.r, rpc.MaxLine)
	if err != nil {
		return nil, err
	}
	var resp syncservice.Response
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *pipeTransport) Close() error {
	err := t.conn.Close()
	if t.done != nil {
		<-t.done
	}
	return err
}

// serveFake wires fake behind an in-process transport: an rpc.ServeConn loop over
// one end of a net.Pipe, the pipeTransport over the other. The server goroutine is
// reaped when the transport is closed or the test cleans up.
func serveFake(t *testing.T, fake *fakeConsumer) syncservice.Transport {
	t.Helper()
	server, client := net.Pipe()
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, fake)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rpc.ServeConn(ctx, server, d)
	}()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})
	return &pipeTransport{conn: client, r: bufio.NewReader(client), done: done}
}

// fakeMesh routes a manifest+peer to a fakeConsumer's transport, so a test can give
// the self branch and a peer branch distinct consumers and assert which one was
// driven and with what origin.
func fakeMesh(t *testing.T, byPeer map[string]*fakeConsumer) {
	t.Helper()
	prev := dialTransport
	dialTransport = func(_ manifest.Manifest, peer, _ string) syncservice.Transport {
		fake, ok := byPeer[peer]
		if !ok {
			t.Fatalf("dialTransport: no fake consumer for peer %q", peer)
		}
		return serveFake(t, fake)
	}
	t.Cleanup(func() { dialTransport = prev })
}

func testManifest() manifest.Manifest {
	return manifest.Manifest{
		Name:   "stub",
		Binary: "stub",
		Watch: manifest.WatchSpec{
			Backend:  "fsnotify",
			Debounce: codec.Duration(10 * time.Millisecond),
		},
		Service: manifest.ServiceSpec{
			Transport: "stdio",
			ServeArgs: []string{"rpc-serve"},
		},
	}
}

func TestManifestResolver(t *testing.T) {
	fake := newFakeConsumer(
		syncservice.WatchItem{ID: "site-a", Fingerprint: "fp-a"},
		syncservice.WatchItem{ID: "site-b", Fingerprint: "fp-b"},
	)
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })
	r := manifestResolver{client: syncservice.NewClient(tx), name: "stub", memo: newFingerprintMemo()}

	tests := []struct {
		name string
		id   string
		want string
	}{
		{"known a", "site-a", "fp-a"},
		{"known b", "site-b", "fp-b"},
		{"missing", "site-z", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Resolve(context.Background(), tt.id)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got != tt.want {
				t.Errorf("Resolve(%q) = %q, want %q", tt.id, got, tt.want)
			}
		})
	}
}

func TestManifestGate(t *testing.T) {
	fake := newFakeConsumer(
		syncservice.WatchItem{ID: "site-a", Fingerprint: "fp-a", Busy: true, BusyReason: "op in progress"},
		syncservice.WatchItem{ID: "site-b", Fingerprint: "fp-b"},
	)
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })
	g := manifestGate{client: syncservice.NewClient(tx), name: "stub", memo: newFingerprintMemo()}

	tests := []struct {
		name       string
		id         string
		wantBusy   bool
		wantReason string
	}{
		{"busy item", "site-a", true, "op in progress"},
		{"idle item", "site-b", false, ""},
		{"missing item", "site-z", false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			busy, reason, err := g.Busy(context.Background(), tt.id)
			if err != nil {
				t.Fatalf("Busy: %v", err)
			}
			if busy != tt.wantBusy || reason != tt.wantReason {
				t.Errorf("Busy(%q) = (%v, %q), want (%v, %q)", tt.id, busy, reason, tt.wantBusy, tt.wantReason)
			}
		})
	}
}

func TestManifestGateFeedsResolverOneList(t *testing.T) {
	fake := newFakeConsumer(syncservice.WatchItem{ID: "site-a", Fingerprint: "fp-a"})
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })
	client := syncservice.NewClient(tx)
	memo := newFingerprintMemo()
	g := manifestGate{client: client, name: "stub", memo: memo}
	r := manifestResolver{client: client, name: "stub", memo: memo}
	ctx := context.Background()

	busy, _, err := g.Busy(ctx, "site-a")
	if err != nil {
		t.Fatalf("Busy: %v", err)
	}
	if busy {
		t.Fatal("Busy(site-a) = true, want false")
	}
	got, err := r.Resolve(ctx, "site-a")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "fp-a" {
		t.Errorf("Resolve(site-a) = %q, want fp-a", got)
	}
	if calls := fake.listCount(); calls != 1 {
		t.Errorf("list calls = %d, want 1 (the resolver must consume the gate's round trip)", calls)
	}

	// The stash is single-use: a resolve outside a gated evaluation lists again.
	if _, err := r.Resolve(ctx, "site-a"); err != nil {
		t.Fatalf("Resolve again: %v", err)
	}
	if calls := fake.listCount(); calls != 2 {
		t.Errorf("list calls = %d, want 2 (a consumed stash must not serve a later evaluation)", calls)
	}
}

func TestManifestNotifierLocal(t *testing.T) {
	fake := newFakeConsumer()
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })
	n := manifestNotifier{local: syncservice.NewClient(tx), m: testManifest(), self: "me@self"}

	if err := n.Notify(context.Background(), "me@self", "site-a"); err != nil {
		t.Fatalf("Notify local: %v", err)
	}
	origin, calls := fake.syncOrigin()
	if calls != 1 {
		t.Fatalf("local sync calls = %d, want 1", calls)
	}
	// Anti-echo: a local notify syncs with an empty origin.
	if origin != "" {
		t.Errorf("local sync origin = %q, want empty", origin)
	}
}

func TestManifestNotifierPeer(t *testing.T) {
	self := newFakeConsumer()
	peer := newFakeConsumer()
	fakeMesh(t, map[string]*fakeConsumer{"me@self": self, "peer@node": peer})

	n := manifestNotifier{local: syncservice.NewClient(serveFake(t, self)), m: testManifest(), self: "me@self"}
	if err := n.Notify(context.Background(), "peer@node", "site-a"); err != nil {
		t.Fatalf("Notify peer: %v", err)
	}

	origin, calls := peer.syncOrigin()
	if calls != 1 {
		t.Fatalf("peer sync calls = %d, want 1", calls)
	}
	// Anti-echo: a peer notify syncs with origin=self so the peer skips notifying back.
	if origin != "me@self" {
		t.Errorf("peer sync origin = %q, want me@self", origin)
	}
	if _, selfCalls := self.syncOrigin(); selfCalls != 0 {
		t.Errorf("self consumer saw %d syncs on a peer notify, want 0", selfCalls)
	}
}

func TestEngineEventDrivesLocalSync(t *testing.T) {
	fake := newFakeConsumer(syncservice.WatchItem{ID: "site-a", Fingerprint: "fp-a"})
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	local := syncservice.NewClient(serveFake(t, fake))
	eng := buildEngine(context.Background(), local, testManifest(), &hostregistry.Registry{Self: "me@self"})

	ctx := context.Background()
	eng.OnEvent(ctx, "site-a")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, calls := fake.syncOrigin(); calls > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	origin, calls := fake.syncOrigin()
	if calls != 1 {
		t.Fatalf("event drove %d local syncs, want 1", calls)
	}
	if origin != "" {
		t.Errorf("event-driven local sync origin = %q, want empty", origin)
	}
}

func TestReloadRPCGenerationOutlivesRequest(t *testing.T) {
	cfgHome, err := os.MkdirTemp("", "skd")
	if err != nil {
		t.Fatalf("mkdir config home: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(cfgHome) })
	t.Setenv("XDG_CONFIG_HOME", cfgHome)

	if _, err := hostregistry.Mesh.Update(context.Background(), func(g *hostregistry.Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}

	watched := t.TempDir()
	fake := newFakeConsumer(syncservice.WatchItem{ID: "only", WatchDirs: []string{watched}, Fingerprint: "fp-1"})
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	manifestsDir, err := ensureManifestsDir()
	if err != nil {
		t.Fatalf("ensure manifests dir: %v", err)
	}
	raw, err := json.Marshal(testManifest())
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(manifestsDir, "stub.json"), raw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("serve did not stop after ctx cancel")
		}
	})

	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		t.Fatalf("sock path: %v", err)
	}
	resp := callReload(t, sock)
	if !resp.OK {
		t.Fatalf("reload rpc: %s", resp.Error)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, calls := fake.syncOrigin(); calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no fs event drove a sync after the reload rpc returned: the watch generation died with the request ctx")
		}
		if err := os.WriteFile(filepath.Join(watched, "touch"), []byte(time.Now().String()), 0o600); err != nil {
			t.Fatalf("touch watched dir: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if origin, _ := fake.syncOrigin(); origin != "" {
		t.Errorf("event-driven local sync origin = %q, want empty", origin)
	}
}

// callReload invokes the reload rpc, retrying dial failures until the daemon has
// bound its socket.
func callReload(t *testing.T, sock string) *rpc.Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := rpc.Call(ctx, sock, &rpc.Request{Method: "reload"})
		cancel()
		if err == nil {
			return resp
		}
		if time.Now().After(deadline) {
			t.Fatalf("reload rpc never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestListForEngineRetriesThenSucceeds(t *testing.T) {
	shrinkBackoff(t)
	fake := newFakeConsumer(syncservice.WatchItem{ID: "only", WatchDirs: []string{"/d"}, Fingerprint: "fp"})
	fake.capsFails = 2 // fail the first two Capabilities calls, then succeed
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })

	items, err := listForEngine(context.Background(), syncservice.NewClient(tx), "stub")
	if err != nil {
		t.Fatalf("listForEngine: %v", err)
	}
	if len(items) != 1 || items[0].ID != "only" {
		t.Fatalf("items = %+v, want one item id=only", items)
	}
	if calls := fake.capabilityCalls(); calls != 3 {
		t.Errorf("capability calls = %d, want 3 (two failures then success)", calls)
	}
}

func TestListForEngineVersionSkewFailsLoud(t *testing.T) {
	shrinkBackoff(t)
	fake := newFakeConsumer()
	fake.capsVersion = 999
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })

	done := make(chan error, 1)
	go func() {
		_, err := listForEngine(context.Background(), syncservice.NewClient(tx), "stub")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("listForEngine on a version skew returned nil, want a loud error")
		}
		if !strings.Contains(err.Error(), "protocol skew") || !strings.Contains(err.Error(), "999") {
			t.Errorf("error = %v, want it to name the skew and version 999", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listForEngine on a version skew hung instead of failing fast")
	}
	if calls := fake.capabilityCalls(); calls != 1 {
		t.Errorf("capability calls = %d, want 1 (a skew must not retry)", calls)
	}
}

func TestListForEngineCtxCancel(t *testing.T) {
	shrinkBackoff(t)
	fake := newFakeConsumer()
	fake.capsFails = 1 << 30 // never succeeds
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := listForEngine(ctx, syncservice.NewClient(tx), "stub")
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("listForEngine returned nil after ctx cancel, want ctx.Err")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listForEngine ignored ctx cancel and hung")
	}
}

// shrinkBackoff shrinks the engine-startup retry knobs so a retry test runs fast,
// restoring them on cleanup.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	prevBackoff, prevBudget := listBackoff, listRetryBudget
	listBackoff = time.Millisecond
	listRetryBudget = 5 * time.Second
	t.Cleanup(func() {
		listBackoff = prevBackoff
		listRetryBudget = prevBudget
	})
}
