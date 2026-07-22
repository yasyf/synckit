package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func TestRuntimeRPCServerProtectsLifecycleCapacity(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	const build = "v9.8.7-test"
	server, err := runtimeRPCServer(rpc.NewDispatcher(), executable, build)
	if err != nil {
		t.Fatalf("runtimeRPCServer: %v", err)
	}
	if server.Wire.Build != rpc.Build {
		t.Fatalf("business build = %q, want %q", server.Wire.Build, rpc.Build)
	}
	if server.Wire.LifecycleBuild != build {
		t.Fatalf("lifecycle build = %q, want %q", server.Wire.LifecycleBuild, build)
	}
	if server.Wire.ReservedProtectedSessions != 1 {
		t.Fatalf("reserved protected sessions = %d, want 1", server.Wire.ReservedProtectedSessions)
	}
	role, ok := server.Wire.ProtectedSessionClassifier.(daemonrole.Classifier)
	if !ok {
		t.Fatalf("protected classifier = %T, want daemonrole.Classifier", server.Wire.ProtectedSessionClassifier)
	}
	if role.RoleID != labelPrefix+".serve" || role.RolePath != executable {
		t.Fatalf("role = %+v", role)
	}
}

func TestServePublishesReleaseBuildAfterActivation(t *testing.T) {
	shortConfigHome(t)
	testDaemonRoleAlias(t)
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		t.Fatalf("socket path: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	const build = "v9.8.7-test"
	go func() { served <- serve(ctx, build) }()

	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Build: rpc.Build, LifecycleBuild: build, MaxFrame: rpc.MaxFrame,
	}}
	t.Cleanup(func() { _ = peer.Close() })
	deadline := time.Now().Add(5 * time.Second)
	var health dkdaemon.Health
	for {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		health, err = peer.Health(probeCtx)
		probeCancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("daemon did not publish lifecycle readiness: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if health.Build != build || health.Protocol != int(wire.ProtocolVersion) || health.State != dkdaemon.StateHealthy {
		t.Fatalf("health = %+v", health)
	}
	dir, err := manifestsDir()
	if err != nil {
		t.Fatalf("manifests dir: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("activation did not create manifests dir: info=%v err=%v", info, err)
	}

	_ = peer.Close()
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not settle after cancellation")
	}
}

func testDaemonRoleAlias(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	dir := t.TempDir()
	alias := filepath.Join(dir, daemonBinary)
	if err := os.Symlink(executable, alias); err != nil {
		t.Fatalf("link daemon role: %v", err)
	}
	t.Setenv("PATH", dir)
	return alias
}

// fakeConsumer is an in-process SyncConsumer that records what it is asked so a
// test can assert origin propagation and the typed list path.
// It is safe for concurrent use: the engine's resolver and notifier run on
// separate goroutines.
type fakeConsumer struct {
	mu sync.Mutex

	items     []syncservice.WatchItem // List result
	listFails int                     // fail the first N List calls
	listCalls int                     // total List calls seen

	lastReconcileOrigin string
	reconcileCalls      int
	lastSyncOrigin      string
	syncCalls           int
}

func newFakeConsumer(items ...syncservice.WatchItem) *fakeConsumer {
	return &fakeConsumer{items: items}
}

func (f *fakeConsumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("fake"), nil
}

func (f *fakeConsumer) List(context.Context) ([]syncservice.WatchItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.listCalls <= f.listFails {
		return nil, errFakeDown
	}
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

func (f *fakeConsumer) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

type errString string

func (e errString) Error() string { return string(e) }

const errFakeDown = errString("fake consumer down")

type servedTransport struct {
	syncservice.Transport
	cancel context.CancelFunc
	done   <-chan error
	once   sync.Once
}

func (t *servedTransport) Close() error {
	var err error
	t.once.Do(func() {
		err = t.Transport.Close()
		t.cancel()
		if serveErr := <-t.done; serveErr != nil {
			err = errors.Join(err, serveErr)
		}
	})
	return err
}

// serveFake wires fake behind an exact persistent Unix session.
func serveFake(t *testing.T, fake *fakeConsumer) syncservice.Transport {
	t.Helper()
	dir, err := os.MkdirTemp("", "syncfake")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "rpc.sock")
	listener, err := rpc.Listen(context.Background(), sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, fake)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rpc.NewServer(d).Serve(ctx, listener) }()
	transport := &servedTransport{Transport: syncservice.Socket(sock), cancel: cancel, done: done}
	t.Cleanup(func() { _ = transport.Close() })
	return transport
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
	testDaemonRoleAlias(t)
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
	go func() { served <- serve(ctx, "v1.0.0-test") }()
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

// countingTransport is a Transport that records how many times it was closed, so a
// test can assert the supervisor closes every transport it builds on teardown. Do
// always fails, so the watch goroutine driving it never blocks on a real exchange.
type countingTransport struct {
	mu      sync.Mutex
	closed  bool
	onClose func()
}

func (*countingTransport) Do(context.Context, *rpc.Request) (*syncservice.Response, error) {
	return nil, errFakeDown
}

func (t *countingTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		t.onClose()
	}
	return nil
}

func (t *countingTransport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// TestReloadClosesEveryTransport proves generation teardown closes every transport the
// supervisor built: a reload tears down and closes the prior generation's clients, and
// stop closes the current one, so no ssh tunnel or child is leaked across a rebind.
func TestReloadClosesEveryTransport(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if _, err := hostregistry.Mesh.Update(context.Background(), func(g *hostregistry.Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatalf("seed mesh: %v", err)
	}

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

	var (
		mu         sync.Mutex
		built      int
		closed     int
		transports []*countingTransport
	)
	prev := dialTransport
	dialTransport = func(manifest.Manifest, string, string) syncservice.Transport {
		transport := &countingTransport{onClose: func() {
			mu.Lock()
			closed++
			mu.Unlock()
		}}
		mu.Lock()
		built++
		transports = append(transports, transport)
		mu.Unlock()
		return transport
	}
	t.Cleanup(func() { dialTransport = prev })

	sup := newSupervisor()
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(sup.stop) }
	t.Cleanup(stop)
	ctx := context.Background()
	if err := sup.reload(ctx); err != nil {
		t.Fatalf("reload 1: %v", err)
	}
	mu.Lock()
	generation1 := append([]*countingTransport(nil), transports...)
	mu.Unlock()
	if err := sup.reload(ctx); err != nil {
		t.Fatalf("reload 2: %v", err)
	}
	for i, transport := range generation1 {
		if !transport.isClosed() {
			t.Fatalf("generation 1 transport %d was not closed by reload", i)
		}
	}
	stop()

	mu.Lock()
	defer mu.Unlock()
	if built == 0 {
		t.Fatal("no transports were built")
	}
	if closed != built {
		t.Fatalf("closed %d of %d transports; generation teardown must close every one", closed, built)
	}
}

// callReload invokes the reload rpc, retrying dial failures until the daemon has
// bound its socket.
func callReload(t *testing.T, sock string) *rpc.Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client := daemonClient(sock)
		resp, err := client.Call(ctx, &rpc.Request{Method: "reload"})
		_ = client.Close()
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
	fake.listFails = 2
	tx := serveFake(t, fake)
	t.Cleanup(func() { _ = tx.Close() })

	items, err := listForEngine(context.Background(), syncservice.NewClient(tx), "stub")
	if err != nil {
		t.Fatalf("listForEngine: %v", err)
	}
	if len(items) != 1 || items[0].ID != "only" {
		t.Fatalf("items = %+v, want one item id=only", items)
	}
	if calls := fake.listCount(); calls != 3 {
		t.Errorf("list calls = %d, want 3 (two failures then success)", calls)
	}
}

func TestListForEngineCtxCancel(t *testing.T) {
	shrinkBackoff(t)
	fake := newFakeConsumer()
	fake.listFails = 1 << 30
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

func TestBackoffAfter(t *testing.T) {
	tests := []struct {
		name    string
		prev    time.Duration
		healthy bool
		want    time.Duration
	}{
		{"first fast failure waits base", 0, false, watchBackoffBase},
		{"fast failure doubles", watchBackoffBase, false, 2 * watchBackoffBase},
		{"fast failure doubles below cap", 40 * time.Second, false, 80 * time.Second},
		{"fast failure clamps to cap", 80 * time.Second, false, watchBackoffMax},
		{"healthy run resets to base", watchBackoffMax, true, watchBackoffBase},
		{"healthy run from zero is base", 0, true, watchBackoffBase},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := backoffAfter(tt.prev, tt.healthy); got != tt.want {
				t.Errorf("backoffAfter(%v, %v) = %v, want %v", tt.prev, tt.healthy, got, tt.want)
			}
		})
	}
}

func TestSuperviseWatchRestartsWithBackoff(t *testing.T) {
	prevBase, prevMax, prevHealthy := watchBackoffBase, watchBackoffMax, watchHealthyRun
	watchBackoffBase = 20 * time.Millisecond
	watchBackoffMax = 1 * time.Second
	watchHealthyRun = time.Hour // the always-failing backend is never "healthy"
	t.Cleanup(func() {
		watchBackoffBase, watchBackoffMax, watchHealthyRun = prevBase, prevMax, prevHealthy
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var at []time.Time

	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseWatch(ctx, "stub", func(context.Context) (time.Duration, error) {
			mu.Lock()
			at = append(at, time.Now())
			n := len(at)
			mu.Unlock()
			if n >= 5 {
				cancel() // ctx cancel must escape the supervisor promptly
			}
			return 0, errFakeDown
		})
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("superviseWatch did not return promptly after ctx cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(at) < 4 {
		t.Fatalf("supervise made %d attempts, want the backend restarted several times", len(at))
	}
	// TestBackoffAfter covers the exact doubling and cap; here we assert only that
	// the gaps grow — a later restart waits strictly longer than the first — proving
	// the backoff is wired into the supervisor loop.
	first := at[1].Sub(at[0])
	last := at[len(at)-1].Sub(at[len(at)-2])
	if last <= first {
		t.Errorf("restart gaps did not grow: first=%v last=%v, want backoff to widen them", first, last)
	}
}

func TestSuperviseWatchCancelDuringBackoff(t *testing.T) {
	prevBase, prevMax, prevHealthy := watchBackoffBase, watchBackoffMax, watchHealthyRun
	watchBackoffBase = 10 * time.Second // long enough that cancel lands mid-sleep
	watchBackoffMax = 10 * time.Second
	watchHealthyRun = time.Hour
	t.Cleanup(func() {
		watchBackoffBase, watchBackoffMax, watchHealthyRun = prevBase, prevMax, prevHealthy
	})

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseWatch(ctx, "stub", func(context.Context) (time.Duration, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			return 0, errFakeDown // instant fail → the supervisor enters the long backoff
		})
	}()

	<-started
	time.Sleep(20 * time.Millisecond) // let the supervisor reach the backoff sleep
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("superviseWatch did not return promptly when ctx was canceled mid-backoff")
	}
}

func TestSuperviseWatchListFailureNeverHealthy(t *testing.T) {
	prevBase, prevMax, prevHealthy := watchBackoffBase, watchBackoffMax, watchHealthyRun
	watchBackoffBase = 20 * time.Millisecond
	watchBackoffMax = 1 * time.Second
	watchHealthyRun = 5 * time.Millisecond // shorter than the wall-clock the run burns
	t.Cleanup(func() {
		watchBackoffBase, watchBackoffMax, watchHealthyRun = prevBase, prevMax, prevHealthy
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var at []time.Time

	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseWatch(ctx, "stub", func(context.Context) (time.Duration, error) {
			mu.Lock()
			at = append(at, time.Now())
			n := len(at)
			mu.Unlock()
			// Burn more wall-clock than watchHealthyRun, but report zero backend time:
			// the list phase failed before the backend ever ran, so it is never healthy.
			time.Sleep(10 * time.Millisecond)
			if n >= 4 {
				cancel()
			}
			return 0, errFakeDown
		})
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("superviseWatch did not return after ctx cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(at) < 4 {
		t.Fatalf("supervise made %d attempts, want at least 4", len(at))
	}
	// A run that never reached the backend must not reset the backoff: the gaps keep
	// growing rather than collapsing back to base every restart.
	first := at[1].Sub(at[0])
	last := at[len(at)-1].Sub(at[len(at)-2])
	if last <= first {
		t.Errorf("backoff reset despite the backend never being reached: first=%v last=%v", first, last)
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
