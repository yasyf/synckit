package daemon_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/daemon"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const harnessService = "stub"

var (
	unrestricted = netpolicy.State{Status: netpolicy.StatusConnected}
	cellular     = netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}
)

func harnessManifest() manifest.Manifest {
	return manifest.Manifest{
		Name:   harnessService,
		Binary: harnessService,
		Watch:  manifest.WatchSpec{Debounce: codec.Duration(10 * time.Millisecond)},
		Service: manifest.ServiceSpec{
			Kind:              "resident",
			SchemaFingerprint: strings.Repeat("a", 64),
		},
	}
}

type monitor struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
}

func (m *monitor) Current() (netpolicy.State, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.changed
}

func (*monitor) Close() error { return nil }

func (m *monitor) set(state netpolicy.State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	close(m.changed)
	m.changed = make(chan struct{})
}

type catalog struct {
	mu       sync.Mutex
	revision syncservice.Revision
	roots    []artifact.Ref
	held     map[string]syncservice.Receipt
	applies  int
}

func (*catalog) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities(harnessService), nil
}

func (*catalog) List(context.Context) ([]syncservice.WatchItem, error) { return nil, nil }

func (*catalog) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{}, nil
}

func (*catalog) Export(context.Context, syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	return syncservice.ChangeEnvelope{}, errors.New("catalog: v1 export is unsupported")
}

func (*catalog) Apply(context.Context, syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	return syncservice.ApplyResult{}, errors.New("catalog: v1 apply is unsupported")
}

func (c *catalog) publish(revision uint64, roots ...artifact.Ref) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revision, c.roots = syncservice.NewRevision(revision), roots
}

func (c *catalog) ExportArtifacts(_ context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	payload, err := json.Marshal(c.roots)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	return syncservice.NewExportedArtifactChange(request.ServiceID, request.SchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), c.revision, payload, c.roots)
}

func (c *catalog) ApplyArtifacts(_ context.Context, change syncservice.ChangeEnvelope, ready []artifact.Ref) (syncservice.ApplyResult, error) {
	var roots []artifact.Ref
	if err := json.Unmarshal(change.Payload, &roots); err != nil {
		return syncservice.ApplyResult{}, err
	}
	if !reflect.DeepEqual(roots, change.Artifacts) {
		return syncservice.ApplyResult{}, errors.New("catalog: payload roots differ from the change's artifacts")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applies++
	var held *syncservice.Receipt
	if receipt, ok := c.held[change.Origin]; ok {
		held = &receipt
	}
	decision, result, err := syncservice.Fence(held, change)
	switch {
	case err != nil || decision != syncservice.FenceApply:
		return result, err
	case len(ready) < len(roots):
		result = syncservice.ApplyResult{AckedRevision: syncservice.NewRevision(0), Partial: true}
		if held != nil {
			result.AckedRevision = held.Revision
		}
		return result, nil
	}
	c.held[change.Origin] = change.Receipt()
	c.roots, c.revision = roots, change.SourceRevision
	return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
}

func (c *catalog) state() ([]artifact.Ref, syncservice.Revision, map[string]syncservice.Receipt, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.roots), c.revision, maps.Clone(c.held), c.applies
}

type link struct {
	dispatcher *rpc.Dispatcher
	down       atomic.Bool
	hook       atomic.Pointer[func(*rpc.Request)]
	puts       atomic.Int64
}

func (l *link) Do(ctx context.Context, request *rpc.Request) (*syncservice.Response, error) {
	if l.down.Load() {
		return nil, errors.New("link down")
	}
	if request.Method == artifact.MethodBatchPut {
		l.puts.Add(1)
	}
	if hook := l.hook.Load(); hook != nil {
		(*hook)(request)
	}
	response := l.dispatcher.Dispatch(ctx, request)
	return &syncservice.Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (*link) Close() error { return nil }

type meshHost struct {
	store    *artifact.Store
	catalog  *catalog
	monitor  *monitor
	dispatch *rpc.Dispatcher
}

func (h *meshHost) put(t *testing.T, content []byte) artifact.Ref {
	t.Helper()
	ref, err := h.store.Put(t.Context(), bytes.NewReader(content), "test/blob")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func (h *meshHost) requireContent(t *testing.T, ref artifact.Ref, want []byte) {
	t.Helper()
	r, err := h.store.Open(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("store holds %d bytes for %s, want the %d delivered bytes", len(got), ref.Digest, len(want))
	}
}

func (h *meshHost) requireAbsent(t *testing.T, ref artifact.Ref) {
	t.Helper()
	if missing, err := h.store.Complete(t.Context(), []artifact.Ref{ref}); err != nil || missing == 0 {
		t.Fatalf("Complete(%s) = %d, %v; want the root incomplete", ref.Digest, missing, err)
	}
}

type mesh struct {
	hosts   map[string]*meshHost
	links   map[[2]string]*link
	harness *daemon.Harness
}

func newMesh(t *testing.T, names ...string) *mesh {
	t.Helper()
	m := &mesh{hosts: map[string]*meshHost{}, links: map[[2]string]*link{}}
	for _, name := range names {
		store, err := artifact.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		h := &meshHost{
			store: store, catalog: &catalog{revision: syncservice.NewRevision(0), held: map[string]syncservice.Receipt{}},
			monitor: &monitor{state: unrestricted, changed: make(chan struct{})}, dispatch: rpc.NewDispatcher(),
		}
		syncservice.RegisterArtifactConsumer(h.dispatch, h.catalog, store, h.monitor)
		m.hosts[name] = h
	}
	for _, from := range names {
		for _, to := range names {
			m.links[[2]string{from, to}] = &link{dispatcher: m.hosts[to].dispatch}
		}
	}
	return m
}

func (m *mesh) link(from, to string) *link { return m.links[[2]string{from, to}] }

func (m *mesh) start(t *testing.T) {
	t.Helper()
	hosts := make([]daemon.HarnessHost, 0, len(m.hosts))
	for _, name := range slices.Sorted(maps.Keys(m.hosts)) {
		hosts = append(hosts, daemon.HarnessHost{
			Name: name, StateDir: t.TempDir(), Monitor: m.hosts[name].monitor,
			Services: map[string]syncservice.Transport{harnessService: m.link(name, name)},
		})
	}
	h, err := daemon.NewHarness(context.Background(), daemon.HarnessConfig{
		Hosts:         hosts,
		Manifests:     []manifest.Manifest{harnessManifest()},
		Links:         func(from, to, _ string) syncservice.Transport { return m.link(from, to) },
		RetryInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	m.harness = h
}

func (m *mesh) waitIdle(t *testing.T, from, to string) delivery.PeerStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	status, err := m.harness.WaitIdle(ctx, from, harnessService, to)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func requireAcked(t *testing.T, status delivery.PeerStatus, revision uint64) {
	t.Helper()
	if status.State != delivery.StateIdle || status.Acked != syncservice.NewRevision(revision) || status.Pending != nil || status.AckedChangeID == "" {
		t.Fatalf("status = %+v, want idle with revision %d acknowledged", status, revision)
	}
}

func TestHarnessDeliversAndAcks(t *testing.T) {
	m := newMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 3*artifact.ChunkSize)
	root := a.put(t, content)
	a.catalog.publish(1, root)
	m.start(t)

	requireAcked(t, m.waitIdle(t, "a@node", "b@node"), 1)
	b.requireContent(t, root, content)
	roots, revision, held, _ := b.catalog.state()
	if !reflect.DeepEqual(roots, []artifact.Ref{root}) || revision != syncservice.NewRevision(1) || held["a@node"].Revision != syncservice.NewRevision(1) {
		t.Fatalf("b catalog = %v at %s with receipts %+v", roots, revision, held)
	}
	statuses, err := m.harness.Status("a@node", harnessService)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Peer != "b@node" || statuses[0].Acked != syncservice.NewRevision(1) {
		t.Fatalf("Status(a@node) = %+v, want b@node acked at 1", statuses)
	}
}

func TestHarnessRelaysWithoutTheOrigin(t *testing.T) {
	m := newMesh(t, "a@node", "b@node", "c@node")
	a, c := m.hosts["a@node"], m.hosts["c@node"]
	m.link("a@node", "c@node").down.Store(true)
	content := randomBytes(t, 3*artifact.ChunkSize)
	root := a.put(t, content)
	a.catalog.publish(1, root)
	m.start(t)

	requireAcked(t, m.waitIdle(t, "a@node", "b@node"), 1)
	if status := m.waitIdle(t, "a@node", "c@node"); status.State != delivery.StatePaused || status.PauseReason != delivery.PausePeerUnreachable || status.Acked != syncservice.NewRevision(0) {
		t.Fatalf("a's lane to c = %+v, want paused peer-unreachable and unacked", status)
	}
	m.link("a@node", "b@node").down.Store(true)

	if err := m.harness.Kick(harnessService, "b@node", "c@node"); err != nil {
		t.Fatal(err)
	}
	requireAcked(t, m.waitIdle(t, "b@node", "c@node"), 1)
	c.requireContent(t, root, content)
	roots, revision, held, _ := c.catalog.state()
	if !reflect.DeepEqual(roots, []artifact.Ref{root}) || revision != syncservice.NewRevision(1) || held["b@node"].Revision != syncservice.NewRevision(1) {
		t.Fatalf("c catalog = %v at %s with receipts %+v", roots, revision, held)
	}
	if puts := m.link("a@node", "c@node").puts.Load(); puts != 0 {
		t.Fatalf("a sent %d parts to c, want 0", puts)
	}
}

func TestHarnessReceiverCellularPausesUntilResumed(t *testing.T) {
	m := newMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 9*artifact.ChunkSize)
	root := a.put(t, content)
	a.catalog.publish(1, root)
	ab := m.link("a@node", "b@node")
	flip := func(request *rpc.Request) {
		if request.Method == artifact.MethodBatchPut && ab.puts.Load() == 3 {
			b.monitor.set(cellular)
		}
	}
	ab.hook.Store(&flip)
	m.start(t)

	status := m.waitIdle(t, "a@node", "b@node")
	if status.State != delivery.StatePaused || status.PauseReason != delivery.PausePeerCellular || status.Acked != syncservice.NewRevision(0) ||
		status.Pending == nil || status.PeerNetwork == nil || !status.PeerNetwork.Cellular {
		t.Fatalf("status after the receiver turned cellular = %+v, want paused peer-cellular with the change pending", status)
	}
	if _, _, _, applies := b.catalog.state(); applies != 0 {
		t.Fatalf("b applied %d changes while cellular, want 0", applies)
	}
	b.requireAbsent(t, root)

	b.monitor.set(unrestricted)
	if err := m.harness.Kick(harnessService, "a@node", "b@node"); err != nil {
		t.Fatal(err)
	}
	requireAcked(t, m.waitIdle(t, "a@node", "b@node"), 1)
	b.requireContent(t, root, content)
}

func TestNewHarnessRejectsInvalidConfig(t *testing.T) {
	host := func(name string, services ...string) daemon.HarnessHost {
		h := daemon.HarnessHost{Name: name, StateDir: t.TempDir(), Monitor: &monitor{changed: make(chan struct{})}, Services: map[string]syncservice.Transport{}}
		for _, service := range services {
			h.Services[service] = &link{}
		}
		return h
	}
	tests := []struct {
		name string
		cfg  daemon.HarnessConfig
		want string
	}{
		{"non-positive retry", daemon.HarnessConfig{Hosts: []daemon.HarnessHost{host("a@node")}}, `harness: RetryInterval 0s is not positive`},
		{"duplicate host", daemon.HarnessConfig{Hosts: []daemon.HarnessHost{host("a@node"), host("a@node")}, RetryInterval: time.Second}, `harness host "a@node" is listed twice`},
		{"missing state dir", daemon.HarnessConfig{Hosts: []daemon.HarnessHost{{Name: "a@node"}}, RetryInterval: time.Second}, `harness host "a@node" has no StateDir`},
		{"unknown service", daemon.HarnessConfig{Hosts: []daemon.HarnessHost{host("a@node", "other")}, RetryInterval: time.Second}, `harness host "a@node" runs service "other" with no manifest`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := daemon.NewHarness(t.Context(), tt.cfg)
			if h != nil || err == nil || err.Error() != tt.want {
				t.Fatalf("NewHarness() = %v, %v; want error %q", h, err, tt.want)
			}
		})
	}
}
