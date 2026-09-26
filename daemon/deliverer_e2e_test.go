package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type catalogApply struct {
	changeID string
	ready    int
	result   syncservice.ApplyResult
}

type catalogConsumer struct {
	mu       sync.Mutex
	revision uint64
	roots    []artifact.Ref
	held     map[string]syncservice.Receipt
	applies  []catalogApply
}

func (*catalogConsumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("stub"), nil
}

func (*catalogConsumer) List(context.Context) ([]syncservice.WatchItem, error) { return nil, nil }

func (*catalogConsumer) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{}, nil
}

func (*catalogConsumer) Export(context.Context, syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	return syncservice.ChangeEnvelope{}, errors.New("catalog: v1 export is unsupported")
}

func (*catalogConsumer) Apply(context.Context, syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	return syncservice.ApplyResult{}, errors.New("catalog: v1 apply is unsupported")
}

func (c *catalogConsumer) publish(revision uint64, roots ...artifact.Ref) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revision, c.roots = revision, roots
}

func (c *catalogConsumer) ExportArtifacts(_ context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	payload, err := json.Marshal(c.roots)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	return syncservice.NewExportedArtifactChange(request.ServiceID, request.SchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(c.revision), payload, c.roots)
}

func (c *catalogConsumer) ApplyArtifacts(_ context.Context, change syncservice.ChangeEnvelope, ready []artifact.Ref) (syncservice.ApplyResult, error) {
	var roots []artifact.Ref
	if err := json.Unmarshal(change.Payload, &roots); err != nil {
		return syncservice.ApplyResult{}, err
	}
	if !reflect.DeepEqual(roots, change.Artifacts) {
		return syncservice.ApplyResult{}, errors.New("catalog: payload roots differ from the change's artifacts")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var held *syncservice.Receipt
	if receipt, ok := c.held[change.Origin]; ok {
		held = &receipt
	}
	decision, result, err := syncservice.Fence(held, change)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	switch {
	case decision != syncservice.FenceApply:
	case len(ready) < len(roots):
		result = syncservice.ApplyResult{AckedRevision: syncservice.NewRevision(0), Partial: true}
		if held != nil {
			result.AckedRevision = held.Revision
		}
	default:
		c.held[change.Origin] = change.Receipt()
		c.roots, c.revision = roots, mustRevision(change.SourceRevision)
		result = syncservice.ApplyResult{AckedRevision: change.SourceRevision}
	}
	c.applies = append(c.applies, catalogApply{changeID: change.ChangeID, ready: len(ready), result: result})
	return result, nil
}

func (c *catalogConsumer) snapshot() ([]artifact.Ref, uint64, []catalogApply, map[string]syncservice.Receipt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.roots), c.revision, slices.Clone(c.applies), maps.Clone(c.held)
}

type e2eHost struct {
	name       string
	store      *artifact.Store
	consumer   *catalogConsumer
	monitor    *fakeMonitor
	dispatcher *rpc.Dispatcher
	deliveries *deliveryStore
}

type wireLink struct {
	down     atomic.Bool
	hook     atomic.Pointer[func(*rpc.Request) error]
	mu       sync.Mutex
	calls    map[string]int
	putBytes int64
	puts     map[int]int
	applied  []string
}

func (l *wireLink) setHook(hook func(*rpc.Request) error) { l.hook.Store(&hook) }

func (l *wireLink) count(method string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[method]
}

func (l *wireLink) wire() (int64, map[int]int, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.putBytes, maps.Clone(l.puts), slices.Clone(l.applied)
}

type linkTransport struct {
	link       *wireLink
	dispatcher *rpc.Dispatcher
}

func (t linkTransport) Do(ctx context.Context, request *rpc.Request) (*syncservice.Response, error) {
	if t.link.down.Load() {
		return nil, errors.New("link down")
	}
	if hook := t.link.hook.Load(); hook != nil {
		if err := (*hook)(request); err != nil {
			return nil, err
		}
	}
	t.link.mu.Lock()
	t.link.calls[request.Method]++
	switch request.Method {
	case artifact.MethodBatchPut:
		data, _ := base64.StdEncoding.DecodeString(request.Params["data"].(string))
		t.link.putBytes += int64(len(data))
		t.link.puts[int(request.Params["index"].(float64))]++
	case syncservice.MethodApplyV2:
		t.link.applied = append(t.link.applied, request.Params["change_id"].(string))
	}
	t.link.mu.Unlock()
	response := t.dispatcher.Dispatch(ctx, request)
	return &syncservice.Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (linkTransport) Close() error { return nil }

type e2eMesh struct {
	t     *testing.T
	scope processScope
	hosts map[string]*e2eHost
	mu    sync.Mutex
	links map[[2]string]*wireLink
}

func newE2EMesh(t *testing.T, names ...string) *e2eMesh {
	t.Helper()
	saved := []time.Duration{pauseRecheck, deliveryBackoffBase, deliveryBackoffMax}
	pauseRecheck, deliveryBackoffBase, deliveryBackoffMax = time.Hour, time.Hour, time.Hour
	prev := dialTransport
	t.Cleanup(func() {
		pauseRecheck, deliveryBackoffBase, deliveryBackoffMax = saved[0], saved[1], saved[2]
		dialTransport = prev
	})
	m := &e2eMesh{t: t, scope: testProcessScope(t), hosts: map[string]*e2eHost{}, links: map[[2]string]*wireLink{}}
	for _, name := range names {
		store, err := artifact.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		h := &e2eHost{
			name: name, store: store, monitor: newFakeMonitor(unrestricted), dispatcher: rpc.NewDispatcher(),
			consumer:   &catalogConsumer{held: map[string]syncservice.Receipt{}},
			deliveries: newDeliveryStore(t.TempDir()),
		}
		syncservice.RegisterArtifactConsumer(h.dispatcher, h.consumer, store, h.monitor)
		m.hosts[name] = h
	}
	dialTransport = func(_ processScope, _ manifest.Manifest, peer, self string) syncservice.Transport {
		return m.transport(self, peer)
	}
	return m
}

func (m *e2eMesh) link(from, to string) *wireLink {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := [2]string{from, to}
	if m.links[key] == nil {
		m.links[key] = &wireLink{calls: map[string]int{}, puts: map[int]int{}}
	}
	return m.links[key]
}

func (m *e2eMesh) transport(from, to string) syncservice.Transport {
	return linkTransport{link: m.link(from, to), dispatcher: m.hosts[to].dispatcher}
}

type e2eDeliverer struct {
	sched  *deliveryScheduler
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func (d *e2eDeliverer) stop() {
	d.cancel()
	d.wg.Wait()
}

func (m *e2eMesh) deliver(from string, peers ...string) *e2eDeliverer {
	ctx, cancel := context.WithCancel(context.Background())
	d := &e2eDeliverer{cancel: cancel, wg: &sync.WaitGroup{}}
	h := m.hosts[from]
	d.sched = newDeliveryScheduler(ctx, d.wg, m.scope, h.deliveries, h.monitor, from)
	d.sched.snapshot = func(context.Context, string) {}
	d.sched.add(testManifest(), syncservice.NewClient(m.transport(from, from)), peers)
	d.sched.start()
	m.t.Cleanup(d.stop)
	return d
}

func (d *e2eDeliverer) status(t *testing.T, peer string) delivery.PeerStatus {
	t.Helper()
	statuses, err := d.sched.status(context.Background(), "stub")
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if status.Peer == peer {
			return status
		}
	}
	t.Fatalf("no status for %s in %+v", peer, statuses)
	return delivery.PeerStatus{}
}

func (d *e2eDeliverer) await(t *testing.T, peer, what string, cond func(delivery.PeerStatus) bool) delivery.PeerStatus {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		status := d.status(t, peer)
		if cond(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; status %+v", what, status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func patternBytes(seed uint64, chunks int) []byte {
	b := make([]byte, chunks*artifact.ChunkSize)
	for i := 0; i < len(b); i += 16 {
		binary.LittleEndian.PutUint64(b[i:], seed)
		binary.LittleEndian.PutUint64(b[i+8:], uint64(i/artifact.ChunkSize))
	}
	return b
}

func (h *e2eHost) put(t *testing.T, content []byte) artifact.Ref {
	t.Helper()
	ref, err := h.store.Put(t.Context(), bytes.NewReader(content), "test/blob")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func (h *e2eHost) requireContent(t *testing.T, ref artifact.Ref, want []byte) {
	t.Helper()
	if missing, err := h.store.Complete(t.Context(), []artifact.Ref{ref}); err != nil || missing != 0 {
		t.Fatalf("%s Complete(%s) = %d, %v; want 0 missing", h.name, ref.Digest, missing, err)
	}
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
		t.Fatalf("%s holds %d bytes for %s, want the %d delivered bytes", h.name, len(got), ref.Digest, len(want))
	}
}

func (h *e2eHost) requireAbsent(t *testing.T, ref artifact.Ref) {
	t.Helper()
	if missing, err := h.store.Complete(t.Context(), []artifact.Ref{ref}); err != nil || missing == 0 {
		t.Fatalf("%s Complete(%s) = %d, %v; want the root incomplete", h.name, ref.Digest, missing, err)
	}
}

func requireNoApplies(t *testing.T, h *e2eHost) {
	t.Helper()
	if _, _, applies, _ := h.consumer.snapshot(); len(applies) != 0 {
		t.Fatalf("%s consumer applied %+v, want nothing", h.name, applies)
	}
}

func onPut(n int64, fn func(*rpc.Request) error) func(*rpc.Request) error {
	var seen atomic.Int64
	return func(request *rpc.Request) error {
		if request.Method == artifact.MethodBatchPut && seen.Add(1) == n {
			return fn(request)
		}
		return nil
	}
}

func unacked(pending string) func(delivery.PeerStatus) bool {
	return func(s delivery.PeerStatus) bool {
		return s.Acked == syncservice.NewRevision(0) && s.Pending != nil && s.Pending.ChangeID == pending
	}
}

func TestE2EDeliversLargeArtifactAcrossBatches(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	first := patternBytes(1, 28)
	second := randomBytes(t, 9*artifact.ChunkSize+1)
	roots := []artifact.Ref{a.put(t, first), a.put(t, second)}
	a.consumer.publish(1, roots...)

	d := m.deliver("a@node", "b@node")
	status := d.await(t, "b@node", "ack", acked(1))

	link := m.link("a@node", "b@node")
	if got := link.count(artifact.MethodBatchCommit); got != 2 {
		t.Fatalf("batch commits = %d, want 2", got)
	}
	_, _, applies, _ := b.consumer.snapshot()
	want := []catalogApply{
		{changeID: status.AckedChangeID, ready: 1, result: syncservice.ApplyResult{AckedRevision: syncservice.NewRevision(0), Partial: true}},
		{changeID: status.AckedChangeID, ready: 2, result: syncservice.ApplyResult{AckedRevision: syncservice.NewRevision(1)}},
	}
	if !reflect.DeepEqual(applies, want) {
		t.Fatalf("receiver applies = %+v, want %+v", applies, want)
	}
	b.requireContent(t, roots[0], first)
	b.requireContent(t, roots[1], second)
	if err := b.store.Verify(t.Context(), roots); err != nil {
		t.Fatalf("receiver Verify() = %v", err)
	}
	if status.Pending != nil || status.Progress.RootsComplete != 2 || status.Progress.ObjectsSent != status.Progress.ObjectsMissing {
		t.Fatalf("status after ack = %+v", status)
	}
}

func TestE2ECorruptPartWithholdsAckUntilCleanRetry(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	deliveryBackoffBase = time.Millisecond
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 9*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)

	retrying, release := make(chan struct{}), make(chan struct{})
	corrupt := onPut(3, func(request *rpc.Request) error {
		data, err := base64.StdEncoding.DecodeString(request.Params["data"].(string))
		if err != nil {
			return err
		}
		data[len(data)/2] ^= 0xff
		request.Params["data"] = base64.StdEncoding.EncodeToString(data)
		return nil
	})
	var gates atomic.Int64
	m.link("a@node", "b@node").setHook(func(request *rpc.Request) error {
		if request.Method == syncservice.MethodCapabilities && gates.Add(1) == 2 {
			close(retrying)
			<-release
		}
		return corrupt(request)
	})

	d := m.deliver("a@node", "b@node")
	<-retrying
	pending, err := a.deliveries.records(t.Context(), "stub")
	if err != nil || len(pending) != 1 || pending[0].Pending == nil {
		t.Fatalf("delivery records = %+v, %v; want one pending", pending, err)
	}
	status := d.status(t, "b@node")
	if status.State != delivery.StateBackoff || !unacked(pending[0].Pending.ChangeID)(status) || status.LastError == "" {
		t.Fatalf("status after a corrupt part = %+v", status)
	}
	requireNoApplies(t, b)
	b.requireAbsent(t, root)

	close(release)
	d.await(t, "b@node", "clean retry ack", acked(1))
	b.requireContent(t, root, content)
	_, puts, _ := m.link("a@node", "b@node").wire()
	if puts[0] != 1 || puts[1] != 1 || puts[2] != 2 {
		t.Fatalf("parts sent = %v, want parts 0 and 1 once and the corrupted part 2 twice", puts)
	}
}

func TestE2EInterruptedTransferResumesFromHaveParts(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 9*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)

	link := m.link("a@node", "b@node")
	interrupted, canceled := make(chan struct{}), make(chan struct{})
	link.setHook(onPut(4, func(*rpc.Request) error {
		close(interrupted)
		<-canceled
		return context.Canceled
	}))
	first := m.deliver("a@node", "b@node")
	<-interrupted
	first.cancel()
	close(canceled)
	first.stop()
	_, before, _ := link.wire()
	if want := map[int]int{0: 1, 1: 1, 2: 1}; !reflect.DeepEqual(before, want) {
		t.Fatalf("parts sent before the interruption = %v, want %v", before, want)
	}
	requireNoApplies(t, b)
	b.requireAbsent(t, root)

	link.setHook(func(*rpc.Request) error { return nil })
	m.deliver("a@node", "b@node").await(t, "b@node", "resumed ack", acked(1))
	b.requireContent(t, root, content)
	_, after, _ := link.wire()
	for index, sent := range after {
		if sent != 1 {
			t.Fatalf("part %d sent %d times across the restart, want once: %v", index, sent, after)
		}
	}
}

func TestE2EReceiverCellularPausesUntilItRecovers(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	pauseRecheck = 20 * time.Millisecond
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 9*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)
	cellular := netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}
	link := m.link("a@node", "b@node")
	link.setHook(onPut(3, func(*rpc.Request) error {
		b.monitor.set(cellular)
		return nil
	}))

	d := m.deliver("a@node", "b@node")
	status := d.await(t, "b@node", "peer-cellular pause", pausedWith(delivery.PausePeerCellular))
	if status.Acked != syncservice.NewRevision(0) || status.PeerNetwork == nil || !status.PeerNetwork.Cellular {
		t.Fatalf("paused status = %+v", status)
	}
	requireNoApplies(t, b)
	b.requireAbsent(t, root)

	b.monitor.set(unrestricted)
	d.await(t, "b@node", "ack after the receiver recovers", acked(1))
	b.requireContent(t, root, content)
	_, puts, _ := link.wire()
	if puts[0] != 1 || puts[1] != 1 || puts[2] != 2 {
		t.Fatalf("parts sent = %v, want parts 0 and 1 once and the refused part 2 twice", puts)
	}
}

func TestE2ELocalExpensivePausesWithinOnePart(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 9*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)
	link := m.link("a@node", "b@node")
	var flippedAt atomic.Int64
	link.setHook(onPut(3, func(*rpc.Request) error {
		sent, _, _ := link.wire()
		flippedAt.Store(sent)
		a.monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true})
		return nil
	}))

	d := m.deliver("a@node", "b@node")
	status := d.await(t, "b@node", "local-expensive pause", pausedWith(delivery.PauseLocalExpensive))
	sent, _, _ := link.wire()
	if delta := sent - flippedAt.Load(); delta > artifact.PartSize {
		t.Fatalf("sent %d bytes after the local flip, want at most one part (%d)", delta, artifact.PartSize)
	}
	if status.Acked != syncservice.NewRevision(0) {
		t.Fatalf("paused status = %+v", status)
	}
	requireNoApplies(t, b)

	a.monitor.set(unrestricted)
	d.await(t, "b@node", "ack after the local network recovers", acked(1))
	b.requireContent(t, root, content)
}

func TestE2EOfflinePeerSupersedesThenAcksLatest(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	link := m.link("a@node", "b@node")
	link.down.Store(true)
	shared := a.put(t, randomBytes(t, 64<<10))
	var revisions [3]artifact.Ref
	for i := range revisions {
		revisions[i] = a.put(t, randomBytes(t, 32<<10))
	}

	a.consumer.publish(1, shared, revisions[0])
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "peer-unreachable pause", pausedWith(delivery.PausePeerUnreachable))
	_, stale, err := a.deliveries.load(t.Context(), "stub", "b@node")
	if err != nil || stale == nil {
		t.Fatalf("load() = %v, %v; want the first pending change", stale, err)
	}
	for revision := uint64(2); revision <= 3; revision++ {
		a.consumer.publish(revision, shared, revisions[revision-1])
		if err := d.sched.Kick("stub", "b@node"); err != nil {
			t.Fatal(err)
		}
		d.await(t, "b@node", fmt.Sprintf("pending %d", revision), func(s delivery.PeerStatus) bool {
			return s.Pending != nil && s.Pending.SourceRevision == syncservice.NewRevision(revision)
		})
	}
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	status := d.status(t, "b@node")
	if status.Acked != syncservice.NewRevision(0) || status.Pending.Superseded != 2 || status.State != delivery.StatePaused {
		t.Fatalf("status after three offline kicks = %+v", status)
	}
	files, err := filepath.Glob(filepath.Join(a.deliveries.pending, "*.json"))
	if err != nil || len(files) != 1 || filepath.Base(files[0]) != status.Pending.ChangeID+".json" {
		t.Fatalf("pending files = %v, %v; want only %s", files, err, status.Pending.ChangeID)
	}
	pins, err := a.store.Pins(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 1 || pins[0].Owner != "synckit.delivery/b@node" || !reflect.DeepEqual(pins[0].Roots, []artifact.Ref{shared, revisions[2]}) {
		t.Fatalf("source pins = %+v, want the latest roots only", pins)
	}
	if err := a.deliveries.acknowledge(t.Context(), "b@node", *stale, syncservice.ApplyResult{AckedRevision: stale.SourceRevision}); !errors.Is(err, errPendingChanged) {
		t.Fatalf("late acknowledge of a superseded change = %v, want errPendingChanged", err)
	}

	latest := status.Pending.ChangeID
	link.down.Store(false)
	a.monitor.set(unrestricted)
	status = d.await(t, "b@node", "ack of the latest", acked(3))
	if status.AckedChangeID != latest {
		t.Fatalf("acked change %s, want the latest %s", status.AckedChangeID, latest)
	}
	if _, _, applied := link.wire(); !reflect.DeepEqual(applied, []string{latest}) {
		t.Fatalf("apply.v2 change ids = %v, want only %s", applied, latest)
	}
	b.requireAbsent(t, revisions[0])
	b.requireAbsent(t, revisions[1])
	if missing, err := b.store.Complete(t.Context(), []artifact.Ref{shared, revisions[2]}); err != nil || missing != 0 {
		t.Fatalf("receiver Complete(latest) = %d, %v", missing, err)
	}
	if err := a.deliveries.acknowledge(t.Context(), "b@node", *stale, syncservice.ApplyResult{AckedRevision: stale.SourceRevision}); !errors.Is(err, errPendingChanged) {
		t.Fatalf("late acknowledge after the ack = %v, want errPendingChanged", err)
	}
	if status := d.status(t, "b@node"); status.Acked != syncservice.NewRevision(3) || status.AckedChangeID != latest {
		t.Fatalf("status after a late ack = %+v", status)
	}
}

func TestE2ERelaysWithoutTheOrigin(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node", "c@node")
	a, c := m.hosts["a@node"], m.hosts["c@node"]
	m.link("a@node", "c@node").down.Store(true)
	content := randomBytes(t, 3*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)

	origin := m.deliver("a@node", "b@node", "c@node")
	origin.await(t, "b@node", "ack at b", acked(1))
	origin.await(t, "c@node", "a cannot reach c", pausedWith(delivery.PausePeerUnreachable))
	m.link("a@node", "b@node").down.Store(true)

	m.deliver("b@node", "c@node").await(t, "c@node", "relayed ack", acked(1))
	c.requireContent(t, root, content)
	roots, revision, _, held := c.consumer.snapshot()
	if !reflect.DeepEqual(roots, []artifact.Ref{root}) || revision != 1 || held["b@node"].Revision != syncservice.NewRevision(1) {
		t.Fatalf("c catalog = %v at %d with receipts %+v", roots, revision, held)
	}
	if got := m.link("a@node", "c@node").count(artifact.MethodBatchPut); got != 0 {
		t.Fatalf("a sent %d parts to c, want 0", got)
	}
	if status := origin.status(t, "c@node"); status.Acked != syncservice.NewRevision(0) {
		t.Fatalf("a's lane to c = %+v, want unacked", status)
	}
}

func TestE2ECoalescesKicks(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a := m.hosts["a@node"]
	a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "first ack", acked(1))

	local := m.link("a@node", "a@node")
	exports := local.count(syncservice.MethodExportV2)
	running, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m.link("a@node", "b@node").setHook(func(request *rpc.Request) error {
		if request.Method == syncservice.MethodCapabilities {
			once.Do(func() {
				close(running)
				<-release
			})
		}
		return nil
	})
	a.consumer.publish(2, a.put(t, randomBytes(t, 4<<10)))
	kick := func() {
		if err := d.sched.Kick("stub", "b@node"); err != nil {
			t.Fatal(err)
		}
	}
	kick()
	<-running
	for range 39 {
		kick()
	}
	close(release)
	d.await(t, "b@node", "second ack", acked(2))
	d.await(t, "b@node", "coalesced follow-up run", func(delivery.PeerStatus) bool {
		return local.count(syncservice.MethodExportV2) >= exports+2
	})
	time.Sleep(50 * time.Millisecond)
	if runs := local.count(syncservice.MethodExportV2) - exports; runs != 2 {
		t.Fatalf("40 kicks ran %d deliveries, want 2", runs)
	}
}

func TestE2EUnchangedContentSendsNothing(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 3*artifact.ChunkSize)
	root := a.put(t, content)
	a.consumer.publish(1, root)
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "first ack", acked(1))

	link, local := m.link("a@node", "b@node"), m.link("a@node", "a@node")
	sent, _, applied := link.wire()
	begins, haves := link.count(artifact.MethodBatchBegin), link.count(artifact.MethodHave)
	a.consumer.publish(2, root)
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	d.await(t, "b@node", "ack of unchanged content", acked(2))
	resent, _, reapplied := link.wire()
	if resent != sent || link.count(artifact.MethodBatchBegin) != begins || link.count(artifact.MethodHave) != haves+1 || len(reapplied) != len(applied)+1 {
		t.Fatalf("unchanged content: %d → %d wire bytes, %d → %d begins, %d → %d haves, %d → %d applies",
			sent, resent, begins, link.count(artifact.MethodBatchBegin), haves, link.count(artifact.MethodHave), len(applied), len(reapplied))
	}

	exports, calls := local.count(syncservice.MethodExportV2), link.count(syncservice.MethodCapabilities)
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	d.await(t, "b@node", "idle re-export", func(s delivery.PeerStatus) bool {
		return local.count(syncservice.MethodExportV2) == exports+1 && s.State == delivery.StateIdle
	})
	if _, _, again := link.wire(); len(again) != len(reapplied) || link.count(syncservice.MethodCapabilities) != calls {
		t.Fatalf("Source == Acked dialed the peer: %d applies, %d capability calls", len(again)-len(reapplied), link.count(syncservice.MethodCapabilities)-calls)
	}
	b.requireContent(t, root, content)
}
