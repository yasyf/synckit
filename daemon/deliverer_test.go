package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

var unrestricted = netpolicy.State{Status: netpolicy.StatusConnected}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type fakeMonitor struct {
	mu      sync.Mutex
	state   netpolicy.State
	changed chan struct{}
}

func newFakeMonitor(state netpolicy.State) *fakeMonitor {
	return &fakeMonitor{state: state, changed: make(chan struct{})}
}

func (m *fakeMonitor) Current() (netpolicy.State, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.changed
}

func (m *fakeMonitor) set(state netpolicy.State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	close(m.changed)
	m.changed = make(chan struct{})
}

func (*fakeMonitor) Close() error { return nil }

type objectSet struct {
	data  map[artifact.Digest][]byte
	kinds map[artifact.Digest]artifact.Kind
}

func newObjectSet() objectSet {
	return objectSet{data: map[artifact.Digest][]byte{}, kinds: map[artifact.Digest]artifact.Kind{}}
}

func (o objectSet) put(kind artifact.Kind, data []byte) artifact.Digest {
	digest := artifact.Sum(data)
	o.data[digest], o.kinds[digest] = data, kind
	return digest
}

func (o objectSet) closure(roots []artifact.Ref) ([]artifact.ObjectEntry, bool) {
	seen := map[artifact.Digest]bool{}
	var out []artifact.ObjectEntry
	complete := true
	var visit func(artifact.Digest)
	visit = func(digest artifact.Digest) {
		if seen[digest] {
			return
		}
		seen[digest] = true
		data, ok := o.data[digest]
		if !ok {
			complete = false
			return
		}
		if o.kinds[digest] == artifact.KindManifest {
			m, err := artifact.DecodeManifest(data)
			if err != nil {
				panic(err)
			}
			for _, chunk := range m.Chunks {
				visit(chunk.Digest)
			}
			for _, dep := range m.Deps {
				visit(dep.Digest)
			}
		}
		out = append(out, artifact.ObjectEntry{Digest: digest, Kind: o.kinds[digest], Size: int64(len(data))})
	}
	for _, root := range roots {
		visit(root.Digest)
	}
	return out, complete
}

type counter struct {
	mu        sync.Mutex
	calls     map[string]int
	inflight  int
	maxFlight int
}

func (c *counter) enter(method string) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[method]++
	c.inflight++
	c.maxFlight = max(c.maxFlight, c.inflight)
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.inflight--
	}
}

func (c *counter) count(methods ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, method := range methods {
		n += c.calls[method]
	}
	return n
}

func register[P any](d *rpc.Dispatcher, c *counter, method string, handle func(P) (any, error)) {
	d.Register(method, func(_ context.Context, p map[string]any) (any, error) {
		defer c.enter(method)()
		var params P
		if err := decodeStruct(p, &params); err != nil {
			return nil, err
		}
		return handle(params)
	})
}

type fakeSource struct {
	counter
	mu        sync.Mutex
	artifacts bool
	objects   objectSet
	kind      syncservice.ChangeKind
	source    uint64
	roots     []artifact.Ref
	pins      map[string][]artifact.Ref
	batches   map[artifact.Digest][][]byte
	block     chan struct{}
	onRead    func()
}

func newFakeSource(artifacts bool) *fakeSource {
	return &fakeSource{
		artifacts: artifacts, objects: newObjectSet(), kind: syncservice.ChangeSnapshot, source: 1,
		pins: map[string][]artifact.Ref{}, batches: map[artifact.Digest][][]byte{},
	}
}

func (f *fakeSource) blob(content []byte) artifact.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	return artifact.Ref{Digest: f.objects.put(artifact.KindBlob, content), Kind: artifact.KindBlob, Size: int64(len(content))}
}

func (f *fakeSource) group(deps ...artifact.Ref) artifact.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := artifact.Manifest{Schema: artifact.ManifestSchema, Media: "test/group", Chunks: []artifact.ChunkRef{}, Deps: deps}
	raw, err := m.Encode()
	if err != nil {
		panic(err)
	}
	return artifact.Ref{Digest: f.objects.put(artifact.KindManifest, raw), Kind: artifact.KindManifest}
}

func (f *fakeSource) publish(source uint64, roots ...artifact.Ref) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.source, f.roots = source, roots
}

func (f *fakeSource) change(since uint64) (syncservice.ChangeEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kind, base := f.kind, since
	if since == 0 || kind == syncservice.ChangeSnapshot {
		kind, base = syncservice.ChangeSnapshot, 0
	}
	return syncservice.NewExportedArtifactChange("stub", testManifest().Service.SchemaFingerprint, kind,
		syncservice.NewRevision(base), syncservice.NewRevision(f.source), fmt.Appendf(nil, `{"v":%d}`, f.source), f.roots)
}

func (f *fakeSource) pinned(owner string) []artifact.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pins[owner]
}

func (f *fakeSource) dispatcher() *rpc.Dispatcher {
	d := rpc.NewDispatcher()
	register(d, &f.counter, syncservice.MethodCapabilities, func(struct{}) (any, error) {
		if f.artifacts {
			return syncservice.ArtifactCapabilities("stub"), nil
		}
		return syncservice.DefaultCapabilities("stub"), nil
	})
	export := func(request syncservice.ExportRequest) (any, error) {
		f.mu.Lock()
		block := f.block
		f.block = nil
		f.mu.Unlock()
		if block != nil {
			<-block
		}
		since, err := request.SinceRevision.Uint64()
		if err != nil {
			return nil, err
		}
		return f.change(since)
	}
	register(d, &f.counter, syncservice.MethodExport, export)
	register(d, &f.counter, syncservice.MethodExportV2, export)
	register(d, &f.counter, artifact.MethodClosure, func(p artifact.ClosureParams) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		objects, _ := f.objects.closure(p.Roots)
		end := min(p.After+p.Limit, len(objects))
		page := artifact.ClosurePage{Objects: objects[p.After:end], Next: end, Done: end == len(objects), TotalObjects: len(objects)}
		return page, nil
	})
	register(d, &f.counter, artifact.MethodBatchBuild, func(p artifact.BatchBuildParams) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		entries := make([]artifact.ObjectEntry, 0, len(p.Objects))
		var raw []byte
		packed := int64(len("SKP1")) + 1
		for _, object := range p.Objects {
			data := f.objects.data[object.Digest]
			entries = append(entries, object)
			raw = append(raw, data...)
			packed += 1 + 32 + int64(len(binary.AppendUvarint(nil, uint64(len(data))))) + int64(len(data))
		}
		var parts [][]byte
		var refs []artifact.PartRef
		for part := range slices.Chunk(raw, artifact.PartSize) {
			parts = append(parts, part)
			refs = append(refs, artifact.PartRef{Digest: artifact.Sum(part), Size: int64(len(part))})
		}
		batch, err := artifact.NewBatchDescriptor(packed, entries, refs)
		if err != nil {
			return nil, err
		}
		f.batches[batch.ID] = parts
		return batch, nil
	})
	register(d, &f.counter, artifact.MethodBatchRead, func(p artifact.BatchReadParams) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.onRead != nil {
			f.onRead()
		}
		return artifact.BatchReadResult{Data: f.batches[p.ID][p.Index]}, nil
	})
	register(d, &f.counter, artifact.MethodBatchDrop, func(p artifact.BatchRef) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.batches, p.ID)
		return struct{}{}, nil
	})
	register(d, &f.counter, artifact.MethodPinsSet, func(p artifact.PinsSetParams) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.pins[p.Owner] = p.Roots
		return struct{}{}, nil
	})
	return d
}

type stagedBatch struct {
	batch artifact.BatchDescriptor
	parts map[int][]byte
}

type fakeSink struct {
	counter
	mu           sync.Mutex
	artifacts    bool
	unreachable  bool
	state        netpolicy.State
	objects      objectSet
	incoming     map[artifact.Digest]*stagedBatch
	committed    []artifact.Digest
	held         *syncservice.Receipt
	applied      int
	partial      bool
	corrupt      bool
	dropPuts     bool
	loseAck      int
	onPut        func()
	onCommit     func()
	lastApplyKey string
}

func newFakeSink(artifacts bool) *fakeSink {
	return &fakeSink{artifacts: artifacts, state: unrestricted, objects: newObjectSet(), incoming: map[artifact.Digest]*stagedBatch{}}
}

func (k *fakeSink) with(fn func(*fakeSink)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	fn(k)
}

func (k *fakeSink) heldRevision() syncservice.Revision {
	if k.held == nil {
		return syncservice.NewRevision(0)
	}
	return k.held.Revision
}

func (k *fakeSink) apply(change syncservice.ChangeEnvelope) (any, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	time.Sleep(time.Millisecond)
	if k.partial {
		return syncservice.ApplyResult{AckedRevision: k.heldRevision(), Partial: true}, nil
	}
	if _, complete := k.objects.closure(change.Artifacts); !complete {
		return syncservice.ApplyResult{AckedRevision: k.heldRevision(), Partial: true}, nil
	}
	decision, result, err := syncservice.Fence(k.held, change)
	if err != nil || decision != syncservice.FenceApply {
		return result, err
	}
	receipt := change.Receipt()
	k.held = &receipt
	k.applied++
	k.lastApplyKey = string(change.Kind)
	if k.loseAck > 0 {
		k.loseAck--
		return nil, errors.New("connection lost after apply")
	}
	return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
}

func (k *fakeSink) refusal(sender netpolicy.State) (netpolicy.State, *artifact.PausedError) {
	if verdict := netpolicy.Evaluate(k.state, sender); !verdict.Allowed {
		return k.state, artifact.PausedFor(verdict)
	}
	return k.state, nil
}

func (k *fakeSink) dispatcher() *rpc.Dispatcher {
	d := rpc.NewDispatcher()
	register(d, &k.counter, syncservice.MethodCapabilities, func(struct{}) (any, error) {
		if k.artifacts {
			return syncservice.ArtifactCapabilities("stub"), nil
		}
		return syncservice.DefaultCapabilities("stub"), nil
	})
	register(d, &k.counter, syncservice.MethodApply, k.apply)
	register(d, &k.counter, syncservice.MethodApplyV2, k.apply)
	register(d, &k.counter, artifact.MethodNetStatus, func(struct{}) (any, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.unreachable {
			return nil, errors.New("ssh: connect: no route to host")
		}
		return artifact.NetStatusResult{State: k.state}, nil
	})
	register(d, &k.counter, artifact.MethodHave, func(p artifact.HaveParams) (any, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		missing := []artifact.Digest{}
		for _, digest := range p.Digests {
			if _, ok := k.objects.data[digest]; !ok {
				missing = append(missing, digest)
			}
		}
		return artifact.HaveResult{Missing: missing}, nil
	})
	register(d, &k.counter, artifact.MethodBatchBegin, func(p artifact.BatchBeginParams) (any, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if live, paused := k.refusal(p.Sender); paused != nil {
			return artifact.BatchBeginResult{Peer: live, Paused: paused}, nil
		}
		staged, ok := k.incoming[p.Batch.ID]
		if !ok {
			staged = &stagedBatch{batch: p.Batch, parts: map[int][]byte{}}
			k.incoming[p.Batch.ID] = staged
		}
		have := []int{}
		for index := range staged.parts {
			have = append(have, index)
		}
		slices.Sort(have)
		return artifact.BatchBeginResult{HaveParts: have, Peer: k.state}, nil
	})
	register(d, &k.counter, artifact.MethodBatchPut, func(p artifact.BatchPutParams) (any, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if live, paused := k.refusal(p.Sender); paused != nil {
			return artifact.BatchPutResult{Peer: live, Paused: paused}, nil
		}
		if k.onPut != nil {
			k.onPut()
		}
		data := bytes.Clone(p.Data)
		if k.corrupt {
			data[0] ^= 0xff
		}
		if !k.dropPuts {
			k.incoming[p.ID].parts[p.Index] = data
		}
		return artifact.BatchPutResult{Peer: k.state}, nil
	})
	register(d, &k.counter, artifact.MethodBatchCommit, func(p artifact.BatchRef) (any, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.onCommit != nil {
			k.onCommit()
		}
		staged := k.incoming[p.ID]
		var raw []byte
		for index, part := range staged.batch.Parts {
			data, ok := staged.parts[index]
			if !ok {
				return nil, fmt.Errorf("missing part %d", index)
			}
			if artifact.Sum(data) != part.Digest {
				return nil, fmt.Errorf("part %d digest mismatch", index)
			}
			raw = append(raw, data...)
		}
		for _, object := range staged.batch.Objects {
			data := raw[:object.Size]
			raw = raw[object.Size:]
			k.objects.put(object.Kind, data)
			k.committed = append(k.committed, object.Digest)
		}
		delete(k.incoming, p.ID)
		return artifact.CommitReport{Stored: len(staged.batch.Objects)}, nil
	})
	return d
}

type deliveryHarness struct {
	t       *testing.T
	monitor *fakeMonitor
	source  *fakeSource
	sinks   map[string]*fakeSink
	store   *deliveryStore
	sched   *deliveryScheduler
}

func newDeliveryHarness(t *testing.T, artifacts bool, peers ...string) *deliveryHarness {
	t.Helper()
	restore := []*time.Duration{&pauseRecheck, &deliveryBackoffBase, &deliveryBackoffMax, &artifactMaxWait}
	saved := []time.Duration{pauseRecheck, deliveryBackoffBase, deliveryBackoffMax, artifactMaxWait}
	pauseRecheck, deliveryBackoffBase, deliveryBackoffMax, artifactMaxWait = time.Hour, time.Hour, time.Hour, 0
	t.Cleanup(func() {
		for i, target := range restore {
			*target = saved[i]
		}
	})
	h := &deliveryHarness{
		t: t, monitor: newFakeMonitor(unrestricted), source: newFakeSource(artifacts),
		sinks: map[string]*fakeSink{}, store: newDeliveryStore(t.TempDir()),
	}
	dispatchers := map[string]*rpc.Dispatcher{"me@self": h.source.dispatcher()}
	for _, peer := range peers {
		h.sinks[peer] = newFakeSink(artifacts)
		dispatchers[peer] = h.sinks[peer].dispatcher()
	}
	prev := dialTransport
	dialTransport = func(_ processScope, _ manifest.Manifest, peer, _ string) syncservice.Transport {
		return directSyncTransport{dispatcher: dispatchers[peer]}
	}
	t.Cleanup(func() { dialTransport = prev })
	return h
}

func (h *deliveryHarness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	h.sched = newDeliveryScheduler(ctx, wg, testProcessScope(h.t), h.store, h.monitor, "me@self")
	h.sched.snapshot = func(context.Context, string) {}
	local := syncservice.NewClient(directSyncTransport{dispatcher: h.source.dispatcher()})
	peers := slices.Sorted(func(yield func(string) bool) {
		for peer := range h.sinks {
			if !yield(peer) {
				return
			}
		}
	})
	h.sched.add(testManifest(), local, peers)
	h.sched.start()
	h.t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

func (h *deliveryHarness) status(peer string) delivery.PeerStatus {
	h.t.Helper()
	statuses, err := h.sched.status(context.Background(), "stub")
	if err != nil {
		h.t.Fatal(err)
	}
	for _, status := range statuses {
		if status.Peer == peer {
			return status
		}
	}
	h.t.Fatalf("no status for %s in %#v", peer, statuses)
	return delivery.PeerStatus{}
}

func (h *deliveryHarness) await(peer string, what string, cond func(delivery.PeerStatus) bool) delivery.PeerStatus {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status := h.status(peer)
		if cond(status) {
			return status
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s; status %+v", what, status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func acked(revision uint64) func(delivery.PeerStatus) bool {
	return func(s delivery.PeerStatus) bool {
		return s.Acked == syncservice.NewRevision(revision) && s.State == delivery.StateIdle
	}
}

func pausedWith(reason delivery.PauseReason) func(delivery.PeerStatus) bool {
	return func(s delivery.PeerStatus) bool { return s.State == delivery.StatePaused && s.PauseReason == reason }
}

func backoff(s delivery.PeerStatus) bool { return s.State == delivery.StateBackoff }

var transferMethods = []string{
	artifact.MethodHave, artifact.MethodBatchBegin, artifact.MethodBatchPut, artifact.MethodBatchCommit,
	syncservice.MethodApplyV2, syncservice.MethodApply,
}

func TestDelivererV1ServiceDelivers(t *testing.T) {
	h := newDeliveryHarness(t, false, "peer@node")
	h.source.kind = syncservice.ChangeDelta
	h.source.publish(3)
	h.start()
	h.await("peer@node", "v1 ack", acked(3))
	sink := h.sinks["peer@node"]
	if got := sink.count(syncservice.MethodApply); got != 1 {
		t.Fatalf("v1 apply calls = %d, want 1", got)
	}
	if got := sink.count(artifact.MethodNetStatus, syncservice.MethodApplyV2, syncservice.MethodCapabilities); got != 0 {
		t.Fatalf("v1 delivery made %d v2/policy calls, want 0", got)
	}
	sink.with(func(k *fakeSink) {
		if k.held.Origin != "me@self" || k.lastApplyKey != string(syncservice.ChangeSnapshot) {
			t.Fatalf("held receipt %#v kind %s", k.held, k.lastApplyKey)
		}
	})
	exports := h.source.count(syncservice.MethodExport)
	if err := h.sched.Kick("stub", "peer@node"); err != nil {
		t.Fatal(err)
	}
	h.await("peer@node", "second export", func(delivery.PeerStatus) bool { return h.source.count(syncservice.MethodExport) == exports+1 })
	h.await("peer@node", "idle", acked(3))
	if got := sink.count(syncservice.MethodApply); got != 1 {
		t.Fatalf("unchanged v1 export applied again: %d calls", got)
	}
}

func TestDelivererTransfersRootsInPriorityOrder(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	shared := h.source.blob([]byte("shared chunk"))
	first := h.source.group(h.source.blob([]byte("first chunk")), shared)
	second := h.source.blob([]byte("second root"))
	third := h.source.group(shared, h.source.blob([]byte("third chunk")))
	h.source.publish(4, second, first, third)
	h.start()
	status := h.await("peer@node", "artifact ack", acked(4))

	sink := h.sinks["peer@node"]
	want, _ := h.source.objects.closure([]artifact.Ref{second, first, third})
	wantOrder := make([]artifact.Digest, 0, len(want))
	for _, object := range want {
		wantOrder = append(wantOrder, object.Digest)
	}
	sink.with(func(k *fakeSink) {
		if !slices.Equal(k.committed, wantOrder) {
			t.Fatalf("committed order %v, want closure priority order %v", k.committed, wantOrder)
		}
	})
	if status.Progress.RootsTotal != 3 || status.Progress.RootsComplete != 3 || status.Progress.ObjectsSent != int64(len(want)) ||
		!status.Progress.EnumerationDone || status.AckedChangeID == "" || status.Pending != nil {
		t.Fatalf("settled status %+v", status)
	}
	if pins := h.source.pinned(deliveryPinOwnerPrefix + "peer@node"); pins != nil {
		t.Fatalf("delivery pins after ack = %v, want none", pins)
	}

	before := sink.count(transferMethods...)
	exports := h.source.count(syncservice.MethodExportV2)
	if err := h.sched.Kick("", ""); err != nil {
		t.Fatal(err)
	}
	h.await("peer@node", "unchanged run", func(delivery.PeerStatus) bool { return h.source.count(syncservice.MethodExportV2) == exports+1 })
	h.await("peer@node", "idle", acked(4))
	if got := sink.count(transferMethods...); got != before {
		t.Fatalf("unchanged content made %d transfer/apply calls, want 0", got-before)
	}
	if got := sink.count(artifact.MethodNetStatus); got != 1 {
		t.Fatalf("unchanged content dialed the peer: %d net.status calls, want 1", got)
	}
}

func TestDelivererPauseReasons(t *testing.T) {
	restricted := map[string]netpolicy.State{
		"disconnected":   {Status: netpolicy.StatusDisconnected},
		"unknown":        {Status: netpolicy.StatusUnknown},
		"cellular":       {Status: netpolicy.StatusConnected, Cellular: true},
		"expensive":      {Status: netpolicy.StatusConnected, Expensive: true},
		"constrained":    {Status: netpolicy.StatusConnected, Constrained: true},
		"manual-metered": {Status: netpolicy.StatusConnected, ManualMetered: true},
	}
	type pauseCase struct {
		name      string
		configure func(*deliveryHarness, *fakeSink)
		want      delivery.PauseReason
		netStatus int
	}
	tests := make([]pauseCase, 0, 2*len(restricted)+2)
	for cause, state := range restricted {
		tests = append(
			tests,
			pauseCase{"local " + cause, func(h *deliveryHarness, _ *fakeSink) { h.monitor.state = state }, delivery.PauseReason("local-" + cause), 1},
			pauseCase{"peer " + cause, func(_ *deliveryHarness, k *fakeSink) { k.state = state }, delivery.PauseReason("peer-" + cause), 1},
		)
	}
	tests = append(
		tests,
		pauseCase{"peer unreachable", func(_ *deliveryHarness, k *fakeSink) { k.unreachable = true }, delivery.PausePeerUnreachable, 1},
		pauseCase{"peer incompatible", func(_ *deliveryHarness, k *fakeSink) { k.artifacts = false }, delivery.PausePeerIncompatible, 0},
	)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newDeliveryHarness(t, true, "peer@node")
			sink := h.sinks["peer@node"]
			tt.configure(h, sink)
			h.source.publish(2, h.source.blob([]byte("payload")))
			h.start()
			status := h.await("peer@node", "pause", pausedWith(tt.want))
			if got := sink.count(transferMethods...); got != 0 {
				t.Fatalf("paused delivery made %d have/batch/apply calls, want 0", got)
			}
			if got := sink.count(artifact.MethodNetStatus); got != tt.netStatus {
				t.Fatalf("net.status calls = %d, want %d", got, tt.netStatus)
			}
			if status.Pending == nil || status.Pending.SourceRevision != "2" || status.Acked != "0" || status.PauseSince.IsZero() || status.LocalNetwork == nil {
				t.Fatalf("paused status %+v", status)
			}
		})
	}
}

func TestDelivererSupersedesWhileOffline(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	sink := h.sinks["peer@node"]
	sink.unreachable = true
	h.source.publish(1, h.source.blob([]byte("v1")))
	h.start()
	h.await("peer@node", "unreachable", pausedWith(delivery.PausePeerUnreachable))
	var latest artifact.Ref
	for source := uint64(2); source <= 4; source++ {
		latest = h.source.blob(fmt.Appendf(nil, "v%d", source))
		h.source.publish(source, latest)
		if err := h.sched.Kick("stub", "peer@node"); err != nil {
			t.Fatal(err)
		}
		h.await("peer@node", "supersede", func(s delivery.PeerStatus) bool {
			return s.Pending != nil && s.Pending.SourceRevision == syncservice.NewRevision(source)
		})
	}
	status := h.status("peer@node")
	if status.Pending.Superseded != 3 || status.Acked != "0" || status.State != delivery.StatePaused {
		t.Fatalf("offline status %+v", status)
	}
	if files := pendingFiles(t, h.store); len(files) != 1 || files[0] != status.Pending.ChangeID+".json" {
		t.Fatalf("pending files = %v", files)
	}
	if pins := h.source.pinned(deliveryPinOwnerPrefix + "peer@node"); !slices.Equal(pins, []artifact.Ref{latest}) {
		t.Fatalf("delivery pins = %v, want only %v", pins, latest)
	}
	if got := sink.count(artifact.MethodNetStatus); got != 1 {
		t.Fatalf("kicks while paused dialed the peer: %d net.status calls, want 1", got)
	}
}

func TestDelivererResumesOnLocalTransition(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	h.monitor.state = netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}
	h.source.publish(2, h.source.blob([]byte("payload")))
	h.start()
	h.await("peer@node", "local pause", pausedWith(delivery.PauseLocalCellular))
	h.monitor.set(unrestricted)
	h.await("peer@node", "resume without kick", acked(2))
}

func TestDelivererPauseMidTransferStopsWithinOnePart(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	sink := h.sinks["peer@node"]
	roots := make([]artifact.Ref, 0, 4)
	for range 4 {
		chunk := make([]byte, artifact.ChunkSize)
		_, _ = rand.Read(chunk)
		roots = append(roots, h.source.blob(chunk))
	}
	h.source.publish(2, roots...)
	sink.onPut = func() { h.monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true}) }
	h.start()
	status := h.await("peer@node", "mid-transfer pause", pausedWith(delivery.PauseLocalExpensive))
	if got := sink.count(artifact.MethodBatchPut); got != 1 {
		t.Fatalf("puts = %d, want 1 after the pause was observed", got)
	}
	if status.Progress.WireBytesSent > artifact.PartSize || status.Progress.WireBytesSent == 0 {
		t.Fatalf("wire bytes = %d, want one part ≤ %d", status.Progress.WireBytesSent, artifact.PartSize)
	}
	if got := sink.count(artifact.MethodBatchCommit, syncservice.MethodApplyV2); got != 0 || status.Acked != "0" {
		t.Fatalf("paused transfer committed or applied: %d calls, acked %s", got, status.Acked)
	}
	sink.with(func(k *fakeSink) { k.onPut = nil })
	h.monitor.set(unrestricted)
	final := h.await("peer@node", "resume", acked(2))
	if got := sink.count(artifact.MethodBatchCommit); got != 1 {
		t.Fatalf("commits after resume = %d, want 1", got)
	}
	if got := final.Progress.WireBytesSent; got != 4*artifact.PartSize {
		t.Fatalf("wire bytes after resume = %d, want %d across both attempts", got, 4*artifact.PartSize)
	}
}

func TestDelivererReStagesSnapshotOnNeedSnapshot(t *testing.T) {
	h := newDeliveryHarness(t, false, "peer@node")
	seed, err := syncservice.NewExportedChange("stub", testManifest().Service.SchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(3), []byte(`{"seed":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if seed, err = syncservice.BindDelivery(seed, "me@self"); err != nil {
		t.Fatal(err)
	}
	if err := h.store.stage(t.Context(), "peer@node", "", seed); err != nil {
		t.Fatal(err)
	}
	if err := h.store.acknowledge(t.Context(), "peer@node", seed, syncservice.ApplyResult{AckedRevision: seed.SourceRevision}); err != nil {
		t.Fatal(err)
	}
	sink := h.sinks["peer@node"]
	sink.held = &syncservice.Receipt{Origin: "me@self", ChangeID: strings.Repeat("d", 64), Revision: "1", PayloadDigest: strings.Repeat("e", 64)}
	h.source.kind = syncservice.ChangeDelta
	h.source.publish(5)
	h.start()
	status := h.await("peer@node", "snapshot ack", acked(5))
	sink.with(func(k *fakeSink) {
		if k.lastApplyKey != string(syncservice.ChangeSnapshot) || k.applied != 1 {
			t.Fatalf("applied %d, last kind %s", k.applied, k.lastApplyKey)
		}
	})
	if got := sink.count(syncservice.MethodApply); got != 2 || status.Generation != 5 {
		t.Fatalf("apply calls = %d generation %d, want 2 and 5", got, status.Generation)
	}
}

func TestDelivererStaleAnswers(t *testing.T) {
	tests := []struct {
		name   string
		digest func(syncservice.ChangeEnvelope) string
		want   func(delivery.PeerStatus) bool
	}{
		{"equal digest acknowledges", func(c syncservice.ChangeEnvelope) string { return c.PayloadDigest }, acked(2)},
		{"different digest regresses", func(syncservice.ChangeEnvelope) string { return strings.Repeat("f", 64) }, func(s delivery.PeerStatus) bool {
			return backoff(s) && strings.Contains(s.LastError, errSourceRegressed.Error()) && s.Pending != nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newDeliveryHarness(t, true, "peer@node")
			h.source.publish(2, h.source.blob([]byte("payload")))
			change, err := h.source.change(0)
			if err != nil {
				t.Fatal(err)
			}
			h.sinks["peer@node"].held = &syncservice.Receipt{
				Origin: "me@self", ChangeID: strings.Repeat("d", 64), Revision: "2", PayloadDigest: tt.digest(change),
			}
			h.start()
			h.await("peer@node", tt.name, tt.want)
		})
	}
}

func TestDelivererWithholdsAckWithoutCompleteTransfer(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*fakeSink)
		lastError string
	}{
		{"partial", func(k *fakeSink) { k.partial = true }, errIncomplete.Error()},
		{"corrupt part", func(k *fakeSink) { k.corrupt = true }, "digest mismatch"},
		{"missing part", func(k *fakeSink) { k.dropPuts = true }, "missing part"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newDeliveryHarness(t, true, "peer@node")
			tt.configure(h.sinks["peer@node"])
			h.source.publish(2, h.source.blob([]byte("payload")))
			h.start()
			status := h.await("peer@node", "backoff", backoff)
			if !strings.Contains(status.LastError, tt.lastError) || status.Acked != "0" || status.Pending == nil || status.NextAttemptAt.IsZero() {
				t.Fatalf("status %+v, want backoff on %q with pending retained", status, tt.lastError)
			}
			if files := pendingFiles(t, h.store); len(files) != 1 {
				t.Fatalf("pending files = %v", files)
			}
		})
	}
}

func TestDelivererReplayIsIdempotent(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	deliveryBackoffBase = 10 * time.Millisecond
	sink := h.sinks["peer@node"]
	sink.loseAck = 1
	h.source.publish(2, h.source.blob([]byte("payload")))
	h.start()
	h.await("peer@node", "replayed ack", acked(2))
	sink.with(func(k *fakeSink) {
		if k.applied != 1 {
			t.Fatalf("consumer applied %d times, want 1", k.applied)
		}
	})
	if got := sink.count(artifact.MethodBatchCommit); got != 1 {
		t.Fatalf("commits = %d, want 1 (replay re-sends nothing)", got)
	}
}

func TestDelivererCoalescesKicks(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	release := make(chan struct{})
	h.source.block = release
	h.source.publish(2, h.source.blob([]byte("payload")))
	h.start()
	h.await("peer@node", "blocked export", func(delivery.PeerStatus) bool { return h.source.count(syncservice.MethodExportV2) == 1 })
	for range 40 {
		if err := h.sched.Kick("stub", ""); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	h.await("peer@node", "ack", acked(2))
	time.Sleep(50 * time.Millisecond)
	if got := h.source.count(syncservice.MethodExportV2); got != 2 {
		t.Fatalf("runs = %d, want 2 for 40 coalesced kicks", got)
	}
}

func TestDelivererSerializesEachLane(t *testing.T) {
	h := newDeliveryHarness(t, true, "a@node", "b@node")
	deliveryBackoffBase = 10 * time.Millisecond
	h.source.publish(1, h.source.blob([]byte("r1")))
	h.start()
	var publishing sync.Mutex
	var final uint64 = 1
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				publishing.Lock()
				final++
				h.source.publish(final, h.source.blob(fmt.Appendf(nil, "r%d", final)))
				publishing.Unlock()
				if err := h.sched.Kick("stub", ""); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	for _, peer := range []string{"a@node", "b@node"} {
		if err := h.sched.Kick("stub", peer); err != nil {
			t.Fatal(err)
		}
		h.await(peer, "final ack", acked(final))
		sink := h.sinks[peer]
		sink.counter.mu.Lock()
		maxFlight := sink.maxFlight
		sink.counter.mu.Unlock()
		if maxFlight != 1 {
			t.Fatalf("%s saw %d concurrent calls, want 1", peer, maxFlight)
		}
	}
}

func TestDeliveryRPC(t *testing.T) {
	h := newDeliveryHarness(t, false, "peer@node")
	h.source.publish(1)
	h.start()
	h.await("peer@node", "ack", acked(1))
	d := rpc.NewDispatcher()
	registerDelivery(d, &supervisor{scheduler: h.sched})

	resp := d.Dispatch(t.Context(), &rpc.Request{Method: delivery.MethodStatus, Params: map[string]any{"service_id": "stub"}})
	var statuses []delivery.PeerStatus
	if !resp.OK || json.Unmarshal(resp.Result, &statuses) != nil || len(statuses) != 1 ||
		statuses[0].Peer != "peer@node" || statuses[0].Acked != "1" || statuses[0].State != delivery.StateIdle {
		t.Fatalf("status response %s %s", resp.Result, resp.Error)
	}
	tests := []struct {
		name   string
		params map[string]any
		ok     bool
	}{
		{"every service", map[string]any{}, true},
		{"one peer", map[string]any{"service_id": "stub", "peer": "peer@node"}, true},
		{"unknown service", map[string]any{"service_id": "nope"}, false},
		{"unknown peer", map[string]any{"service_id": "stub", "peer": "ghost@node"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := d.Dispatch(t.Context(), &rpc.Request{Method: delivery.MethodKick, Params: tt.params})
			if resp.OK != tt.ok {
				t.Fatalf("kick %v ok = %v (%s), want %v", tt.params, resp.OK, resp.Error, tt.ok)
			}
		})
	}
}

func TestDelivererRelaysWhileAnotherPeerIsDown(t *testing.T) {
	h := newDeliveryHarness(t, true, "a@node", "c@node")
	h.sinks["a@node"].unreachable = true
	relayed := h.source.group(h.source.blob([]byte("captured on a")))
	h.source.publish(3, relayed)
	h.start()
	h.await("a@node", "origin down", pausedWith(delivery.PausePeerUnreachable))
	h.await("c@node", "relay ack", acked(3))
	h.sinks["c@node"].with(func(k *fakeSink) {
		if _, complete := k.objects.closure([]artifact.Ref{relayed}); !complete {
			t.Fatal("relayed closure incomplete on c@node")
		}
	})
}

func TestDelivererReconcilesPinsAfterRestart(t *testing.T) {
	tests := []struct {
		name  string
		acked bool
	}{
		{"crash before narrowing a staged pending", false},
		{"crash after the ack before unpinning", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newDeliveryHarness(t, true, "peer@node")
			h.sinks["peer@node"].unreachable = true
			root := h.source.blob([]byte("current"))
			stale := h.source.blob([]byte("superseded"))
			h.source.publish(1, root)
			change, err := syncservice.NewExportedArtifactChange("stub", testManifest().Service.SchemaFingerprint,
				syncservice.ChangeSnapshot, syncservice.NewRevision(0), syncservice.NewRevision(1), []byte(`{"v":1}`), []artifact.Ref{root})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := syncservice.BindDelivery(change, "me@self")
			if err != nil {
				t.Fatal(err)
			}
			if err := h.store.stage(t.Context(), "peer@node", "", bound); err != nil {
				t.Fatal(err)
			}
			want := []artifact.Ref{root}
			if tt.acked {
				if err := h.store.acknowledge(t.Context(), "peer@node", bound, syncservice.ApplyResult{AckedRevision: bound.SourceRevision}); err != nil {
					t.Fatal(err)
				}
				want = nil
			}
			owner := deliveryPinOwnerPrefix + "peer@node"
			h.source.mu.Lock()
			h.source.pins[owner] = []artifact.Ref{root, stale}
			h.source.mu.Unlock()
			h.start()
			deadline := time.Now().Add(5 * time.Second)
			for !slices.Equal(h.source.pinned(owner), want) {
				if time.Now().After(deadline) {
					t.Fatalf("delivery pins = %v, want %v", h.source.pinned(owner), want)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

func TestSharedBatchDropsAfterLastSender(t *testing.T) {
	s := &deliveryScheduler{batches: map[sharedBatchKey]*sharedBatch{}}
	key := sharedBatchKeyFor("stub", []artifact.ObjectEntry{{Digest: artifact.Sum([]byte("x")), Kind: artifact.KindBlob, Size: 1}})
	id := artifact.Sum([]byte("batch"))
	var dropped []artifact.Digest
	drop := func(id artifact.Digest) error {
		dropped = append(dropped, id)
		return nil
	}
	steps := []struct {
		name      string
		hold      bool
		committed artifact.Digest
		want      []artifact.Digest
	}{
		{"first sender holds", true, "", nil},
		{"second sender holds", true, "", nil},
		{"first sender commits while the second still reads", false, id, nil},
		{"second sender fails last and drops the committed batch", false, "", []artifact.Digest{id}},
		{"a later sender holds", true, "", []artifact.Digest{id}},
		{"a failed sender alone never drops", false, "", []artifact.Digest{id}},
	}
	for _, step := range steps {
		if step.hold {
			s.holdBatch(key)
		} else if err := s.releaseBatch(key, step.committed, drop); err != nil {
			t.Fatalf("%s: releaseBatch: %v", step.name, err)
		}
		if !slices.Equal(dropped, step.want) {
			t.Fatalf("%s: dropped = %v, want %v", step.name, dropped, step.want)
		}
	}
	if len(s.batches) != 0 {
		t.Fatalf("batches = %v, want none held", s.batches)
	}
}

func TestDelivererReadmitsAfterReadingEachPart(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	sink := h.sinks["peer@node"]
	h.source.publish(2, h.source.blob([]byte("payload")))
	h.source.onRead = func() {
		h.monitor.mu.Lock()
		defer h.monitor.mu.Unlock()
		h.monitor.state = netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}
	}
	h.start()
	h.await("peer@node", "manual metering pause", pausedWith(delivery.PauseLocalManualMetered))
	if got := sink.count(artifact.MethodBatchPut); got != 0 {
		t.Fatalf("puts = %d, want 0 once metering was enabled during the part read", got)
	}
}

func TestDelivererSupersedesAfterTheFinalBatch(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	sink := h.sinks["peer@node"]
	root := h.source.blob([]byte("payload"))
	h.source.publish(2, root)
	var once sync.Once
	sink.onCommit = func() {
		once.Do(func() {
			h.source.publish(3, root)
			if err := h.sched.Kick("stub", "peer@node"); err != nil {
				t.Error(err)
			}
		})
	}
	h.start()
	h.await("peer@node", "ack of the superseding revision", acked(3))
	if got := sink.count(syncservice.MethodApplyV2); got != 1 {
		t.Fatalf("applies = %d, want 1: the superseded revision must not be applied", got)
	}
}

func TestDelivererCoalescesTheFirstArtifactRun(t *testing.T) {
	h := newDeliveryHarness(t, true, "peer@node")
	artifactMaxWait = 400 * time.Millisecond
	h.source.publish(2, h.source.blob([]byte("payload")))
	started := time.Now()
	h.start()
	h.await("peer@node", "ack", acked(2))
	if waited := time.Since(started); waited < artifactMaxWait {
		t.Fatalf("first artifact run acked after %s, want the %s coalescing window", waited, artifactMaxWait)
	}
}

func TestUnionRootsKeepsManifestReachability(t *testing.T) {
	digest := artifact.Sum([]byte("shared"))
	asBlob := artifact.Ref{Digest: digest, Kind: artifact.KindBlob, Size: 6}
	asManifest := artifact.Ref{Digest: digest, Kind: artifact.KindManifest, Size: 9}
	other := artifact.Ref{Digest: artifact.Sum([]byte("other")), Kind: artifact.KindBlob, Size: 5}
	tests := []struct {
		name      string
		old, next []artifact.Ref
		want      []artifact.Ref
	}{
		{"old manifest", []artifact.Ref{asManifest, other}, []artifact.Ref{asBlob}, []artifact.Ref{asManifest, other}},
		{"next manifest", []artifact.Ref{asBlob}, []artifact.Ref{other, asManifest}, []artifact.Ref{other, asManifest}},
		{"both blobs", []artifact.Ref{asBlob}, []artifact.Ref{asBlob, other}, []artifact.Ref{asBlob, other}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unionRoots(tt.old, tt.next); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("unionRoots = %v, want %v", got, tt.want)
			}
		})
	}
}
