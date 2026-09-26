package artifact

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

type fakeMonitor struct {
	mu    sync.Mutex
	state netpolicy.State
	calls int
}

func (f *fakeMonitor) Current() (netpolicy.State, <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.state, make(chan struct{})
}

func (f *fakeMonitor) Close() error { return nil }

func (f *fakeMonitor) set(state netpolicy.State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
}

var (
	unrestricted = netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}
	cellular     = netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, ObservedAt: time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)}
)

func call[R any](t *testing.T, d *rpc.Dispatcher, method string, params any) (R, error) {
	t.Helper()
	var result R
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("encode %s params: %v", method, err)
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatalf("decode %s params: %v", method, err)
	}
	response := d.Dispatch(t.Context(), &rpc.Request{Method: method, Params: raw})
	if !response.OK {
		return result, errors.New(response.Error)
	}
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode %s result: %v", method, err)
	}
	return result, nil
}

func mustCall[R any](t *testing.T, d *rpc.Dispatcher, method string, params any) R {
	t.Helper()
	result, err := call[R](t, d, method, params)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return result
}

func TestReceiverEvaluatesLivePolicyOnEveryCall(t *testing.T) {
	src, roots, batch := sourceBatch(t, 9, 2*ChunkSize)
	parts := outboxParts(t, src, batch)
	dst := newStore(t)
	monitor := &fakeMonitor{state: unrestricted}
	d := rpc.NewDispatcher()
	Register(d, dst, monitor)

	refused := mustCall[BatchBeginResult](t, d, MethodBatchBegin, BatchBeginParams{Batch: batch, Sender: cellular})
	if want := (BatchBeginResult{HaveParts: []int{}, Peer: unrestricted, Paused: &PausedError{Code: PauseSenderCellular, Reason: "remote: cellular"}}); !reflect.DeepEqual(refused, want) {
		t.Fatalf("begin with a cellular sender = %+v, want %+v", refused, want)
	}
	if entries, err := os.ReadDir(filepath.Join(dst.root, incomingDir)); err != nil || len(entries) != 0 {
		t.Fatalf("refused begin staged %v, %v", entries, err)
	}

	begun := mustCall[BatchBeginResult](t, d, MethodBatchBegin, BatchBeginParams{Batch: batch, Sender: unrestricted})
	if want := (BatchBeginResult{HaveParts: []int{}, Peer: unrestricted}); !reflect.DeepEqual(begun, want) {
		t.Fatalf("begin = %+v, want %+v", begun, want)
	}
	put := mustCall[BatchPutResult](t, d, MethodBatchPut, BatchPutParams{ID: batch.ID, Index: 0, Data: parts[0], Sender: unrestricted})
	if want := (BatchPutResult{Peer: unrestricted}); !reflect.DeepEqual(put, want) {
		t.Fatalf("put 0 = %+v, want %+v", put, want)
	}

	monitor.set(cellular)
	put = mustCall[BatchPutResult](t, d, MethodBatchPut, BatchPutParams{ID: batch.ID, Index: 1, Data: parts[1], Sender: unrestricted})
	if want := (BatchPutResult{Peer: cellular, Paused: &PausedError{Code: PauseReceiverCellular, Reason: "local: cellular"}}); !reflect.DeepEqual(put, want) {
		t.Fatalf("put 1 after the receiver flipped to cellular = %+v, want %+v", put, want)
	}
	if _, err := os.Stat(filepath.Join(dst.root, incomingDir, string(batch.ID), partName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused put wrote part 1: %v", err)
	}
	refused = mustCall[BatchBeginResult](t, d, MethodBatchBegin, BatchBeginParams{Batch: batch, Sender: unrestricted})
	if refused.Paused == nil || refused.Paused.Code != PauseReceiverCellular {
		t.Fatalf("begin while the receiver is cellular = %+v, want receiver-cellular", refused)
	}

	monitor.set(unrestricted)
	resumed := mustCall[BatchBeginResult](t, d, MethodBatchBegin, BatchBeginParams{Batch: batch, Sender: unrestricted})
	if !reflect.DeepEqual(resumed.HaveParts, []int{0}) || resumed.Paused != nil {
		t.Fatalf("resumed begin = %+v, want have_parts [0]", resumed)
	}
	for index := 1; index < len(parts); index++ {
		mustCall[BatchPutResult](t, d, MethodBatchPut, BatchPutParams{ID: batch.ID, Index: index, Data: parts[index], Sender: unrestricted})
	}
	report := mustCall[CommitReport](t, d, MethodBatchCommit, BatchRef{ID: batch.ID})
	if report.Stored != len(batch.Objects) {
		t.Fatalf("commit = %+v, want %d stored", report, len(batch.Objects))
	}
	if err := dst.Verify(t.Context(), roots); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestRegisterServesSourceMethods(t *testing.T) {
	s := newStore(t)
	fixture := buildClosureFixture(t, s)
	monitor := &fakeMonitor{state: unrestricted}
	d := rpc.NewDispatcher()
	Register(d, s, monitor)

	for _, method := range Methods {
		response := d.Dispatch(t.Context(), &rpc.Request{Method: method, Params: map[string]any{}})
		if strings.Contains(response.Error, "unknown method") {
			t.Fatalf("%s is not registered", method)
		}
	}
	if status := mustCall[NetStatusResult](t, d, MethodNetStatus, struct{}{}); !reflect.DeepEqual(status.State, unrestricted) {
		t.Fatalf("net status = %+v, want %+v", status.State, unrestricted)
	}

	var objects []ObjectEntry
	var nexts []int
	for after, done := 0, false; !done; {
		page := mustCall[ClosurePage](t, d, MethodClosure, ClosureParams{Roots: fixture.roots, After: after, Limit: 3})
		if page.TotalObjects != len(fixture.objects) || page.TotalBytes != fixture.bytes {
			t.Fatalf("page totals = %d objects %d bytes, want %d and %d", page.TotalObjects, page.TotalBytes, len(fixture.objects), fixture.bytes)
		}
		objects = append(objects, page.Objects...)
		nexts = append(nexts, page.Next)
		after, done = page.Next, page.Done
	}
	if !reflect.DeepEqual(objects, fixture.objects) || !reflect.DeepEqual(nexts, []int{3, 6, 7}) {
		t.Fatalf("paged closure = %+v nexts %v, want %+v nexts [3 6 7]", objects, nexts, fixture.objects)
	}
	if _, err := call[ClosurePage](t, d, MethodClosure, ClosureParams{Roots: fixture.roots, After: 8, Limit: 3}); err == nil {
		t.Fatal("closure page past the end succeeded")
	}

	absent := Sum([]byte("absent"))
	have := mustCall[HaveResult](t, d, MethodHave, HaveParams{Digests: []Digest{fixture.objects[0].Digest, absent}})
	if !reflect.DeepEqual(have.Missing, []Digest{absent}) {
		t.Fatalf("have = %v, want [%s]", have.Missing, absent)
	}

	entries := []ObjectEntry{fixture.objects[0], fixture.objects[1]}
	built := mustCall[BatchDescriptor](t, d, MethodBatchBuild, BatchBuildParams{Objects: entries})
	direct, err := s.BuildBatch(t.Context(), entries)
	if err != nil || !reflect.DeepEqual(built, direct) {
		t.Fatalf("batch.build = %+v, direct %+v, %v", built, direct, err)
	}
	part := mustCall[BatchReadResult](t, d, MethodBatchRead, BatchReadParams{ID: built.ID, Index: 0})
	if int64(len(part.Data)) != built.Parts[0].Size || Sum(part.Data) != built.Parts[0].Digest {
		t.Fatalf("batch.read returned %d bytes that do not match part 0", len(part.Data))
	}
	mustCall[struct{}](t, d, MethodBatchDrop, BatchRef{ID: built.ID})
	if _, err := call[BatchReadResult](t, d, MethodBatchRead, BatchReadParams{ID: built.ID, Index: 0}); err == nil {
		t.Fatal("batch.read after drop succeeded")
	}

	mustCall[struct{}](t, d, MethodPinsSet, PinsSetParams{Owner: "synckit.delivery/peer", Roots: fixture.roots})
	pins, err := s.Pins(t.Context())
	if err != nil || len(pins) != 1 || pins[0].Owner != "synckit.delivery/peer" || !reflect.DeepEqual(pins[0].Roots, fixture.roots) {
		t.Fatalf("Pins after pins.set = %+v, %v", pins, err)
	}
	if _, err := call[struct{}](t, d, MethodPinsSet, PinsSetParams{Owner: "", Roots: fixture.roots}); err == nil {
		t.Fatal("pins.set with an empty owner succeeded")
	}
}
