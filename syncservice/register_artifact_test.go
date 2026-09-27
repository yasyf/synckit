package syncservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

type fakeAcceptStore struct {
	missing map[artifact.Digest]int
	pins    map[string][]artifact.Ref
	log     *[]string
	fail    func(owner string, roots []artifact.Ref) error
}

func (s *fakeAcceptStore) Complete(_ context.Context, roots []artifact.Ref) (int, error) {
	missing := 0
	for _, root := range roots {
		*s.log = append(*s.log, "complete "+string(root.Digest)[:4])
		missing += s.missing[root.Digest]
	}
	return missing, nil
}

func (s *fakeAcceptStore) SetPins(_ context.Context, owner string, roots []artifact.Ref) error {
	*s.log = append(*s.log, fmt.Sprintf("pin %s %s", owner, refNames(roots)))
	if s.fail != nil {
		if err := s.fail(owner, roots); err != nil {
			return err
		}
	}
	if len(roots) == 0 {
		delete(s.pins, owner)
		return nil
	}
	s.pins[owner] = append([]artifact.Ref(nil), roots...)
	return nil
}

func (s *fakeAcceptStore) Pins(context.Context) ([]artifact.PinSet, error) {
	pins := make([]artifact.PinSet, 0, len(s.pins))
	for owner, roots := range s.pins {
		pins = append(pins, artifact.PinSet{Owner: owner, Roots: roots})
	}
	slices.SortFunc(pins, func(a, b artifact.PinSet) int { return strings.Compare(a.Owner, b.Owner) })
	return pins, nil
}

type fakeArtifactConsumer struct {
	fakeConsumer
	result func(ChangeEnvelope) ApplyResult
	err    error
	ready  []artifact.Ref
	log    *[]string
}

func (*fakeArtifactConsumer) ExportArtifacts(_ context.Context, request ExportRequest) (ChangeEnvelope, error) {
	return NewExportedArtifactChange(request.ServiceID, request.SchemaFingerprint, ChangeSnapshot, NewRevision(0), NewRevision(3), []byte(`{}`), applyRoots)
}

func (f *fakeArtifactConsumer) ApplyArtifacts(_ context.Context, change ChangeEnvelope, ready []artifact.Ref) (ApplyResult, error) {
	f.ready = ready
	*f.log = append(*f.log, "apply "+refNames(ready))
	if f.err != nil {
		return ApplyResult{}, f.err
	}
	return f.result(change), nil
}

var applyRoots = []artifact.Ref{
	{Digest: artifact.Sum([]byte("a")), Kind: artifact.KindBlob, Size: 1},
	{Digest: artifact.Sum([]byte("b")), Kind: artifact.KindBlob, Size: 1},
	{Digest: artifact.Sum([]byte("c")), Kind: artifact.KindManifest, Size: 9},
}

func refNames(roots []artifact.Ref) string {
	names := make([]string, 0, len(roots))
	for _, root := range roots {
		names = append(names, string(root.Digest)[:4])
	}
	return "[" + strings.Join(names, " ") + "]"
}

func artifactChange(t *testing.T, source uint64) ChangeEnvelope {
	t.Helper()
	return artifactChangeOf(t, source, applyRoots)
}

func artifactChangeOf(t *testing.T, source uint64, roots []artifact.Ref) ChangeEnvelope {
	t.Helper()
	change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(source), []byte(`{}`), roots)
	if err != nil {
		t.Fatal(err)
	}
	change, err = BindDelivery(change, "host-b")
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func TestApplyV2ReadinessPinsAndAck(t *testing.T) {
	a, b, c := string(applyRoots[0].Digest)[:4], string(applyRoots[1].Digest)[:4], string(applyRoots[2].Digest)[:4]
	all := refNames(applyRoots)
	held := []artifact.Ref{{Digest: artifact.Sum([]byte("held")), Kind: artifact.KindBlob, Size: 4}}
	change := artifactChange(t, 3)
	owner := acceptedPinPrefix + "host-b/" + change.ChangeID
	acked := func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} }
	tests := []struct {
		name       string
		missing    map[artifact.Digest]int
		result     func(ChangeEnvelope) ApplyResult
		wantErr    string
		wantReady  string
		wantPinned []artifact.Ref
		wantLog    []string
	}{
		{
			name:       "complete ack",
			result:     acked,
			wantReady:  all,
			wantPinned: applyRoots,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin synckit.acked/host-b " + all,
				"pin " + owner + " []",
			},
		},
		{
			name:       "partial keeps prior ack",
			missing:    map[artifact.Digest]int{applyRoots[1].Digest: 2},
			result:     func(ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: NewRevision(1), Partial: true} },
			wantReady:  "[" + a + " " + c + "]",
			wantPinned: applyRoots,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + a + " " + c + "]",
				"pin synckit.acked/host-b " + all,
				"pin " + owner + " []",
			},
		},
		{
			name:       "false ack refused",
			missing:    map[artifact.Digest]int{applyRoots[2].Digest: 1},
			result:     acked,
			wantErr:    ErrIncompleteAck.Error() + ": 2 of 3 roots ready",
			wantReady:  "[" + a + " " + b + "]",
			wantPinned: append(slices.Clone(held), applyRoots...),
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + a + " " + b + "]",
			},
		},
		{
			name: "stale releases the change",
			result: func(ChangeEnvelope) ApplyResult {
				return ApplyResult{AckedRevision: NewRevision(9), Stale: true, HeldDigest: "d"}
			},
			wantReady:  all,
			wantPinned: held,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin " + owner + " []",
			},
		},
		{
			name: "stale at the source revision releases the change",
			result: func(change ChangeEnvelope) ApplyResult {
				return ApplyResult{AckedRevision: change.SourceRevision, Stale: true, HeldDigest: "d"}
			},
			wantReady:  all,
			wantPinned: held,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin " + owner + " []",
			},
		},
		{
			name: "stale holding this change finalizes its ack",
			result: func(change ChangeEnvelope) ApplyResult {
				return ApplyResult{AckedRevision: change.SourceRevision, Stale: true, HeldDigest: change.PayloadDigest}
			},
			wantReady:  all,
			wantPinned: applyRoots,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin synckit.acked/host-b " + all,
				"pin " + owner + " []",
			},
		},
		{
			name:       "need snapshot releases the change",
			missing:    map[artifact.Digest]int{applyRoots[0].Digest: 1},
			result:     func(ChangeEnvelope) ApplyResult { return ApplyResult{NeedSnapshot: true} },
			wantReady:  "[" + b + " " + c + "]",
			wantPinned: held,
			wantLog: []string{
				"pin " + owner + " " + all,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + b + " " + c + "]",
				"pin " + owner + " []",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			store := &fakeAcceptStore{missing: tt.missing, pins: map[string][]artifact.Ref{"synckit.acked/host-b": held}, log: &log}
			consumer := &fakeArtifactConsumer{result: tt.result, log: &log}
			dispatcher := rpc.NewDispatcher()
			registerArtifactConsumer(dispatcher, consumer, store, staticMonitor{state: connectedState})

			got, err := NewClient(directTransport{dispatcher}).ApplyV2(t.Context(), change, connectedState.RestrictedEpoch)
			if tt.wantErr != "" {
				if err == nil || err.Error() != MethodApplyV2+": "+tt.wantErr {
					t.Fatalf("ApplyV2() = %+v, %v; want error %q", got, err, tt.wantErr)
				}
			} else if want := tt.result(change); err != nil || got != want {
				t.Fatalf("ApplyV2() = %+v, %v; want %+v", got, err, want)
			}
			if refNames(consumer.ready) != tt.wantReady {
				t.Errorf("consumer ready = %s, want %s", refNames(consumer.ready), tt.wantReady)
			}
			if got, want := pinnedUnion(store), sortedNames(tt.wantPinned); got != want {
				t.Errorf("pinned roots = %s, want %s", got, want)
			}
			if !reflect.DeepEqual(log, tt.wantLog) {
				t.Errorf("call log = %q, want %q", log, tt.wantLog)
			}
		})
	}
}

func TestApplyV2FalseAckIsIncompleteAck(t *testing.T) {
	var log []string
	store := &fakeAcceptStore{missing: map[artifact.Digest]int{applyRoots[0].Digest: 1}, pins: map[string][]artifact.Ref{}, log: &log}
	consumer := &fakeArtifactConsumer{
		result: func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} },
		log:    &log,
	}
	_, err := applyArtifacts(t.Context(), consumer, store, artifactChange(t, 3))
	if !errors.Is(err, ErrIncompleteAck) {
		t.Fatalf("applyArtifacts() = %v, want ErrIncompleteAck", err)
	}
	if got := pinnedUnion(store); got != sortedNames(applyRoots) {
		t.Fatalf("pinned roots after a refused ack = %s, want every root still pinned", got)
	}
}

func TestApplyV2ReleasesFailedChangesAndKeepsInterruptedAcks(t *testing.T) {
	var log []string
	held := []artifact.Ref{{Digest: artifact.Sum([]byte("held")), Kind: artifact.KindBlob, Size: 4}}
	store := &fakeAcceptStore{pins: map[string][]artifact.Ref{"synckit.acked/host-b": held}, log: &log}
	consumer := &fakeArtifactConsumer{
		result: func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} },
		log:    &log,
	}
	rootsOf := func(label string) []artifact.Ref {
		return []artifact.Ref{
			{Digest: artifact.Sum([]byte(label + "-1")), Kind: artifact.KindBlob, Size: 4},
			{Digest: artifact.Sum([]byte(label + "-2")), Kind: artifact.KindManifest, Size: 9},
		}
	}
	assertPins := func(step string, wantPinned, wantAcked []artifact.Ref) {
		t.Helper()
		if got, want := pinnedUnion(store), sortedNames(wantPinned); got != want {
			t.Fatalf("%s: pinned roots = %s, want %s", step, got, want)
		}
		if got := store.pins["synckit.acked/host-b"]; !reflect.DeepEqual(got, wantAcked) {
			t.Fatalf("%s: acked pins = %s, want %s", step, refNames(got), refNames(wantAcked))
		}
	}

	failed := errors.New("apply failed")
	consumer.err = failed
	for i := range 5 {
		roots := rootsOf(fmt.Sprintf("failing-%d", i))
		if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, uint64(10+i), roots)); !errors.Is(err, failed) {
			t.Fatalf("failing apply %d = %v, want the consumer failure", i, err)
		}
		assertPins(fmt.Sprintf("failing apply %d", i), held, held)
	}
	consumer.err = nil

	recordFailed := errors.New("record acked roots failed")
	store.fail = func(owner string, _ []artifact.Ref) error {
		if owner == ackedPinPrefix+"host-b" {
			return recordFailed
		}
		return nil
	}
	interrupted := artifactChange(t, 20)
	if _, err := applyArtifacts(t.Context(), consumer, store, interrupted); !errors.Is(err, recordFailed) {
		t.Fatalf("interrupted ack = %v, want the record failure", err)
	}
	store.fail = nil
	assertPins("interrupted ack", slices.Concat(held, applyRoots), held)

	consumer.err = failed
	if _, err := applyArtifacts(t.Context(), consumer, store, interrupted); !errors.Is(err, failed) {
		t.Fatalf("failing retry of the interrupted ack = %v, want the consumer failure", err)
	}
	assertPins("failing retry of the interrupted ack", slices.Concat(held, applyRoots), held)
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 21, rootsOf("next"))); !errors.Is(err, failed) {
		t.Fatalf("apply after an interrupted ack = %v, want the consumer failure", err)
	}
	assertPins("apply after an interrupted ack", slices.Concat(held, applyRoots), held)
	consumer.err = nil

	consumer.result = func(ChangeEnvelope) ApplyResult { return ApplyResult{NeedSnapshot: true} }
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 22, rootsOf("snapshot"))); err != nil {
		t.Fatal(err)
	}
	assertPins("need snapshot", slices.Concat(held, applyRoots), held)

	consumer.result = func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} }
	final := rootsOf("final")
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 23, final)); err != nil {
		t.Fatal(err)
	}
	assertPins("ack", final, final)

	consumer.err = failed
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 24, rootsOf("later"))); !errors.Is(err, failed) {
		t.Fatalf("apply after the ack = %v, want the consumer failure", err)
	}
	assertPins("failing apply after the ack", final, final)
}

func TestArtifactConsumerRefusesArtifactsOnV1(t *testing.T) {
	var log []string
	consumer := &artifactV1Consumer{fakeArtifactConsumer{log: &log}}
	dispatcher := rpc.NewDispatcher()
	registerArtifactConsumer(dispatcher, consumer, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}, staticMonitor{state: connectedState})
	params, err := structParams(artifactChange(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	request := ExportRequest{ServiceID: "fake", SchemaFingerprint: testSchema, SinceRevision: NewRevision(0)}
	exportParams, err := structParams(request)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		method string
		params map[string]any
	}{
		{"apply.v1", MethodApply, params},
		{"export.v1", MethodExport, exportParams},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := dispatcher.Dispatch(t.Context(), &rpc.Request{Method: tt.method, Params: tt.params})
			if response.OK || response.Error != ErrArtifactsOnV1.Error() {
				t.Fatalf("%s = %+v, want %q", tt.method, response, ErrArtifactsOnV1)
			}
		})
	}
	if consumer.applyOrigin != "" || len(log) != 0 {
		t.Fatalf("v1 refusal reached the consumer: origin %q, log %q", consumer.applyOrigin, log)
	}
}

type artifactV1Consumer struct{ fakeArtifactConsumer }

func (*artifactV1Consumer) Export(_ context.Context, request ExportRequest) (ChangeEnvelope, error) {
	return NewExportedArtifactChange(request.ServiceID, request.SchemaFingerprint, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), applyRoots)
}

func TestExportV2ReturnsArtifactRoots(t *testing.T) {
	var log []string
	dispatcher := rpc.NewDispatcher()
	registerArtifactConsumer(dispatcher, &fakeArtifactConsumer{log: &log}, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}, staticMonitor{state: connectedState})
	request := ExportRequest{ServiceID: "fake", SchemaFingerprint: testSchema, SinceRevision: NewRevision(0)}

	change, err := NewClient(directTransport{dispatcher}).ExportV2(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if change.SourceRevision != NewRevision(3) || !reflect.DeepEqual(change.Artifacts, applyRoots) {
		t.Fatalf("ExportV2() = %+v", change)
	}
}

func TestArtifactConsumerCapabilities(t *testing.T) {
	want := ArtifactCapabilities("fake").Methods
	tests := []struct {
		name    string
		methods []string
		want    []string
	}{
		{"default methods gain v2 and artifact methods", DefaultCapabilities("fake").Methods, want},
		{"artifact methods stay unduplicated", ArtifactCapabilities("fake").Methods, want},
		{"consumer extras keep their place", []string{"svc.custom", MethodList}, append([]string{"svc.custom", MethodList}, withoutMethods(want, MethodList)...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			consumer := &capabilityConsumer{fakeArtifactConsumer: fakeArtifactConsumer{log: &log}, methods: tt.methods}
			dispatcher := rpc.NewDispatcher()
			registerArtifactConsumer(dispatcher, consumer, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}, staticMonitor{state: connectedState})

			got, err := NewClient(directTransport{dispatcher}).Capabilities(t.Context())
			if err != nil || got.Name != "fake" || !reflect.DeepEqual(got.Methods, tt.want) {
				t.Fatalf("Capabilities() = %+v, %v; want methods %q", got, err, tt.want)
			}
		})
	}
}

type capabilityConsumer struct {
	fakeArtifactConsumer
	methods []string
}

func (c *capabilityConsumer) Capabilities(context.Context) (Capabilities, error) {
	return Capabilities{Name: "fake", Methods: c.methods}, nil
}

func withoutMethods(methods []string, drop string) []string {
	kept := make([]string, 0, len(methods))
	for _, method := range methods {
		if method != drop {
			kept = append(kept, method)
		}
	}
	return kept
}

var connectedState = netpolicy.State{Status: netpolicy.StatusConnected}

type staticMonitor struct{ state netpolicy.State }

func (m staticMonitor) Current() (netpolicy.State, <-chan struct{}) { return m.state, nil }

func (staticMonitor) Close() error { return nil }

func TestRegisterArtifactConsumerWithStore(t *testing.T) {
	present := []byte("present root")
	absent := artifact.Ref{Digest: artifact.Sum([]byte("absent root")), Kind: artifact.KindBlob, Size: 11}
	presentRef := artifact.Ref{Digest: artifact.Sum(present), Kind: artifact.KindBlob, Size: int64(len(present))}
	ack := func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} }
	partial := func(ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: NewRevision(0), Partial: true} }
	tests := []struct {
		name       string
		roots      []artifact.Ref
		result     func(ChangeEnvelope) ApplyResult
		wantErr    string
		wantReady  []artifact.Ref
		wantPinned []artifact.Ref
	}{
		{name: "complete ack", roots: []artifact.Ref{presentRef}, result: ack, wantReady: []artifact.Ref{presentRef}, wantPinned: []artifact.Ref{presentRef}},
		{
			name: "absent root refuses ack", roots: []artifact.Ref{presentRef, absent}, result: ack,
			wantErr: fmt.Sprintf("%s: %v: 1 of 2 roots ready", MethodApplyV2, ErrIncompleteAck), wantReady: []artifact.Ref{presentRef},
			wantPinned: []artifact.Ref{presentRef, absent},
		},
		{
			name: "absent root partial", roots: []artifact.Ref{presentRef, absent}, result: partial,
			wantReady: []artifact.Ref{presentRef}, wantPinned: []artifact.Ref{presentRef, absent},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := artifact.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.PutBlob(t.Context(), present); err != nil {
				t.Fatal(err)
			}
			var log []string
			consumer := &fakeArtifactConsumer{result: tt.result, log: &log}
			dispatcher := rpc.NewDispatcher()
			RegisterArtifactConsumer(dispatcher, consumer, store, staticMonitor{state: connectedState})
			client := NewClient(directTransport{dispatcher})

			state, err := client.NetStatus(t.Context())
			if err != nil || state != connectedState {
				t.Fatalf("NetStatus() = %+v, %v; want %+v", state, err, connectedState)
			}
			change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(3), []byte(`{}`), tt.roots)
			if err != nil {
				t.Fatal(err)
			}
			if change, err = BindDelivery(change, "host-b"); err != nil {
				t.Fatal(err)
			}
			got, err := client.ApplyV2(t.Context(), change, state.RestrictedEpoch)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("ApplyV2() = %+v, %v; want error %q", got, err, tt.wantErr)
				}
			} else if want := tt.result(change); err != nil || got != want {
				t.Fatalf("ApplyV2() = %+v, %v; want %+v", got, err, want)
			}
			if !reflect.DeepEqual(consumer.ready, tt.wantReady) {
				t.Errorf("consumer ready = %s, want %s", refNames(consumer.ready), refNames(tt.wantReady))
			}
			pins, err := store.Pins(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var pinned []artifact.Ref
			for _, pin := range pins {
				pinned = append(pinned, pin.Roots...)
			}
			if got, want := sortedNames(pinned), sortedNames(tt.wantPinned); got != want {
				t.Errorf("pinned roots = %s, want %s", got, want)
			}
		})
	}
}

func TestApplyV2RefusesBeforeDecoding(t *testing.T) {
	tests := []struct {
		name     string
		live     netpolicy.State
		admitted uint64
		want     artifact.PauseCode
	}{
		{"while the receiver is restricted", netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, RestrictedEpoch: 4}, 4, artifact.PauseReceiverCellular},
		{"once the receiver's epoch moved", netpolicy.State{Status: netpolicy.StatusConnected, RestrictedEpoch: 4}, 3, artifact.PauseReceiverRestrictedMidTransfer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			dispatcher := rpc.NewDispatcher()
			registerArtifactConsumer(dispatcher, &fakeArtifactConsumer{log: &log}, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}, staticMonitor{state: tt.live})
			response := dispatcher.Dispatch(t.Context(), &rpc.Request{Method: MethodApplyV2, Params: map[string]any{"kind": 7, artifact.AdmittedParam: tt.admitted}})
			if !response.OK {
				t.Fatalf("apply.v2 = %+v, want the typed refusal", response)
			}
			var result ApplyResult
			if err := json.Unmarshal(response.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.Paused == nil || result.Paused.Code != tt.want || len(log) != 0 {
				t.Fatalf("apply.v2 = %+v with log %v, want a %s refusal and no store or consumer call", result, log, tt.want)
			}
		})
	}
}

func TestArtifactChangesMustBeSnapshots(t *testing.T) {
	tests := []struct {
		name    string
		kind    ChangeKind
		base    uint64
		roots   []artifact.Ref
		wantErr bool
	}{
		{"snapshot with artifacts", ChangeSnapshot, 0, applyRoots, false},
		{"delta with artifacts", ChangeDelta, 1, applyRoots, true},
		{"delta without artifacts", ChangeDelta, 1, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			change, err := NewExportedChange("fake", testSchema, tt.kind, NewRevision(tt.base), NewRevision(2), []byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			change.Artifacts = tt.roots
			if err := change.Validate(false); (err != nil) != tt.wantErr {
				t.Fatalf("Validate = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestApplyV2KeepsCommittedRootsAfterAnInterruptedAck(t *testing.T) {
	rootsOf := func(label string) []artifact.Ref {
		return []artifact.Ref{
			{Digest: artifact.Sum([]byte(label + "-1")), Kind: artifact.KindBlob, Size: 4},
			{Digest: artifact.Sum([]byte(label + "-2")), Kind: artifact.KindManifest, Size: 9},
		}
	}
	held := rootsOf("held")
	committed := rootsOf("committed")
	failed := errors.New("apply failed")
	tests := []struct {
		name   string
		result func(ChangeEnvelope) ApplyResult
		err    error
	}{
		{name: "next apply fails", err: failed},
		{name: "next apply refused", result: func(ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: NewRevision(20)} }},
		{name: "next apply stale", result: func(ChangeEnvelope) ApplyResult {
			return ApplyResult{AckedRevision: NewRevision(20), Stale: true, HeldDigest: "d"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			store := &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}
			consumer := &fakeArtifactConsumer{
				result: func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} },
				log:    &log,
			}
			if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 10, held)); err != nil {
				t.Fatal(err)
			}
			recordFailed := errors.New("record acked roots failed")
			store.fail = func(owner string, roots []artifact.Ref) error {
				if owner == ackedPinPrefix+"host-b" && refNames(roots) == refNames(committed) {
					return recordFailed
				}
				return nil
			}
			if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 20, committed)); !errors.Is(err, recordFailed) {
				t.Fatalf("interrupted ack = %v, want the record failure", err)
			}
			store.fail = nil
			consumer.result, consumer.err = tt.result, tt.err
			if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 30, rootsOf("next"))); !errors.Is(err, tt.err) {
				t.Fatalf("next apply = %v, want %v", err, tt.err)
			}
			if got, want := pinnedUnion(store), sortedNames(slices.Concat(held, committed)); got != want {
				t.Fatalf("pinned roots = %s, want %s: the consumer holds the interrupted change", got, want)
			}
			consumer.result = func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} }
			consumer.err = nil
			final := rootsOf("final")
			if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 40, final)); err != nil {
				t.Fatal(err)
			}
			if got := pinnedUnion(store); got != sortedNames(final) {
				t.Fatalf("pinned roots after the next ack = %s, want %s", got, refNames(final))
			}
		})
	}
}

func pinnedUnion(store *fakeAcceptStore) string {
	pins, _ := store.Pins(context.Background())
	seen := map[artifact.Digest]bool{}
	var union []artifact.Ref
	for _, pin := range pins {
		for _, root := range pin.Roots {
			if !seen[root.Digest] {
				seen[root.Digest] = true
				union = append(union, root)
			}
		}
	}
	return sortedNames(union)
}

func sortedNames(refs []artifact.Ref) string {
	sorted := slices.Clone(refs)
	slices.SortFunc(sorted, func(a, b artifact.Ref) int { return strings.Compare(string(a.Digest), string(b.Digest)) })
	return refNames(sorted)
}

func TestApplyV2PinsKeepAManifestRootSharingABlobDigest(t *testing.T) {
	var log []string
	store := &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log}
	consumer := &fakeArtifactConsumer{
		result: func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} },
		log:    &log,
	}
	digest := artifact.Sum([]byte("shared"))
	asBlob := []artifact.Ref{{Digest: digest, Kind: artifact.KindBlob, Size: 6}}
	asManifest := []artifact.Ref{{Digest: digest, Kind: artifact.KindManifest, Size: 9}}
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 10, asBlob)); err != nil {
		t.Fatal(err)
	}
	consumer.err = errors.New("apply failed")
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 20, asManifest)); err == nil {
		t.Fatal("apply = nil, want the consumer failure")
	}
	consumer.err = nil
	var seen []artifact.Ref
	consumer.result = func(change ChangeEnvelope) ApplyResult {
		pins, _ := store.Pins(context.Background())
		for _, pin := range pins {
			seen = append(seen, pin.Roots...)
		}
		return ApplyResult{AckedRevision: change.SourceRevision}
	}
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChangeOf(t, 30, asManifest)); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(seen, asManifest[0]) {
		t.Fatalf("pins during the manifest apply = %v, want the manifest interpretation of %s", seen, digest)
	}
}
