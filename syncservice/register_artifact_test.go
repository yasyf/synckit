package syncservice

import (
	"context"
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
	fail    func(roots []artifact.Ref) error
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
		if err := s.fail(roots); err != nil {
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
	change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(source), []byte(`{}`), applyRoots)
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
	union := refNames(append(slices.Clone(held), applyRoots...))
	acked := func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} }
	tests := []struct {
		name         string
		missing      map[artifact.Digest]int
		result       func(ChangeEnvelope) ApplyResult
		wantErr      string
		wantReady    string
		wantAccepted []artifact.Ref
		wantLog      []string
	}{
		{
			name:         "complete ack",
			result:       acked,
			wantReady:    all,
			wantAccepted: applyRoots,
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin synckit.accepted/host-b " + all,
			},
		},
		{
			name:         "partial keeps prior ack",
			missing:      map[artifact.Digest]int{applyRoots[1].Digest: 2},
			result:       func(ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: NewRevision(1), Partial: true} },
			wantReady:    "[" + a + " " + c + "]",
			wantAccepted: applyRoots,
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + a + " " + c + "]",
				"pin synckit.accepted/host-b " + all,
			},
		},
		{
			name:         "false ack refused",
			missing:      map[artifact.Digest]int{applyRoots[2].Digest: 1},
			result:       acked,
			wantErr:      ErrIncompleteAck.Error() + ": 2 of 3 roots ready",
			wantReady:    "[" + a + " " + b + "]",
			wantAccepted: append(slices.Clone(held), applyRoots...),
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + a + " " + b + "]",
			},
		},
		{
			name: "stale leaves accepted pins",
			result: func(ChangeEnvelope) ApplyResult {
				return ApplyResult{AckedRevision: NewRevision(9), Stale: true, HeldDigest: "d"}
			},
			wantReady:    all,
			wantAccepted: held,
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin synckit.accepted/host-b " + refNames(held),
			},
		},
		{
			name: "stale at the source revision leaves accepted pins",
			result: func(change ChangeEnvelope) ApplyResult {
				return ApplyResult{AckedRevision: change.SourceRevision, Stale: true, HeldDigest: "d"}
			},
			wantReady:    all,
			wantAccepted: held,
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply " + all,
				"pin synckit.accepted/host-b " + refNames(held),
			},
		},
		{
			name:         "need snapshot leaves accepted pins",
			missing:      map[artifact.Digest]int{applyRoots[0].Digest: 1},
			result:       func(ChangeEnvelope) ApplyResult { return ApplyResult{NeedSnapshot: true} },
			wantReady:    "[" + b + " " + c + "]",
			wantAccepted: held,
			wantLog: []string{
				"pin synckit.accepted/host-b " + union,
				"complete " + a, "complete " + b, "complete " + c,
				"apply [" + b + " " + c + "]",
				"pin synckit.accepted/host-b " + refNames(held),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			store := &fakeAcceptStore{missing: tt.missing, pins: map[string][]artifact.Ref{"synckit.accepted/host-b": held}, log: &log}
			consumer := &fakeArtifactConsumer{result: tt.result, log: &log}
			dispatcher := rpc.NewDispatcher()
			registerArtifactConsumer(dispatcher, consumer, store)
			change := artifactChange(t, 3)

			got, err := NewClient(directTransport{dispatcher}).ApplyV2(t.Context(), change)
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
			if !reflect.DeepEqual(store.pins["synckit.accepted/host-b"], tt.wantAccepted) {
				t.Errorf("accepted pins = %s, want %s", refNames(store.pins["synckit.accepted/host-b"]), refNames(tt.wantAccepted))
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
	if got := refNames(store.pins["synckit.accepted/host-b"]); got != refNames(applyRoots) {
		t.Fatalf("accepted pins after a refused ack = %s, want every root still pinned", got)
	}
}

func TestApplyV2KeepsInterruptedAcceptancePinned(t *testing.T) {
	var log []string
	prior := []artifact.Ref{{Digest: artifact.Sum([]byte("held")), Kind: artifact.KindBlob, Size: 4}}
	store := &fakeAcceptStore{pins: map[string][]artifact.Ref{"synckit.accepted/host-b": prior}, log: &log}
	canceled := errors.New("narrowing canceled")
	store.fail = func(roots []artifact.Ref) error {
		if refNames(roots) == refNames(applyRoots) {
			return canceled
		}
		return nil
	}
	consumer := &fakeArtifactConsumer{
		result: func(change ChangeEnvelope) ApplyResult { return ApplyResult{AckedRevision: change.SourceRevision} },
		log:    &log,
	}
	if _, err := applyArtifacts(t.Context(), consumer, store, artifactChange(t, 3)); !errors.Is(err, canceled) {
		t.Fatalf("applyArtifacts(B) = %v, want the narrowing failure", err)
	}
	store.fail = nil
	nextRoots := []artifact.Ref{{Digest: artifact.Sum([]byte("next")), Kind: artifact.KindBlob, Size: 4}}
	next, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(4), []byte(`{"n":1}`), nextRoots)
	if err != nil {
		t.Fatal(err)
	}
	if next, err = BindDelivery(next, "host-b"); err != nil {
		t.Fatal(err)
	}
	consumer.err = errors.New("apply C failed")
	if _, err := applyArtifacts(t.Context(), consumer, store, next); !errors.Is(err, consumer.err) {
		t.Fatalf("applyArtifacts(C) = %v, want the consumer failure", err)
	}
	want := slices.Concat(prior, applyRoots, nextRoots)
	if got := store.pins["synckit.accepted/host-b"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("accepted pins after an interrupted B and a failed C = %s, want %s", refNames(got), refNames(want))
	}
}

func TestArtifactConsumerRefusesArtifactsOnV1(t *testing.T) {
	var log []string
	consumer := &artifactV1Consumer{fakeArtifactConsumer{log: &log}}
	dispatcher := rpc.NewDispatcher()
	registerArtifactConsumer(dispatcher, consumer, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log})
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
	registerArtifactConsumer(dispatcher, &fakeArtifactConsumer{log: &log}, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log})
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
			registerArtifactConsumer(dispatcher, consumer, &fakeAcceptStore{pins: map[string][]artifact.Ref{}, log: &log})

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
		name         string
		roots        []artifact.Ref
		result       func(ChangeEnvelope) ApplyResult
		wantErr      string
		wantReady    []artifact.Ref
		wantAccepted []artifact.Ref
	}{
		{name: "complete ack", roots: []artifact.Ref{presentRef}, result: ack, wantReady: []artifact.Ref{presentRef}, wantAccepted: []artifact.Ref{presentRef}},
		{
			name: "absent root refuses ack", roots: []artifact.Ref{presentRef, absent}, result: ack,
			wantErr: fmt.Sprintf("%s: %v: 1 of 2 roots ready", MethodApplyV2, ErrIncompleteAck), wantReady: []artifact.Ref{presentRef},
			wantAccepted: []artifact.Ref{presentRef, absent},
		},
		{
			name: "absent root partial", roots: []artifact.Ref{presentRef, absent}, result: partial,
			wantReady: []artifact.Ref{presentRef}, wantAccepted: []artifact.Ref{presentRef, absent},
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
			cellular := netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}
			RegisterArtifactConsumer(dispatcher, consumer, store, staticMonitor{state: cellular})
			client := NewClient(directTransport{dispatcher})

			state, err := client.NetStatus(t.Context())
			if err != nil || state != cellular {
				t.Fatalf("NetStatus() = %+v, %v; want %+v", state, err, cellular)
			}
			change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(3), []byte(`{}`), tt.roots)
			if err != nil {
				t.Fatal(err)
			}
			if change, err = BindDelivery(change, "host-b"); err != nil {
				t.Fatal(err)
			}
			got, err := client.ApplyV2(t.Context(), change)
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
			var accepted []artifact.Ref
			for _, pin := range pins {
				if pin.Owner == "synckit.accepted/host-b" {
					accepted = pin.Roots
				}
			}
			if !reflect.DeepEqual(accepted, tt.wantAccepted) {
				t.Errorf("accepted pins = %s, want %s", refNames(accepted), refNames(tt.wantAccepted))
			}
		})
	}
}
