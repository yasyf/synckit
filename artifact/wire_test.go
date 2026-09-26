package artifact

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/synckit/netpolicy"
)

func TestMethods(t *testing.T) {
	want := []string{
		"synckit.net.status.v1",
		"synckit.artifact.closure.v1",
		"synckit.artifact.have.v1",
		"synckit.artifact.batch.build.v1",
		"synckit.artifact.batch.read.v1",
		"synckit.artifact.batch.drop.v1",
		"synckit.artifact.batch.begin.v1",
		"synckit.artifact.batch.put.v1",
		"synckit.artifact.batch.commit.v1",
		"synckit.artifact.pins.set.v1",
	}
	if !reflect.DeepEqual(Methods, want) {
		t.Fatalf("Methods = %v, want %v", Methods, want)
	}
}

func TestPausedFor(t *testing.T) {
	open := netpolicy.State{Status: netpolicy.StatusConnected}
	tests := []struct {
		name     string
		receiver netpolicy.State
		sender   netpolicy.State
		want     PauseCode
	}{
		{"receiver disconnected", netpolicy.State{Status: netpolicy.StatusDisconnected}, open, PauseReceiverDisconnected},
		{"receiver unknown", netpolicy.State{}, open, PauseReceiverUnknown},
		{"receiver cellular", netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, Expensive: true}, open, PauseReceiverCellular},
		{"receiver expensive", netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true}, open, PauseReceiverExpensive},
		{"receiver constrained", netpolicy.State{Status: netpolicy.StatusConnected, Constrained: true}, open, PauseReceiverConstrained},
		{"receiver manual metered", netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}, open, PauseReceiverManualMetered},
		{"sender disconnected", open, netpolicy.State{Status: netpolicy.StatusDisconnected}, PauseSenderDisconnected},
		{"sender absent", open, netpolicy.State{}, PauseSenderUnknown},
		{"sender cellular", open, netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}, PauseSenderCellular},
		{"sender expensive", open, netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true}, PauseSenderExpensive},
		{"sender constrained", open, netpolicy.State{Status: netpolicy.StatusConnected, Constrained: true}, PauseSenderConstrained},
		{"sender manual metered", open, netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}, PauseSenderManualMetered},
		{"receiver blocks first", netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}, netpolicy.State{}, PauseReceiverCellular},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verdict := netpolicy.Evaluate(tt.receiver, tt.sender)
			got := PausedFor(verdict)
			if got.Code != tt.want || got.Reason != verdict.Reason {
				t.Fatalf("PausedFor(%+v) = %+v, want code %s", verdict, got, tt.want)
			}
			if err := got.Code.Validate(); err != nil {
				t.Fatalf("Code.Validate() = %v", err)
			}
		})
	}
}

func TestPausedForPanicsOnAllowed(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("PausedFor(allowed) did not panic")
		}
	}()
	open := netpolicy.State{Status: netpolicy.StatusConnected}
	_ = PausedFor(netpolicy.Evaluate(open, open))
}

func TestPauseCodeValidate(t *testing.T) {
	for _, code := range []PauseCode{"", "receiver-", "peer-cellular", "local-cellular", "receiver-wifi"} {
		if err := code.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("PauseCode(%q).Validate() = %v, want ErrInvalid", code, err)
		}
	}
}

func TestPausedErrorWire(t *testing.T) {
	result := BatchPutResult{
		Peer:   netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, ObservedAt: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)},
		Paused: &PausedError{Code: PauseReceiverCellular, Reason: "local: cellular"},
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"peer":{"status":"connected","expensive":false,"constrained":false,"cellular":true,"manual_metered":false,"observed_at":"2026-09-26T00:00:00Z"},` +
		`"paused":{"code":"receiver-cellular","reason":"local: cellular"}}`
	if string(encoded) != want {
		t.Fatalf("json = %s, want %s", encoded, want)
	}
	var paused *PausedError
	if err := error(result.Paused); !errors.As(err, &paused) || err.Error() != "artifact: receiver paused bulk transfer (receiver-cellular): local: cellular" {
		t.Fatalf("PausedError = %v", err)
	}
	open, err := json.Marshal(BatchBeginResult{HaveParts: []int{0, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"have_parts":[0,2],"peer":{"status":"","expensive":false,"constrained":false,"cellular":false,"manual_metered":false,"observed_at":"0001-01-01T00:00:00Z"}}`; string(open) != want {
		t.Fatalf("begin json = %s, want %s", open, want)
	}
}

func TestParamsValidate(t *testing.T) {
	descriptor, err := NewBatchDescriptor(178, testObjects(), []PartRef{{digestPart, 50}})
	if err != nil {
		t.Fatal(err)
	}
	roots := []Ref{{digestOne, KindManifest, 10}}
	tooManyDigests := make([]Digest, MaxHaveDigests+1)
	for i := range tooManyDigests {
		tooManyDigests[i] = digestOne
	}
	tests := []struct {
		name   string
		params interface{ Validate() error }
		valid  bool
	}{
		{"closure", ClosureParams{Roots: roots, After: 3, Limit: MaxClosurePage}, true},
		{"closure no roots", ClosureParams{Limit: 1}, false},
		{"closure duplicate roots", ClosureParams{Roots: []Ref{roots[0], roots[0]}, Limit: 1}, false},
		{"closure negative after", ClosureParams{Roots: roots, After: -1, Limit: 1}, false},
		{"closure zero limit", ClosureParams{Roots: roots}, false},
		{"closure oversize limit", ClosureParams{Roots: roots, Limit: MaxClosurePage + 1}, false},
		{"have", HaveParams{Digests: []Digest{digestOne, digestTwo}}, true},
		{"have empty", HaveParams{}, true},
		{"have too many", HaveParams{Digests: tooManyDigests}, false},
		{"have bad digest", HaveParams{Digests: []Digest{"x"}}, false},
		{"build", BatchBuildParams{Objects: []Digest{digestOne, digestTwo}}, true},
		{"build empty", BatchBuildParams{}, false},
		{"build duplicate", BatchBuildParams{Objects: []Digest{digestOne, digestOne}}, false},
		{"build bad digest", BatchBuildParams{Objects: []Digest{"x"}}, false},
		{"ref", BatchRef{ID: digestOne}, true},
		{"ref bad id", BatchRef{}, false},
		{"read", BatchReadParams{ID: digestOne, Index: 0}, true},
		{"read negative index", BatchReadParams{ID: digestOne, Index: -1}, false},
		{"read index past parts", BatchReadParams{ID: digestOne, Index: maxBatchParts}, false},
		{"begin", BatchBeginParams{Batch: descriptor}, true},
		{"begin tampered", BatchBeginParams{Batch: BatchDescriptor{ID: descriptor.ID, Schema: BatchSchema, Codec: BatchCodec, RawSize: 178, Objects: testObjects()}}, false},
		{"put", BatchPutParams{ID: digestOne, Index: 1, Data: make([]byte, PartSize)}, true},
		{"put empty", BatchPutParams{ID: digestOne}, false},
		{"put oversize", BatchPutParams{ID: digestOne, Data: make([]byte, PartSize+1)}, false},
		{"put negative index", BatchPutParams{ID: digestOne, Index: -1, Data: []byte{1}}, false},
		{"pins", PinsSetParams{Owner: "synckit.delivery/host-b", Roots: roots}, true},
		{"pins clear", PinsSetParams{Owner: "synckit.accepted/host-a"}, true},
		{"pins no owner", PinsSetParams{Roots: roots}, false},
		{"pins newline owner", PinsSetParams{Owner: "a\nb"}, false},
		{"pins invalid root", PinsSetParams{Owner: "o", Roots: []Ref{{digestOne, KindBlob, 0}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func TestParamsJSON(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"closure", ClosureParams{Roots: []Ref{{digestOne, KindBlob, 1}}, After: 2, Limit: 3}, `{"roots":[{"digest":"` + string(digestOne) + `","kind":"blob","size":1}],"after":2,"limit":3}`},
		{"closure page", ClosurePage{Objects: []ObjectEntry{{digestOne, KindBlob, 1}}, Next: 1, Done: true, TotalObjects: 1, TotalBytes: 1}, `{"objects":[{"digest":"` + string(digestOne) + `","kind":"blob","size":1}],"next":1,"done":true,"total_objects":1,"total_bytes":1}`},
		{"have", HaveParams{Digests: []Digest{digestOne}}, `{"digests":["` + string(digestOne) + `"]}`},
		{"have result", HaveResult{Missing: []Digest{digestOne}}, `{"missing":["` + string(digestOne) + `"]}`},
		{"build", BatchBuildParams{Objects: []Digest{digestOne}}, `{"objects":["` + string(digestOne) + `"]}`},
		{"read", BatchReadParams{ID: digestOne, Index: 4}, `{"id":"` + string(digestOne) + `","index":4}`},
		{"read result", BatchReadResult{Data: []byte("hi")}, `{"data":"aGk="}`},
		{"net status", NetStatusResult{}, `{"state":{"status":"","expensive":false,"constrained":false,"cellular":false,"manual_metered":false,"observed_at":"0001-01-01T00:00:00Z"}}`},
		{"pins", PinsSetParams{Owner: "o"}, `{"owner":"o","roots":null}`},
	}
	for _, tt := range tests {
		encoded, err := json.Marshal(tt.value)
		if err != nil || string(encoded) != tt.want {
			t.Errorf("%s: json = %s, %v; want %s", tt.name, encoded, err, tt.want)
		}
	}
}
