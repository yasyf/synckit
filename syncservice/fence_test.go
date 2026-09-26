package syncservice

import (
	"encoding/json"
	"testing"
)

func fenceChange(t *testing.T, kind ChangeKind, base, source uint64, payload string) ChangeEnvelope {
	t.Helper()
	change, err := NewExportedArtifactChange("fake", testSchema, kind, NewRevision(base), NewRevision(source), []byte(payload), testRoots)
	if err != nil {
		t.Fatal(err)
	}
	change, err = BindDelivery(change, "host-a")
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func TestFence(t *testing.T) {
	heldChange := fenceChange(t, ChangeSnapshot, 0, 5, `{"v":5}`)
	held := heldChange.Receipt()
	tests := []struct {
		name     string
		held     *Receipt
		change   ChangeEnvelope
		want     FenceDecision
		wantAck  ApplyResult
		wantFail bool
	}{
		{name: "no receipt snapshot applies", change: fenceChange(t, ChangeSnapshot, 0, 1, `{}`), want: FenceApply},
		{name: "no receipt delta from zero applies", change: fenceChange(t, ChangeDelta, 0, 1, `{}`), want: FenceApply},
		{name: "no receipt delta from later base needs snapshot", change: fenceChange(t, ChangeDelta, 3, 4, `{}`), want: FenceNeedSnapshot, wantAck: ApplyResult{NeedSnapshot: true}},
		{name: "exact replay", held: &held, change: heldChange, want: FenceReplay, wantAck: ApplyResult{AckedRevision: "5"}},
		{name: "equal source different payload is stale", held: &held, change: fenceChange(t, ChangeSnapshot, 0, 5, `{"v":"other"}`), want: FenceStale, wantAck: ApplyResult{AckedRevision: "5", Stale: true, HeldDigest: held.PayloadDigest}},
		{name: "older source is stale", held: &held, change: fenceChange(t, ChangeSnapshot, 0, 4, `{}`), want: FenceStale, wantAck: ApplyResult{AckedRevision: "5", Stale: true, HeldDigest: held.PayloadDigest}},
		{name: "newer snapshot applies", held: &held, change: fenceChange(t, ChangeSnapshot, 0, 9, `{}`), want: FenceApply},
		{name: "delta on held base applies", held: &held, change: fenceChange(t, ChangeDelta, 5, 6, `{}`), want: FenceApply},
		{name: "delta on other base needs snapshot", held: &held, change: fenceChange(t, ChangeDelta, 4, 6, `{}`), want: FenceNeedSnapshot, wantAck: ApplyResult{NeedSnapshot: true}},
		{name: "receipt from another origin fails", held: &Receipt{Origin: "host-b", ChangeID: held.ChangeID, Revision: "5", PayloadDigest: held.PayloadDigest}, change: heldChange, wantFail: true},
		{name: "corrupt held revision fails", held: &Receipt{Origin: "host-a", Revision: "05"}, change: heldChange, wantFail: true},
		{name: "unbound change fails", change: func() ChangeEnvelope { c := heldChange; c.ChangeID = ""; return c }(), wantFail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, ack, err := Fence(tt.held, tt.change)
			if tt.wantFail {
				if err == nil {
					t.Fatalf("Fence() = %v, %+v, want error", decision, ack)
				}
				return
			}
			if err != nil || decision != tt.want || ack != tt.wantAck {
				t.Fatalf("Fence() = %v, %+v, %v; want %v, %+v", decision, ack, err, tt.want, tt.wantAck)
			}
		})
	}
}

func TestReceipt(t *testing.T) {
	change := fenceChange(t, ChangeSnapshot, 0, 7, `{}`)
	want := Receipt{Origin: "host-a", ChangeID: change.ChangeID, Revision: "7", PayloadDigest: change.PayloadDigest}
	if got := change.Receipt(); got != want {
		t.Fatalf("Receipt() = %+v, want %+v", got, want)
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"origin":"host-a","change_id":"` + change.ChangeID + `","revision":"7","payload_digest":"` + change.PayloadDigest + `"}`
	if string(encoded) != wantJSON {
		t.Fatalf("receipt json = %s, want %s", encoded, wantJSON)
	}
}
