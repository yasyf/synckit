package delivery

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

func TestReasons(t *testing.T) {
	open := netpolicy.State{Status: netpolicy.StatusConnected}
	tests := []struct {
		name         string
		blocked      netpolicy.State
		wantRefusal  PauseReason
		wantVerdict  PauseReason
		senderRefuse PauseReason
	}{
		{"disconnected", netpolicy.State{Status: netpolicy.StatusDisconnected}, PausePeerDisconnected, PauseLocalDisconnected, PauseLocalDisconnected},
		{"unknown", netpolicy.State{}, PausePeerUnknown, PauseLocalUnknown, PauseLocalUnknown},
		{"cellular", netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true}, PausePeerCellular, PauseLocalCellular, PauseLocalCellular},
		{"expensive", netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true}, PausePeerExpensive, PauseLocalExpensive, PauseLocalExpensive},
		{"constrained", netpolicy.State{Status: netpolicy.StatusConnected, Constrained: true}, PausePeerConstrained, PauseLocalConstrained, PauseLocalConstrained},
		{"manual metered", netpolicy.State{Status: netpolicy.StatusConnected, ManualMetered: true}, PausePeerManualMetered, PauseLocalManualMetered, PauseLocalManualMetered},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiverBlocked := artifact.PausedFor(netpolicy.Evaluate(tt.blocked, open))
			if got := ReasonForRefusal(receiverBlocked); got != tt.wantRefusal {
				t.Errorf("receiver-side refusal = %s, want %s", got, tt.wantRefusal)
			}
			senderBlocked := artifact.PausedFor(netpolicy.Evaluate(open, tt.blocked))
			if got := ReasonForRefusal(senderBlocked); got != tt.senderRefuse {
				t.Errorf("sender-side refusal = %s, want %s", got, tt.senderRefuse)
			}
			if got := ReasonForVerdict(netpolicy.Evaluate(tt.blocked, open)); got != tt.wantVerdict {
				t.Errorf("own local verdict = %s, want %s", got, tt.wantVerdict)
			}
			if got := ReasonForVerdict(netpolicy.Evaluate(open, tt.blocked)); got != tt.wantRefusal {
				t.Errorf("own peer verdict = %s, want %s", got, tt.wantRefusal)
			}
		})
	}
	midTransfer := &artifact.PausedError{Code: artifact.PauseReceiverRestrictedMidTransfer, Reason: "local: restricted mid-transfer"}
	if got := ReasonForRefusal(midTransfer); got != PausePeerRestrictedMidTransfer {
		t.Errorf("receiver mid-transfer refusal = %s, want %s", got, PausePeerRestrictedMidTransfer)
	}
}

func TestPeerStatusJSON(t *testing.T) {
	at := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	local := netpolicy.State{Status: netpolicy.StatusConnected, ObservedAt: at}
	status := PeerStatus{
		ServiceID: "cc-sync", Peer: "host-b", Generation: 3,
		Acked: syncservice.NewRevision(4), AckedChangeID: "c4", AckedAt: at,
		Pending: &Pending{
			ChangeID: "c5", Kind: syncservice.ChangeSnapshot, BaseRevision: "0", SourceRevision: "5",
			Roots: 2, StagedAt: at, Superseded: 1,
		},
		State: StatePaused, PauseReason: PausePeerCellular, PauseSince: at,
		LastAttemptAt: at, NextAttemptAt: at,
		Progress: Progress{
			RootsTotal: 2, RootsComplete: 1, ObjectsMissing: 3, ObjectsSent: 4,
			BytesMissing: 5, BytesSent: 6, WireBytesSent: 7, InFlightLimit: artifact.PartSize, EnumerationDone: true,
		},
		LocalNetwork: &local,
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	stamp := `"2026-09-26T12:00:00Z"`
	want := `{"service_id":"cc-sync","peer":"host-b","generation":3,"acked":"4","acked_change_id":"c4","acked_at":` + stamp + `,` +
		`"pending":{"change_id":"c5","kind":"snapshot","base_revision":"0","source_revision":"5","roots":2,"staged_at":` + stamp + `,"superseded":1},` +
		`"state":"paused","pause_reason":"peer-cellular","pause_since":` + stamp + `,"last_attempt_at":` + stamp + `,"next_attempt_at":` + stamp + `,` +
		`"progress":{"roots_total":2,"roots_complete":1,"objects_missing":3,"objects_sent":4,"bytes_missing":5,"bytes_sent":6,"wire_bytes_sent":7,"in_flight_limit":1048576,"enumeration_done":true},` +
		`"local_network":{"status":"connected","expensive":false,"constrained":false,"cellular":false,"manual_metered":false,"observed_at":` + stamp + `,"restricted_epoch":0}}`
	if string(encoded) != want {
		t.Fatalf("json = %s\nwant %s", encoded, want)
	}
	params, err := json.Marshal([]any{StatusParams{ServiceID: "cc-sync"}, KickParams{ServiceID: "cc-sync", Peer: "host-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `[{"service_id":"cc-sync"},{"service_id":"cc-sync","peer":"host-b"}]`; string(params) != want {
		t.Fatalf("params json = %s, want %s", params, want)
	}
}
