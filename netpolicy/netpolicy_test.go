package netpolicy

import (
	"encoding/json"
	"testing"
	"time"
)

var connected = State{Status: StatusConnected}

func TestEvaluate(t *testing.T) {
	causes := []struct {
		name  string
		state State
		cause string
	}{
		{"zero", State{}, "unknown"},
		{"unknown", State{Status: StatusUnknown}, "unknown"},
		{"unrecognized status", State{Status: "satisfiable"}, "unknown"},
		{"disconnected", State{Status: StatusDisconnected}, "disconnected"},
		{"cellular", State{Status: StatusConnected, Cellular: true}, "cellular"},
		{"expensive", State{Status: StatusConnected, Expensive: true}, "expensive"},
		{"constrained", State{Status: StatusConnected, Constrained: true}, "constrained"},
		{"manual metered", State{Status: StatusConnected, ManualMetered: true}, "manual metered"},
		{"cellular outranks expensive", State{Status: StatusConnected, Cellular: true, Expensive: true}, "cellular"},
		{"disconnected outranks flags", State{Status: StatusDisconnected, Expensive: true}, "disconnected"},
	}
	type testCase struct {
		name          string
		local, remote State
		want          Verdict
	}
	tests := make([]testCase, 0, 2+2*len(causes))
	tests = append(tests,
		testCase{"both unrestricted", connected, connected, Verdict{Allowed: true}},
		testCase{"both blocked reports local", State{Status: StatusConnected, Cellular: true}, State{}, Verdict{Reason: "local: cellular"}},
	)
	for _, c := range causes {
		tests = append(tests,
			testCase{"local " + c.name, c.state, connected, Verdict{Reason: "local: " + c.cause}},
			testCase{"remote " + c.name, connected, c.state, Verdict{Reason: "remote: " + c.cause}},
		)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Evaluate(tt.local, tt.remote); got != tt.want {
				t.Errorf("Evaluate() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestUnrestricted(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  bool
	}{
		{"connected", connected, true},
		{"zero", State{}, false},
		{"constrained", State{Status: StatusConnected, Constrained: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.Unrestricted(); got != tt.want {
				t.Errorf("Unrestricted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateWire(t *testing.T) {
	s := State{
		Status:          StatusConnected,
		Cellular:        true,
		ManualMetered:   true,
		ObservedAt:      time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		RestrictedEpoch: 7,
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"status":"connected","expensive":false,"constrained":false,"cellular":true,"manual_metered":true,"observed_at":"2026-09-26T12:00:00Z","restricted_epoch":7}`
	if string(data) != want {
		t.Fatalf("wire = %s, want %s", data, want)
	}
	var decoded State
	if err := json.Unmarshal(data, &decoded); err != nil || decoded != s {
		t.Fatalf("round trip = %+v, %v; want %+v", decoded, err, s)
	}
	var absent State
	if err := json.Unmarshal([]byte(`{}`), &absent); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := Evaluate(connected, absent); got != (Verdict{Reason: "remote: unknown"}) {
		t.Errorf("Evaluate(absent remote) = %+v, want remote: unknown", got)
	}
}
