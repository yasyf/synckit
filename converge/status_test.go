package converge

import (
	"testing"
	"time"
)

func TestPeerStatusFirstDownOnly(t *testing.T) {
	s := NewPeerStatus()
	if !s.Down("peer") {
		t.Fatal("first Down = false, want true (a new outage begins)")
	}
	if s.Down("peer") {
		t.Fatal("second Down = true, want false (outage already open)")
	}
}

func TestPeerStatusRecoveryReportsDownFor(t *testing.T) {
	s := NewPeerStatus()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }

	if !s.Down("peer") {
		t.Fatal("Down = false, want true")
	}
	now = now.Add(90 * time.Second)

	downFor, recovered := s.Up("peer")
	if !recovered {
		t.Fatal("Up recovered = false, want true (outage ends)")
	}
	if downFor != 90*time.Second {
		t.Fatalf("down_for = %s, want 90s", downFor)
	}

	if d, recovered := s.Up("peer"); recovered || d != 0 {
		t.Fatalf("second Up = (%s, %v), want (0, false) — no outage to end", d, recovered)
	}
}

func TestPeerStatusIndependentPeers(t *testing.T) {
	s := NewPeerStatus()

	if !s.Down("a") {
		t.Fatal("Down(a) = false, want true")
	}
	// b never went down, so recovering it reports no transition.
	if _, recovered := s.Up("b"); recovered {
		t.Fatal("Up(b) recovered = true, want false (b never went down)")
	}
	if !s.Down("b") {
		t.Fatal("Down(b) = false, want true (independent of a)")
	}

	// a is still down; recovering a is a real transition, and it does not touch b.
	if _, recovered := s.Up("a"); !recovered {
		t.Fatal("Up(a) recovered = false, want true (a was down)")
	}
	if _, recovered := s.Up("a"); recovered {
		t.Fatal("second Up(a) recovered = true, want false (already recovered)")
	}
	if _, recovered := s.Up("b"); !recovered {
		t.Fatal("Up(b) recovered = false, want true (b still down)")
	}
}
