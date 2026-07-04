package converge

import (
	"sync"
	"time"
)

// PeerStatus remembers each peer's up/down state across reconcile passes, so an
// outage logs one line when it starts and one when it ends instead of one per
// pass. A consumer holds one tracker for the life of its process. Safe for
// concurrent use.
type PeerStatus struct {
	mu   sync.Mutex
	down map[string]time.Time // peer -> when the current outage began
	now  func() time.Time     // test seam
}

// NewPeerStatus returns an empty tracker with every peer presumed up.
func NewPeerStatus() *PeerStatus {
	return &PeerStatus{down: make(map[string]time.Time), now: time.Now}
}

// Down records a failed attempt against peer, reporting whether it begins a new
// outage — true only on the first failure of an outage, false while the peer
// stays down.
func (s *PeerStatus) Down(peer string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.down[peer]; ok {
		return false
	}
	s.down[peer] = s.now()
	return true
}

// Up records a successful attempt, reporting whether it ends an outage and for
// how long the peer was down. It returns (0, false) when the peer was already up.
func (s *PeerStatus) Up(peer string) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	started, ok := s.down[peer]
	if !ok {
		return 0, false
	}
	delete(s.down, peer)
	return s.now().Sub(started), true
}
