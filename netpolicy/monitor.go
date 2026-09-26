package netpolicy

import "sync"

// Monitor observes this host's network cost State.
type Monitor interface {
	// Current returns the latest State, with the manual override merged in, and
	// a channel closed at the next OS path change. Override edits surface on the
	// next Current call rather than through the channel.
	Current() (State, <-chan struct{})
	// Close stops observation and releases every OS resource the Monitor holds.
	Close() error
}

type observed struct {
	manual  *manualSource
	mu      sync.Mutex
	state   State
	changed chan struct{}
}

func newObserved(manualPath string) *observed {
	return &observed{
		manual:  newManualSource(manualPath),
		state:   State{Status: StatusUnknown},
		changed: make(chan struct{}),
	}
}

func (o *observed) Current() (State, <-chan struct{}) {
	metered := o.manual.metered()
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.state
	s.ManualMetered = metered
	return s, o.changed
}

func (o *observed) publish(s State) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.state = s
	close(o.changed)
	o.changed = make(chan struct{})
}
