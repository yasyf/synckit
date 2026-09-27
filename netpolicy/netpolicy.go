// Package netpolicy gates bulk artifact transfer between synckit peers on
// network cost: both endpoints must report a known, connected, unrestricted
// network. Local capture and tiny policy control messages are never gated.
package netpolicy

import "time"

// Status is the reachability an endpoint's OS reports for its default path.
type Status string

const (
	// StatusUnknown means no path has been observed yet, or the platform cannot
	// observe one. Any unrecognized Status evaluates as unknown.
	StatusUnknown Status = "unknown"
	// StatusDisconnected means the OS reports no usable route, including a path
	// that only becomes usable once a connection attempt brings a link up.
	StatusDisconnected Status = "disconnected"
	// StatusConnected means the OS reports a usable route.
	StatusConnected Status = "connected"
)

// State is one endpoint's network cost snapshot. It crosses the wire as a
// tiny policy control message, so a peer's zero or absent State reads as
// unknown and blocks bulk transfer.
type State struct {
	Status        Status    `json:"status"`
	Expensive     bool      `json:"expensive"`
	Constrained   bool      `json:"constrained"`
	Cellular      bool      `json:"cellular"`
	ManualMetered bool      `json:"manual_metered"`
	ObservedAt    time.Time `json:"observed_at"`
	// RestrictedEpoch lets a transfer started on an Unrestricted State detect a
	// restricted period that began and ended before its next read: when a
	// later State from the same Monitor has the same RestrictedEpoch as an
	// Unrestricted earlier one, the Monitor observed no restriction in between.
	// The epoch advances on every OS path update after which the State is not
	// Unrestricted, and on every observed change to the manual setting file
	// except one that clears a metered mark the Monitor already observed; an
	// unmetered rewrite counts too, since metered may have been switched on
	// and off between two reads of the file. Each Current call re-checks the
	// file's identity, size, and modification time and the random edit mark
	// SaveManual rewrites beside it, so a SaveManual between two reads is
	// never missed, even when the file is deleted again before the second
	// read. RestrictedEpoch compares only States from one Monitor; it crosses
	// the wire so a receiver can refuse every bulk call of a transfer admitted
	// under an earlier epoch of its own.
	RestrictedEpoch uint64 `json:"restricted_epoch"`
}

// Unrestricted reports whether s alone permits bulk transfer: connected and
// free of every cost flag.
func (s State) Unrestricted() bool {
	return s.blocker() == ""
}

func (s State) blocker() string {
	switch {
	case s.Status == StatusDisconnected:
		return "disconnected"
	case s.Status != StatusConnected:
		return "unknown"
	case s.Cellular:
		return "cellular"
	case s.Expensive:
		return "expensive"
	case s.Constrained:
		return "constrained"
	case s.ManualMetered:
		return "manual metered"
	}
	return ""
}

// Verdict is the policy's answer for one local/remote pair. Reason names the
// first blocking endpoint and cause, such as "local: cellular" or
// "remote: unknown", and is empty when Allowed.
type Verdict struct {
	Allowed bool
	Reason  string
}

// Evaluate allows bulk transfer only when both local and remote are
// Unrestricted, checking local first.
func Evaluate(local, remote State) Verdict {
	if cause := local.blocker(); cause != "" {
		return Verdict{Reason: "local: " + cause}
	}
	if cause := remote.blocker(); cause != "" {
		return Verdict{Reason: "remote: " + cause}
	}
	return Verdict{Allowed: true}
}
