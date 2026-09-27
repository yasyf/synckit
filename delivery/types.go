// Package delivery reports synckitd's per-peer delivery of consumer changes:
// the durable acknowledgement and pending change for every (service, peer)
// pair, overlaid with the deliverer's live state, pause reason, and transfer
// progress.
//
// A consumer revision is durable on peer P only once it is at or below
// PeerStatus.Acked for P; Progress and Pending are informational. After the
// deliverer observes a pause, up to one in-flight part (artifact.PartSize
// compressed bytes) may still cross, and a cellular backhaul the OS does not
// mark as cellular or expensive reads as unrestricted unless the host is
// manually marked metered.
package delivery

import (
	"fmt"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

// The synckitd rpc method names serving delivery status and kicks.
const (
	// MethodStatus reports every PeerStatus of one service, or of all
	// services for an empty service ID.
	MethodStatus = "delivery.status"
	// MethodKick schedules one delivery run for a service to a peer, or to
	// every mesh host for an empty peer.
	MethodKick = "delivery.kick"
)

// State is the deliverer's live phase for one (service, peer) pair.
type State string

const (
	// StateIdle means the peer has acknowledged the latest local revision.
	StateIdle State = "idle"
	// StateStaged means a pending change awaits transfer.
	StateStaged State = "staged"
	// StateTransferring means artifact batches are moving to the peer.
	StateTransferring State = "transferring"
	// StateApplying means the peer is applying the pending change.
	StateApplying State = "applying"
	// StatePaused means the network policy holds the transfer; see
	// PauseReason.
	StatePaused State = "paused"
	// StateBackoff means the last run failed and the next waits out a
	// backoff; see LastError.
	StateBackoff State = "backoff"
)

// PauseReason names which endpoint's network policy paused a delivery and
// why, from the delivering host's side.
type PauseReason string

const (
	// PauseLocalDisconnected means this host has no usable route.
	PauseLocalDisconnected PauseReason = "local-disconnected"
	// PauseLocalUnknown means this host has not observed its network.
	PauseLocalUnknown PauseReason = "local-unknown"
	// PauseLocalCellular means this host is on cellular.
	PauseLocalCellular PauseReason = "local-cellular"
	// PauseLocalExpensive means this host's network is expensive.
	PauseLocalExpensive PauseReason = "local-expensive"
	// PauseLocalConstrained means this host's network is constrained.
	PauseLocalConstrained PauseReason = "local-constrained"
	// PauseLocalManualMetered means this host is manually marked metered.
	PauseLocalManualMetered PauseReason = "local-manual-metered"
	// PauseLocalRestrictedMidTransfer means this host's network was restricted
	// at some point after the transfer was admitted and is unrestricted again;
	// the deliverer stops the transfer and restarts it at once.
	PauseLocalRestrictedMidTransfer PauseReason = "local-restricted-mid-transfer"
	// PausePeerDisconnected means the peer has no usable route.
	PausePeerDisconnected PauseReason = "peer-disconnected"
	// PausePeerUnknown means the peer has not observed its network.
	PausePeerUnknown PauseReason = "peer-unknown"
	// PausePeerCellular means the peer is on cellular.
	PausePeerCellular PauseReason = "peer-cellular"
	// PausePeerExpensive means the peer's network is expensive.
	PausePeerExpensive PauseReason = "peer-expensive"
	// PausePeerConstrained means the peer's network is constrained.
	PausePeerConstrained PauseReason = "peer-constrained"
	// PausePeerManualMetered means the peer is manually marked metered.
	PausePeerManualMetered PauseReason = "peer-manual-metered"
	// PausePeerUnreachable means the peer's network status call failed.
	PausePeerUnreachable PauseReason = "peer-unreachable"
	// PausePeerIncompatible means the peer lacks the v2 or artifact methods
	// an artifact change needs.
	PausePeerIncompatible PauseReason = "peer-incompatible"
)

var refusalReasons = map[artifact.PauseCode]PauseReason{
	artifact.PauseReceiverDisconnected:  PausePeerDisconnected,
	artifact.PauseReceiverUnknown:       PausePeerUnknown,
	artifact.PauseReceiverCellular:      PausePeerCellular,
	artifact.PauseReceiverExpensive:     PausePeerExpensive,
	artifact.PauseReceiverConstrained:   PausePeerConstrained,
	artifact.PauseReceiverManualMetered: PausePeerManualMetered,
	artifact.PauseSenderDisconnected:    PauseLocalDisconnected,
	artifact.PauseSenderUnknown:         PauseLocalUnknown,
	artifact.PauseSenderCellular:        PauseLocalCellular,
	artifact.PauseSenderExpensive:       PauseLocalExpensive,
	artifact.PauseSenderConstrained:     PauseLocalConstrained,
	artifact.PauseSenderManualMetered:   PauseLocalManualMetered,
}

var verdictReasons = map[artifact.PauseCode]PauseReason{
	artifact.PauseReceiverDisconnected:  PauseLocalDisconnected,
	artifact.PauseReceiverUnknown:       PauseLocalUnknown,
	artifact.PauseReceiverCellular:      PauseLocalCellular,
	artifact.PauseReceiverExpensive:     PauseLocalExpensive,
	artifact.PauseReceiverConstrained:   PauseLocalConstrained,
	artifact.PauseReceiverManualMetered: PauseLocalManualMetered,
	artifact.PauseSenderDisconnected:    PausePeerDisconnected,
	artifact.PauseSenderUnknown:         PausePeerUnknown,
	artifact.PauseSenderCellular:        PausePeerCellular,
	artifact.PauseSenderExpensive:       PausePeerExpensive,
	artifact.PauseSenderConstrained:     PausePeerConstrained,
	artifact.PauseSenderManualMetered:   PausePeerManualMetered,
}

// ReasonForRefusal maps a peer's batch.begin or batch.put refusal to the
// delivering host's PauseReason: a receiver-side block is the peer's, a
// sender-side block is this host's. The refusal's code must be valid.
func ReasonForRefusal(refusal *artifact.PausedError) PauseReason {
	reason, ok := refusalReasons[refusal.Code]
	if !ok {
		panic(fmt.Sprintf("delivery: no pause reason for refusal code %q", refusal.Code))
	}
	return reason
}

// ReasonForVerdict maps the delivering host's own blocked gate verdict,
// netpolicy.Evaluate(local, peer), to its PauseReason.
func ReasonForVerdict(verdict netpolicy.Verdict) PauseReason {
	return verdictReasons[artifact.PausedFor(verdict).Code]
}

// Pending is the staged change awaiting the peer's acknowledgement.
// Superseded counts the pending changes it replaced while unacknowledged.
type Pending struct {
	ChangeID       string                 `json:"change_id"`
	Kind           syncservice.ChangeKind `json:"kind"`
	BaseRevision   syncservice.Revision   `json:"base_revision"`
	SourceRevision syncservice.Revision   `json:"source_revision"`
	Roots          int                    `json:"roots"`
	StagedAt       time.Time              `json:"staged_at"`
	Superseded     uint64                 `json:"superseded"`
}

// Progress is the live transfer tally of the pending change. Byte counts
// other than WireBytesSent are uncompressed; WireBytesSent counts compressed
// part bytes, abandoned uploads included. InFlightLimit is the most bytes
// that may still cross after a pause is observed.
type Progress struct {
	RootsTotal      int   `json:"roots_total"`
	RootsComplete   int   `json:"roots_complete"`
	ObjectsMissing  int64 `json:"objects_missing"`
	ObjectsSent     int64 `json:"objects_sent"`
	BytesMissing    int64 `json:"bytes_missing"`
	BytesSent       int64 `json:"bytes_sent"`
	WireBytesSent   int64 `json:"wire_bytes_sent"`
	InFlightLimit   int64 `json:"in_flight_limit"`
	EnumerationDone bool  `json:"enumeration_done"`
}

// PeerStatus is one (service, peer) delivery: the durable acknowledgement
// and pending change from delivery state, overlaid with the deliverer's live
// State, pause, error, schedule, progress, and both endpoints' last observed
// network State.
type PeerStatus struct {
	ServiceID     string               `json:"service_id"`
	Peer          string               `json:"peer"`
	Generation    uint64               `json:"generation"`
	Acked         syncservice.Revision `json:"acked"`
	AckedChangeID string               `json:"acked_change_id"`
	AckedAt       time.Time            `json:"acked_at"`
	Pending       *Pending             `json:"pending,omitempty"`
	State         State                `json:"state"`
	PauseReason   PauseReason          `json:"pause_reason,omitempty"`
	PauseSince    time.Time            `json:"pause_since"`
	LastError     string               `json:"last_error,omitempty"`
	LastAttemptAt time.Time            `json:"last_attempt_at"`
	NextAttemptAt time.Time            `json:"next_attempt_at"`
	Progress      Progress             `json:"progress"`
	LocalNetwork  *netpolicy.State     `json:"local_network,omitempty"`
	PeerNetwork   *netpolicy.State     `json:"peer_network,omitempty"`
}

// StatusParams are the params of MethodStatus; an empty ServiceID means
// every service.
type StatusParams struct {
	ServiceID string `json:"service_id"`
}

// KickParams are the params of MethodKick; an empty Peer means every mesh
// host.
type KickParams struct {
	ServiceID string `json:"service_id"`
	Peer      string `json:"peer"`
}
