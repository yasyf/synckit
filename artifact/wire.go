package artifact

import (
	"fmt"
	"strings"

	"github.com/yasyf/synckit/netpolicy"
)

// The artifact rpc method names a consumer's dispatcher serves.
const (
	// MethodNetStatus reports the consumer host's live network State. It is a
	// control call and always allowed.
	MethodNetStatus = "synckit.net.status.v1"
	// MethodClosure pages through the closure of a root list.
	MethodClosure = "synckit.artifact.closure.v1"
	// MethodHave reports which digests the store lacks.
	MethodHave = "synckit.artifact.have.v1"
	// MethodBatchBuild builds an outbox batch from stored objects.
	MethodBatchBuild = "synckit.artifact.batch.build.v1"
	// MethodBatchRead reads one outbox batch part.
	MethodBatchRead = "synckit.artifact.batch.read.v1"
	// MethodBatchDrop removes one outbox batch.
	MethodBatchDrop = "synckit.artifact.batch.drop.v1"
	// MethodBatchBegin stages one incoming batch and reports the parts it
	// already holds, or refuses with a PausedError.
	MethodBatchBegin = "synckit.artifact.batch.begin.v1"
	// MethodBatchPut writes one incoming batch part, or refuses with a
	// PausedError before writing.
	MethodBatchPut = "synckit.artifact.batch.put.v1"
	// MethodBatchCommit verifies and stores every object of a fully staged
	// incoming batch.
	MethodBatchCommit = "synckit.artifact.batch.commit.v1"
	// MethodPinsSet replaces one owner's pinned roots.
	MethodPinsSet = "synckit.artifact.pins.set.v1"
)

// Methods lists every artifact rpc method in the order capabilities report
// them.
var Methods = []string{
	MethodNetStatus,
	MethodClosure,
	MethodHave,
	MethodBatchBuild,
	MethodBatchRead,
	MethodBatchDrop,
	MethodBatchBegin,
	MethodBatchPut,
	MethodBatchCommit,
	MethodPinsSet,
}

// PauseCode is the stable wire code of a receiver's policy refusal: which
// endpoint blocked, named from the receiver's side, and why.
type PauseCode string

const (
	// PauseReceiverDisconnected means the receiver has no usable route.
	PauseReceiverDisconnected PauseCode = "receiver-disconnected"
	// PauseReceiverUnknown means the receiver has not observed its network.
	PauseReceiverUnknown PauseCode = "receiver-unknown"
	// PauseReceiverCellular means the receiver is on cellular.
	PauseReceiverCellular PauseCode = "receiver-cellular"
	// PauseReceiverExpensive means the receiver's network is expensive.
	PauseReceiverExpensive PauseCode = "receiver-expensive"
	// PauseReceiverConstrained means the receiver's network is constrained.
	PauseReceiverConstrained PauseCode = "receiver-constrained"
	// PauseReceiverManualMetered means the receiver's user marked it metered.
	PauseReceiverManualMetered PauseCode = "receiver-manual-metered"
	// PauseSenderDisconnected means the sender declared no usable route.
	PauseSenderDisconnected PauseCode = "sender-disconnected"
	// PauseSenderUnknown means the sender declared an unknown or absent state.
	PauseSenderUnknown PauseCode = "sender-unknown"
	// PauseSenderCellular means the sender declared cellular.
	PauseSenderCellular PauseCode = "sender-cellular"
	// PauseSenderExpensive means the sender declared an expensive network.
	PauseSenderExpensive PauseCode = "sender-expensive"
	// PauseSenderConstrained means the sender declared a constrained network.
	PauseSenderConstrained PauseCode = "sender-constrained"
	// PauseSenderManualMetered means the sender declared a manual metered mark.
	PauseSenderManualMetered PauseCode = "sender-manual-metered"
)

var pauseCodes = map[string]PauseCode{
	"local: disconnected":    PauseReceiverDisconnected,
	"local: unknown":         PauseReceiverUnknown,
	"local: cellular":        PauseReceiverCellular,
	"local: expensive":       PauseReceiverExpensive,
	"local: constrained":     PauseReceiverConstrained,
	"local: manual metered":  PauseReceiverManualMetered,
	"remote: disconnected":   PauseSenderDisconnected,
	"remote: unknown":        PauseSenderUnknown,
	"remote: cellular":       PauseSenderCellular,
	"remote: expensive":      PauseSenderExpensive,
	"remote: constrained":    PauseSenderConstrained,
	"remote: manual metered": PauseSenderManualMetered,
}

// Validate requires one of the defined PauseCode values.
func (c PauseCode) Validate() error {
	for _, code := range pauseCodes {
		if code == c {
			return nil
		}
	}
	return fmt.Errorf("%w: pause code %q", ErrInvalid, string(c))
}

// PausedError is the typed refusal of batch.begin and batch.put: the
// receiver evaluated its own live State against the sender's declared State
// and wrote nothing. It crosses the wire in the result's Paused field, and
// the syncservice client returns it as the call's error.
type PausedError struct {
	Code   PauseCode `json:"code"`
	Reason string    `json:"reason"`
}

func (e *PausedError) Error() string {
	return fmt.Sprintf("artifact: receiver paused bulk transfer (%s): %s", e.Code, e.Reason)
}

// PausedFor returns the refusal for verdict, which must be the blocked
// result of netpolicy.Evaluate(receiverLive, sender). It panics on an
// allowed verdict or a reason netpolicy does not produce.
func PausedFor(verdict netpolicy.Verdict) *PausedError {
	code, ok := pauseCodes[verdict.Reason]
	if verdict.Allowed || !ok {
		panic(fmt.Sprintf("artifact: no pause code for verdict %+v", verdict))
	}
	return &PausedError{Code: code, Reason: verdict.Reason}
}

// NetStatusResult is the result of MethodNetStatus.
type NetStatusResult struct {
	State netpolicy.State `json:"state"`
}

// ClosureParams asks for the closure page of Roots starting at object index
// After, at most Limit objects.
type ClosureParams struct {
	Roots []Ref `json:"roots"`
	After int   `json:"after"`
	Limit int   `json:"limit"`
}

// Validate checks the roots and the page window.
func (p ClosureParams) Validate() error {
	if len(p.Roots) == 0 {
		return fmt.Errorf("%w: closure needs at least one root", ErrInvalid)
	}
	if err := ValidateRoots(p.Roots); err != nil {
		return err
	}
	if p.After < 0 || p.Limit <= 0 || p.Limit > MaxClosurePage {
		return fmt.Errorf("%w: closure page after %d limit %d, want after >= 0 and limit 1..%d", ErrInvalid, p.After, p.Limit, MaxClosurePage)
	}
	return nil
}

// ClosurePage is one offset page of a closure. Next is the After of the
// following page; Done marks the last page. Offsets are stable because a
// closure is a pure function of immutable manifests.
type ClosurePage struct {
	Objects      []ObjectEntry `json:"objects"`
	Next         int           `json:"next"`
	Done         bool          `json:"done"`
	TotalObjects int           `json:"total_objects"`
	TotalBytes   int64         `json:"total_bytes"`
}

// HaveParams asks which of Digests the store lacks.
type HaveParams struct {
	Digests []Digest `json:"digests"`
}

// Validate checks the query bound and every digest.
func (p HaveParams) Validate() error {
	if len(p.Digests) > MaxHaveDigests {
		return fmt.Errorf("%w: %d digests exceed %d", ErrInvalid, len(p.Digests), MaxHaveDigests)
	}
	for _, digest := range p.Digests {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// HaveResult lists the queried digests the store lacks, in query order.
type HaveResult struct {
	Missing []Digest `json:"missing"`
}

// BatchBuildParams asks the source store to build a batch of Objects in the
// given order, each packed under the role its closure reference gives it.
type BatchBuildParams struct {
	Objects []ObjectEntry `json:"objects"`
}

// Validate checks the object count and unique valid entries.
func (p BatchBuildParams) Validate() error {
	if len(p.Objects) == 0 || len(p.Objects) > MaxBatchObjects {
		return fmt.Errorf("%w: batch build of %d objects, want 1..%d", ErrInvalid, len(p.Objects), MaxBatchObjects)
	}
	seen := make(map[Digest]struct{}, len(p.Objects))
	for _, object := range p.Objects {
		if err := object.Validate(); err != nil {
			return err
		}
		if _, dup := seen[object.Digest]; dup {
			return fmt.Errorf("%w: duplicate batch object %s", ErrInvalid, object.Digest)
		}
		seen[object.Digest] = struct{}{}
	}
	return nil
}

// BatchRef names one batch; it is the params of batch.drop and batch.commit.
type BatchRef struct {
	ID Digest `json:"id"`
}

// Validate checks the batch ID.
func (p BatchRef) Validate() error {
	return p.ID.Validate()
}

// BatchReadParams names one outbox part to read.
type BatchReadParams struct {
	ID    Digest `json:"id"`
	Index int    `json:"index"`
}

// Validate checks the batch ID and part index.
func (p BatchReadParams) Validate() error {
	if err := p.ID.Validate(); err != nil {
		return err
	}
	if p.Index < 0 || p.Index >= maxBatchParts {
		return fmt.Errorf("%w: part index %d is outside 0..%d", ErrInvalid, p.Index, maxBatchParts-1)
	}
	return nil
}

// BatchReadResult carries one outbox part's compressed bytes.
type BatchReadResult struct {
	Data []byte `json:"data"`
}

// BatchBeginParams stages Batch on the receiver. Sender is the sender's live
// State at the call; the receiver refuses unless both it and its own live
// State are unrestricted.
type BatchBeginParams struct {
	Batch  BatchDescriptor `json:"batch"`
	Sender netpolicy.State `json:"sender"`
}

// Validate checks the descriptor.
func (p BatchBeginParams) Validate() error {
	return p.Batch.Validate()
}

// BatchBeginResult reports the parts the receiver already holds, ascending,
// and the receiver's live State. Paused is set, and nothing was staged, when
// the policy refused.
type BatchBeginResult struct {
	HaveParts []int           `json:"have_parts"`
	Peer      netpolicy.State `json:"peer"`
	Paused    *PausedError    `json:"paused,omitempty"`
}

// BatchPutParams writes part Index of batch ID. Sender is the sender's live
// State at the call; the receiver re-evaluates before every write.
type BatchPutParams struct {
	ID     Digest          `json:"id"`
	Index  int             `json:"index"`
	Data   []byte          `json:"data"`
	Sender netpolicy.State `json:"sender"`
}

// Validate checks the batch ID, part index, and part length.
func (p BatchPutParams) Validate() error {
	if err := (BatchReadParams{ID: p.ID, Index: p.Index}).Validate(); err != nil {
		return err
	}
	if len(p.Data) == 0 || len(p.Data) > PartSize {
		return fmt.Errorf("%w: part of %d bytes, want 1..%d", ErrInvalid, len(p.Data), PartSize)
	}
	return nil
}

// BatchPutResult reports the receiver's live State after the put. Paused is
// set, and nothing was written, when the policy refused.
type BatchPutResult struct {
	Peer   netpolicy.State `json:"peer"`
	Paused *PausedError    `json:"paused,omitempty"`
}

// PinsSetParams replaces Owner's pinned roots; empty Roots removes the pin
// set.
type PinsSetParams struct {
	Owner string `json:"owner"`
	Roots []Ref  `json:"roots"`
}

// Validate checks the owner name and the roots.
func (p PinsSetParams) Validate() error {
	if p.Owner == "" || strings.ContainsAny(p.Owner, "\x00\r\n") {
		return fmt.Errorf("%w: pin owner %q", ErrInvalid, p.Owner)
	}
	if len(p.Roots) > MaxPinRoots {
		return fmt.Errorf("%w: %d pinned roots exceed %d", ErrInvalid, len(p.Roots), MaxPinRoots)
	}
	return validateRefs(p.Roots, "pinned root")
}
