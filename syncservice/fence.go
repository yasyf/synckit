package syncservice

import "fmt"

// Receipt is the change a receiver last applied completely from one origin.
// A consumer persists it atomically with a complete apply, never a partial
// one.
type Receipt struct {
	Origin        string   `json:"origin"`
	ChangeID      string   `json:"change_id"`
	Revision      Revision `json:"revision"`
	PayloadDigest string   `json:"payload_digest"`
}

// Receipt returns the receipt a complete apply of e records.
func (e ChangeEnvelope) Receipt() Receipt {
	return Receipt{Origin: e.Origin, ChangeID: e.ChangeID, Revision: e.SourceRevision, PayloadDigest: e.PayloadDigest}
}

// FenceDecision is the receiver's verdict on one incoming change against its
// held receipt.
type FenceDecision int

const (
	// FenceApply means the change advances the held receipt and must be
	// applied.
	FenceApply FenceDecision = iota
	// FenceReplay means the change is exactly the held receipt; acknowledge
	// it again without applying.
	FenceReplay
	// FenceStale means the held receipt is already at or past the change's
	// source revision.
	FenceStale
	// FenceNeedSnapshot means the change is a delta whose base is not the held
	// revision.
	FenceNeedSnapshot
)

// Fence decides how a receiver holding held, the receipt from change.Origin
// or nil when it holds none, handles change. The ApplyResult is the reply for
// every decision but FenceApply: Replay acknowledges the held revision, Stale
// reports it with its digest, and NeedSnapshot requests a full snapshot. A
// missing receipt counts as revision zero.
func Fence(held *Receipt, change ChangeEnvelope) (FenceDecision, ApplyResult, error) {
	if err := change.Validate(true); err != nil {
		return 0, ApplyResult{}, err
	}
	source, err := change.SourceRevision.Uint64()
	if err != nil {
		return 0, ApplyResult{}, err
	}
	base, err := change.BaseRevision.Uint64()
	if err != nil {
		return 0, ApplyResult{}, err
	}
	var heldRevision uint64
	if held != nil {
		if held.Origin != change.Origin {
			return 0, ApplyResult{}, fmt.Errorf("syncservice: receipt origin %q fences a change from %q", held.Origin, change.Origin)
		}
		if heldRevision, err = held.Revision.Uint64(); err != nil {
			return 0, ApplyResult{}, fmt.Errorf("syncservice: held receipt: %w", err)
		}
		if held.ChangeID == change.ChangeID && held.Revision == change.SourceRevision && held.PayloadDigest == change.PayloadDigest {
			return FenceReplay, ApplyResult{AckedRevision: held.Revision}, nil
		}
		if source <= heldRevision {
			return FenceStale, ApplyResult{AckedRevision: held.Revision, Stale: true, HeldDigest: held.PayloadDigest}, nil
		}
	}
	if change.Kind == ChangeDelta && base != heldRevision {
		return FenceNeedSnapshot, ApplyResult{NeedSnapshot: true}, nil
	}
	return FenceApply, ApplyResult{}, nil
}
