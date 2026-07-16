// Package consent is the generic consent protocol synckit consumers share:
// verdict classification, requestor+subject TTL grants, routed-approval nonce
// minting, the fail-closed peer walk that routes a consent prompt to a live
// approver, and the engine + RPC service synckitd answers it with. The engine
// carries the attestation extension (argv, nonce, signature) as opaque
// transport — verification belongs to the caller's root verifier, never here.
package consent

import "errors"

// Verdict classifies a consent outcome for a renderer or wire reply.
type Verdict int

const (
	// VerdictOK is an approval: the human consented, or a live grant covered
	// the request.
	VerdictOK Verdict = iota
	// VerdictUnavailable means no gate could fire right now — no live session,
	// local or routed — and the caller should retry elsewhere or later.
	VerdictUnavailable
	// VerdictDenied means a human declined the consent prompt.
	VerdictDenied
	// VerdictFatal is everything else: a real failure the caller surfaces.
	VerdictFatal
)

// String renders the verdict's wire word.
func (v Verdict) String() string {
	switch v {
	case VerdictOK:
		return "approved"
	case VerdictUnavailable:
		return "unavailable"
	case VerdictDenied:
		return "denied"
	case VerdictFatal:
		return "fatal"
	}
	return "invalid"
}

// AuthRequired reports that no live session — local or routed — could gate the
// request. It is the error the consent path fails closed with, and a routed
// approval that fails to bind (a nonce or endpoint mismatch) raises it too: an
// unbound approval is a security failure, never a retry. Callers branch on it
// via errors.As.
type AuthRequired struct {
	Msg string
}

func (e *AuthRequired) Error() string { return e.Msg }

// Denied reports that a human on Peer declined a routed consent prompt. A
// denial is terminal: no other approver is ever asked.
type Denied struct {
	Peer string
}

func (e *Denied) Error() string { return "consent denied from " + e.Peer }

// Classify maps a consent error to the verdict a caller branches on: nil is
// OK, a fail-closed AuthRequired is Unavailable, a human denial is Denied, and
// anything else is Fatal. Domain wrappers layer their own error types in front
// and delegate the rest here.
func Classify(err error) Verdict {
	if err == nil {
		return VerdictOK
	}
	var authErr *AuthRequired
	if errors.As(err, &authErr) {
		return VerdictUnavailable
	}
	var denied *Denied
	if errors.As(err, &denied) {
		return VerdictDenied
	}
	return VerdictFatal
}
