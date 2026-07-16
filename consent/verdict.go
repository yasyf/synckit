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
// request. It is the error the consent path fails closed with when no valid
// answer was obtained; callers may retry or degrade per their own policy.
// Callers branch on it via errors.As.
type AuthRequired struct {
	Msg string
}

func (e *AuthRequired) Error() string { return e.Msg }

// BindingMismatch reports a routed approval that failed to echo the exact
// nonce and endpoint this host sent: an unbound approval is an attack or
// corruption signal, never a live-approver problem, so Classify maps it to
// VerdictFatal — the whole request stops. It is never VerdictUnavailable and
// never retried. Callers branch on it via errors.As.
type BindingMismatch struct {
	Peer string
}

func (e *BindingMismatch) Error() string {
	return "consent approved from " + e.Peer + " did not echo this request's nonce and endpoint"
}

// Denied reports that a human on Peer declined a routed consent prompt. A
// denial is terminal: no other approver is ever asked.
type Denied struct {
	Peer string
}

func (e *Denied) Error() string { return "consent denied from " + e.Peer }

// Classify maps a consent error to the verdict a caller branches on: nil is
// OK, a fail-closed AuthRequired is Unavailable, a human denial is Denied, and
// anything else — a *BindingMismatch included — is Fatal. Domain wrappers
// layer their own error types in front and delegate the rest here.
func Classify(err error) Verdict {
	if err == nil {
		return VerdictOK
	}
	var mismatch *BindingMismatch
	if errors.As(err, &mismatch) {
		return VerdictFatal
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
