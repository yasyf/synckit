package consent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/presence"
	"github.com/yasyf/synckit/rpc"
)

// The wire statuses a relay reply carries.
const (
	statusApproved    = "approved"
	statusDenied      = "denied"
	statusUnavailable = "unavailable"
)

// Runner runs a remote command over ssh and returns its stdout — the boundary
// the routed-consent handshake and the peer liveness probe cross.
// hostregistry.ExecSSH satisfies it.
type Runner interface {
	Run(ctx context.Context, target, remoteCmd string, stdin []byte) (string, error)
}

// Attempt composes one relay invocation against peer, binding the fresh
// per-attempt nonce: the remote command to run and the stdin to feed it.
type Attempt func(peer, nonce string) (cmd string, stdin []byte, err error)

// Reply is one peer's answer to a relayed consent request: the verdict status,
// the exact nonce + endpoint echo that binds the approval, and — when the
// relay carried the attestation extension — the opaque signature material.
// Peer names the candidate that answered; it never crosses the wire.
type Reply struct {
	Status   string `json:"status"`
	Nonce    string `json:"nonce"`
	Endpoint string `json:"endpoint"`
	KeyID    string `json:"key_id"`
	Sig      string `json:"sig"`
	SignedBy string `json:"signed_by"`
	Peer     string `json:"-"`
}

// Router walks approver candidates for a routed consent, probe-gating each on
// a liveness read before its relay leg.
type Router struct {
	Runner Runner
	// ProbeCmd is the remote liveness probe; its stdout is a
	// presence.SessionSnapshot JSON.
	ProbeCmd string
	// Nonce mints the per-attempt echo nonce; a field so a test can pin the
	// echo binding. Defaults to NewNonce.
	Nonce func() (string, error)
	// RelayTimeout bounds one relay leg, which may block on a routed human
	// consent — a Touch ID tap on the peer. Derived from the peer handler's
	// own rpc.DispatchTimeout: the human keeps nearly that full window, and
	// the 30s margin makes us give up just before the peer's deadline fires.
	RelayTimeout time.Duration
	// ProbeTimeout bounds one peer's liveness probe: a data-plane read that
	// must fail in seconds, never ride a consent flight's window.
	ProbeTimeout time.Duration
}

// NewRouter builds a Router over runner with the crypto/rand nonce source and
// the default leg timeouts; override the fields after construction to pin them.
func NewRouter(runner Runner, probeCmd string) *Router {
	return &Router{
		Runner:       runner,
		ProbeCmd:     probeCmd,
		Nonce:        NewNonce,
		RelayTimeout: rpc.DispatchTimeout - 30*time.Second,
		ProbeTimeout: 10 * time.Second,
	}
}

// Route walks candidates in order and returns the first bound approval, or
// *AuthRequired once they are exhausted. A candidate that is not live, whose
// probe trips ProbeRoutesAround, whose relay leg timed out or hit ssh's exit
// 255, or that answers unavailable is skipped; any other failure is fatal.
// A denial (*Denied) is terminal; an unbound approval is *BindingMismatch.
func (r *Router) Route(ctx context.Context, candidates []string, endpoint string, attempt Attempt) (*Reply, error) {
	var lastErr error
	for _, peer := range candidates {
		live, err := r.Live(ctx, peer)
		if err != nil && !ProbeRoutesAround(err) {
			return nil, err
		}
		if err != nil || !live {
			continue
		}
		reply, next, err := r.relay(ctx, peer, endpoint, attempt)
		if !next {
			return reply, err
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &AuthRequired{Msg: "no peer has a live session to approve consent"}
}

// relay runs one routed-consent attempt against peer, minting a fresh nonce
// for it. next reports whether Route should advance to another approver — the
// relay leg failed at the transport (routesAround), or the peer answered an
// explicit unavailable; next false carries the terminal outcome: the bound
// reply, a denial, an unbound approval's *BindingMismatch, or a fatal protocol
// failure (an unparseable reply or an unexpected status).
func (r *Router) relay(ctx context.Context, peer, endpoint string, attempt Attempt) (reply *Reply, next bool, err error) {
	nonce, err := r.Nonce()
	if err != nil {
		return nil, false, err
	}
	cmd, stdin, err := attempt(peer, nonce)
	if err != nil {
		return nil, false, err
	}
	cctx, cancel := context.WithTimeout(ctx, r.RelayTimeout)
	defer cancel()
	out, err := r.Runner.Run(cctx, peer, cmd, stdin)
	if err != nil {
		if routesAround(err) {
			return nil, true, &AuthRequired{Msg: fmt.Sprintf("consent unreachable at %s: %v", peer, err)}
		}
		return nil, false, fmt.Errorf("consent relay to %s: %w", peer, err)
	}
	var rep Reply
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		return nil, false, fmt.Errorf("parse consent reply from %s: %w", peer, err)
	}
	switch rep.Status {
	case statusDenied:
		return nil, false, &Denied{Peer: peer}
	case statusUnavailable:
		return nil, true, &AuthRequired{Msg: fmt.Sprintf("consent unavailable from %s", peer)}
	case statusApproved:
	default:
		return nil, false, fmt.Errorf("consent from %s answered unexpected status %q", peer, rep.Status)
	}
	if rep.Nonce != nonce || rep.Endpoint != endpoint {
		return nil, false, &BindingMismatch{Peer: peer}
	}
	rep.Peer = peer
	return &rep, false, nil
}

// Live reports whether peer has a live, unlocked, un-shared console session,
// read over ssh via ProbeCmd under ProbeTimeout. A screen-shared peer is not a
// valid approver — its Touch ID prompt may be tapped by the remote viewer
// rather than the physically-present human — so it is skipped.
func (r *Router) Live(ctx context.Context, peer string) (bool, error) {
	pctx, cancel := context.WithTimeout(ctx, r.ProbeTimeout)
	defer cancel()
	out, err := r.Runner.Run(pctx, peer, r.ProbeCmd, nil)
	if err != nil {
		return false, err
	}
	var snap presence.SessionSnapshot
	if err := json.Unmarshal([]byte(out), &snap); err != nil {
		return false, fmt.Errorf("parse presence from %s: %w", peer, err)
	}
	return snap.OnConsole && !snap.Locked && !snap.ScreenShared, nil
}

// routesAround reports whether a relay-leg failure is a genuine transport
// failure the failover may route around: a timed-out leg, or an
// *hostregistry.SSHError caused by ssh's own exit-255 connection failure.
// Anything else is a protocol failure the caller propagates.
func routesAround(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var sshErr *hostregistry.SSHError
	return errors.As(err, &sshErr) && sshErr.ConnFailure()
}

// ProbeRoutesAround reports whether a liveness-probe failure skips to the next
// candidate: a timeout, or an *hostregistry.SSHError whose ssh ran and failed.
// A probe whose ssh never ran says nothing about the peer and stays fatal.
func ProbeRoutesAround(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var sshErr *hostregistry.SSHError
	return errors.As(err, &sshErr) && sshErr.Exited()
}
