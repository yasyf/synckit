package consent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/synckit/presence"
)

// The remote hops a routed consent shells on a peer, answered by the synckitd
// CLI over the peer's own daemon socket.
const (
	RelayCommand    = "synckitd consent relay"
	PresenceCommand = "synckitd consent presence"
)

// Probe reads this host's console GUI session. Injected so the engine runs in
// tests against synthetic snapshots without touching macOS.
type Probe func(ctx context.Context) (presence.SessionSnapshot, error)

// ResolveFunc resolves the ordered approver candidates for a routed consent —
// every mesh peer except this host.
type ResolveFunc func(ctx context.Context) ([]string, error)

// Request is one consent ask. Requestor derives server-side from the calling
// connection's credentials — never client-supplied. Argv and Nonce are the
// attestation extension: the engine carries them to the prompting helper and
// returns the signature opaquely; it never verifies — the caller's root
// verifier owns that. Origin names the requesting host on a relayed leg. A
// zero TTL means every call prompts.
type Request struct {
	Requestor string
	Client    string
	Reason    string
	Subject   string
	Origin    string
	Argv      []string
	Nonce     string
	TTL       time.Duration
	LocalOnly bool
}

// Attestation is the opaque signature material an approved
// attestation-carrying prompt returns: the signing key id, the signature, and
// the host whose Secure Enclave key signed.
type Attestation struct {
	KeyID    string `json:"key_id"`
	Sig      string `json:"sig"`
	SignedBy string `json:"signed_by"`
}

// PromptResult is a Prompter's outcome: the human's verdict and, for an
// approved attestation-carrying prompt, the signature material.
type PromptResult struct {
	Verdict     Verdict
	Attestation *Attestation
}

// Prompter gates one request behind the local human. A gate that cannot fire
// right now (a locked screen, no prompt surface) answers VerdictUnavailable so
// the engine routes; an error is fatal.
type Prompter interface {
	Prompt(ctx context.Context, req Request) (PromptResult, error)
}

// Decision is the engine's answer to one consent ask.
type Decision struct {
	Verdict      Verdict
	ApprovedBy   string
	Routed       bool
	Cached       bool
	GrantedUntil time.Time
	Attestation  *Attestation
}

// RelayRequest is the payload the origin feeds a peer's relay leg on stdin:
// the display material, the exact nonce + endpoint the reply must echo, and
// the opaque attestation extension (argv plus the origin's signing nonce).
type RelayRequest struct {
	Client    string   `json:"client"`
	Reason    string   `json:"reason"`
	Subject   string   `json:"subject"`
	Nonce     string   `json:"nonce"`
	Endpoint  string   `json:"endpoint"`
	Origin    string   `json:"origin"`
	Argv      []string `json:"argv,omitempty"`
	SignNonce string   `json:"sign_nonce,omitempty"`
}

// Engine decides consent asks: a live grant is served silently, an attended
// host prompts locally behind an internal mutex (two sheets never stack), and
// a host that cannot prompt routes the gate to a live peer.
type Engine struct {
	Self     string
	Prompter Prompter
	Probe    Probe
	Grants   *Grants
	Router   *Router
	Resolve  ResolveFunc

	promptMu sync.Mutex
}

// NewEngine builds an engine over injected collaborators with a fresh grant
// store.
func NewEngine(self string, prompter Prompter, probe Probe, router *Router, resolve ResolveFunc) *Engine {
	return &Engine{
		Self:     self,
		Prompter: prompter,
		Probe:    probe,
		Grants:   NewGrants(),
		Router:   router,
		Resolve:  resolve,
	}
}

// Decide runs the consent ladder for req: a live grant for the requestor and
// subject is served silently (Cached), an attended host prompts locally, a
// local gate that answers unavailable — or a host that is not attended —
// routes to a live peer unless req.LocalOnly pins the gate here. A denial,
// local or routed, is Denied; a routed walk that binds no approval is
// Unavailable; a fatal (or unrecognized) prompt verdict and protocol failures
// are fatal errors — never a fallback route.
func (e *Engine) Decide(ctx context.Context, req Request) (Decision, error) {
	if until, ok := e.Grants.Granted(req.Requestor, req.Subject); ok {
		return Decision{Verdict: VerdictOK, Cached: true, GrantedUntil: until}, nil
	}
	snap, err := e.Probe(ctx)
	if err != nil {
		return Decision{}, err
	}
	attended, err := presence.Attended(snap)
	if err != nil {
		return Decision{}, err
	}
	if attended {
		res, err := e.prompt(ctx, req)
		if err != nil {
			return Decision{}, err
		}
		switch res.Verdict {
		case VerdictOK:
			if res.Attestation != nil {
				res.Attestation.SignedBy = e.Self
			}
			return e.approve(req, Decision{Verdict: VerdictOK, ApprovedBy: e.Self, Attestation: res.Attestation}), nil
		case VerdictDenied:
			return Decision{Verdict: VerdictDenied}, nil
		case VerdictUnavailable:
			// The local gate cannot fire right now; fall through to the route.
		default:
			// Fatal/unrecognized verdicts are errors — never routable.
			return Decision{}, fmt.Errorf("consent: local prompt returned %s verdict", res.Verdict)
		}
	}
	if req.LocalOnly {
		return Decision{Verdict: VerdictUnavailable}, nil
	}
	return e.route(ctx, req)
}

// Relay is the approver leg of a routed consent: it verifies this host is
// attended, prompts behind the same internal mutex as Decide — stamping the
// origin's provenance server-side, so a requestor cannot compose it away —
// and returns the verdict plus the opaque signature material. It NEVER routes
// onward: the approver is the routed gate's terminus, so a 3+ mesh can never
// loop an approval back out. An approver-side probe failure answers
// Unavailable — retryable by another mesh host — never a fatal error.
func (e *Engine) Relay(ctx context.Context, req Request) (Decision, error) {
	snap, err := e.Probe(ctx)
	if err != nil {
		return Decision{Verdict: VerdictUnavailable}, nil
	}
	attended, err := presence.Attended(snap)
	if err != nil {
		return Decision{}, err
	}
	if !attended {
		return Decision{Verdict: VerdictUnavailable}, nil
	}
	if len(req.Argv) == 0 && req.Origin != "" {
		// The signing path instead hands req.Origin to the helper, which
		// appends the provenance to the argv display it hashes itself.
		req.Reason += " — requested from " + req.Origin
	}
	res, err := e.prompt(ctx, req)
	if err != nil {
		return Decision{}, err
	}
	d := Decision{Verdict: res.Verdict}
	if res.Attestation != nil {
		res.Attestation.SignedBy = e.Self
		d.Attestation = res.Attestation
	}
	return d, nil
}

// prompt serializes the interactive consent sheets: one prompt fires at a
// time, so a local Decide and an inbound Relay never stack two sheets. The
// mutex is held only around the Prompter call, never across routing.
func (e *Engine) prompt(ctx context.Context, req Request) (PromptResult, error) {
	e.promptMu.Lock()
	defer e.promptMu.Unlock()
	return e.Prompter.Prompt(ctx, req)
}

// approve records the grant an approval earns — a zero TTL records nothing,
// so every call prompts — and stamps the decision's grant window.
func (e *Engine) approve(req Request, d Decision) Decision {
	if req.TTL <= 0 {
		return d
	}
	e.Grants.Grant(req.Requestor, []string{req.Subject}, req.TTL)
	d.GrantedUntil = time.Now().Add(req.TTL)
	return d
}

// route sends the gate across the approver candidates, carrying the display
// material and the opaque attestation extension; the Router owns the echo
// binding. A denial or an exhausted walk folds into the wire verdict; any
// other failure propagates fatally.
func (e *Engine) route(ctx context.Context, req Request) (Decision, error) {
	candidates, err := e.Resolve(ctx)
	if err != nil {
		return Decision{}, err
	}
	endpoint := e.Self + ":" + req.Subject
	relay := RelayRequest{
		Client:    req.Client,
		Reason:    req.Reason,
		Subject:   req.Subject,
		Endpoint:  endpoint,
		Origin:    e.Self,
		Argv:      req.Argv,
		SignNonce: req.Nonce,
	}
	reply, err := e.Router.Route(ctx, candidates, endpoint, func(_, nonce string) (string, []byte, error) {
		attempt := relay
		attempt.Nonce = nonce
		stdin, err := json.Marshal(attempt)
		if err != nil {
			return "", nil, err
		}
		return RelayCommand, stdin, nil
	})
	if err != nil {
		switch Classify(err) {
		case VerdictDenied:
			return Decision{Verdict: VerdictDenied, Routed: true}, nil
		case VerdictUnavailable:
			return Decision{Verdict: VerdictUnavailable, Routed: true}, nil
		case VerdictOK, VerdictFatal:
			return Decision{}, err
		}
	}
	d := Decision{Verdict: VerdictOK, ApprovedBy: reply.Peer, Routed: true}
	if reply.SignedBy != "" {
		d.ApprovedBy = reply.SignedBy
	}
	if reply.Sig != "" {
		d.Attestation = &Attestation{KeyID: reply.KeyID, Sig: reply.Sig, SignedBy: reply.SignedBy}
	}
	return e.approve(req, d), nil
}
