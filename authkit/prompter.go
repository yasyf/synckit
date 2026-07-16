package authkit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/consent"
)

// ReasonEnvVar carries the prompt text into the helper's verdict-only consent
// subcommand. Exported so a consumer shelling the helper directly over
// Bridge.Run (cookiesync's vault wrappers) sets it rather than hard-coding the
// name.
const ReasonEnvVar = "AUTHKIT_REASON"

// signRequest is the consent-sign stdin payload: the nonce and argv the helper
// hashes, displays, and signs itself — never a pre-composed reason, so a lying
// transport cannot show one command and sign another — plus the requesting
// host the helper appends as "— requested from <host>".
type signRequest struct {
	Nonce         string   `json:"nonce"`
	Argv          []string `json:"argv"`
	RequestedFrom string   `json:"requested_from,omitempty"`
}

// Gate prompts through the installed signed helper, implementing
// consent.Prompter: a request carrying the attestation extension runs
// consent-sign; a bare request runs the verdict-only consent subcommand with
// the reason in AUTHKIT_REASON. A screen-locked helper (exit 3) reports
// Unavailable so the engine routes the gate to a live peer; a caller-pin or
// usage failure (exit 4) is a fatal error the engine never routes around.
type Gate struct {
	Bridge Bridge
}

// Prompt gates req behind the helper's Touch ID sheet.
func (g Gate) Prompt(ctx context.Context, req consent.Request) (consent.PromptResult, error) {
	if len(req.Argv) > 0 {
		return g.sign(ctx, req)
	}
	res, err := g.Bridge.Run(ctx, nil, []string{ReasonEnvVar + "=" + req.Reason}, "consent")
	if err != nil {
		return consent.PromptResult{}, err
	}
	verdict, err := verdictFor(res)
	return consent.PromptResult{Verdict: verdict}, err
}

// sign runs consent-sign: the helper computes and displays the digest of the
// argv it signs, and emits {key_id, sig} on stdout for an approval.
func (g Gate) sign(ctx context.Context, req consent.Request) (consent.PromptResult, error) {
	stdin, err := json.Marshal(signRequest{Nonce: req.Nonce, Argv: req.Argv, RequestedFrom: req.Origin})
	if err != nil {
		return consent.PromptResult{}, fmt.Errorf("encode consent-sign request: %w", err)
	}
	res, err := g.Bridge.Run(ctx, stdin, nil, "consent-sign")
	if err != nil {
		return consent.PromptResult{}, err
	}
	if res.Code != CodeApproved {
		verdict, err := verdictFor(res)
		return consent.PromptResult{Verdict: verdict}, err
	}
	var att consent.Attestation
	if err := json.Unmarshal(res.Stdout, &att); err != nil {
		return consent.PromptResult{}, fmt.Errorf("parse consent-sign output: %w", err)
	}
	return consent.PromptResult{Verdict: consent.VerdictOK, Attestation: &att}, nil
}

// verdictFor maps the helper's exit-code contract to a consent verdict: 0
// approved, 1 denied, 2 unavailable, and 3 (screen-locked) unavailable too, so
// the engine routes instead of failing. 4 (a caller-pin rejection or usage
// error) is a fatal error the engine must never route around, as is any other
// exit; both carry the stderr diagnostic.
func verdictFor(res Result) (consent.Verdict, error) {
	switch res.Code {
	case CodeApproved:
		return consent.VerdictOK, nil
	case CodeDenied:
		return consent.VerdictDenied, nil
	case CodeUnavailable, CodeScreenLocked:
		return consent.VerdictUnavailable, nil
	case CodeCallerRejected:
		return consent.VerdictFatal, fmt.Errorf("authkit helper rejected the caller or invocation (exit %d): %s", res.Code, bytes.TrimSpace(res.Stderr))
	default:
		return consent.VerdictFatal, fmt.Errorf("authkit helper exited %d: %s", res.Code, bytes.TrimSpace(res.Stderr))
	}
}
