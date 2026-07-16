package consent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/yasyf/synckit/rpc"
)

// The method names the consent service answers on the daemon socket.
const (
	MethodRequest  = "consent.request"
	MethodRelay    = "consent.relay"
	MethodPresence = "consent.presence"
)

// Register binds the consent service onto d: consent.request (the local
// consumer surface), consent.relay (the approver leg a peer's routed walk
// shells), and consent.presence (the liveness probe). Every method uses plain
// Register — NEVER RegisterExclusive: the exclusive mutex is shared with
// handlers that take the state flock, and a 10-minute consent prompt behind
// it would wedge the whole daemon.
func Register(d *rpc.Dispatcher, e *Engine) {
	d.Register(MethodRequest, e.handleRequest)
	d.Register(MethodRelay, e.handleRelay)
	d.Register(MethodPresence, e.handlePresence)
}

// handleRequest answers consent.request. The requestor principal derives from
// the calling connection's session id ("sid:" + PeerSID) — never from a
// client-supplied param — so a caller cannot ride another principal's grant.
func (e *Engine) handleRequest(ctx context.Context, params map[string]any) (any, error) {
	sid, ok := rpc.PeerSID(ctx)
	if !ok {
		return nil, errors.New("consent.request: peer session id unavailable on this connection")
	}
	client, err := stringParam(params, "client")
	if err != nil {
		return nil, err
	}
	reason, err := stringParam(params, "reason")
	if err != nil {
		return nil, err
	}
	subject, err := stringParam(params, "subject")
	if err != nil {
		return nil, err
	}
	argv, err := stringsParam(params, "argv")
	if err != nil {
		return nil, err
	}
	nonce, err := optionalString(params, "nonce")
	if err != nil {
		return nil, err
	}
	ttl, err := millisParam(params, "ttl_ms")
	if err != nil {
		return nil, err
	}
	localOnly, err := boolParam(params, "local_only")
	if err != nil {
		return nil, err
	}
	d, err := e.Decide(ctx, Request{
		Requestor: "sid:" + strconv.Itoa(sid),
		Client:    client,
		Reason:    reason,
		Subject:   subject,
		Argv:      argv,
		Nonce:     nonce,
		TTL:       ttl,
		LocalOnly: localOnly,
	})
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"verdict":     d.Verdict.String(),
		"approved_by": d.ApprovedBy,
		"routed":      d.Routed,
		"cached":      d.Cached,
	}
	if !d.GrantedUntil.IsZero() {
		result["granted_until"] = d.GrantedUntil.Format(time.RFC3339)
	}
	if d.Attestation != nil {
		result["attestation"] = map[string]any{
			"key_id":    d.Attestation.KeyID,
			"sig":       d.Attestation.Sig,
			"signed_by": d.Attestation.SignedBy,
		}
	}
	return result, nil
}

// handleRelay answers consent.relay — the approver leg a peer's routed walk
// invokes. An approved reply echoes the request's nonce + endpoint VERBATIM to
// bind the approval; denied and unavailable replies carry the status alone.
func (e *Engine) handleRelay(ctx context.Context, params map[string]any) (any, error) {
	nonce, err := stringParam(params, "nonce")
	if err != nil {
		return nil, err
	}
	endpoint, err := stringParam(params, "endpoint")
	if err != nil {
		return nil, err
	}
	origin, err := stringParam(params, "origin")
	if err != nil {
		return nil, err
	}
	reason, err := stringParam(params, "reason")
	if err != nil {
		return nil, err
	}
	subject, err := stringParam(params, "subject")
	if err != nil {
		return nil, err
	}
	client, err := stringParam(params, "client")
	if err != nil {
		return nil, err
	}
	if client == "" {
		return nil, errors.New(`consent.relay: param "client" must not be empty`)
	}
	argv, err := stringsParam(params, "argv")
	if err != nil {
		return nil, err
	}
	signNonce, err := optionalString(params, "sign_nonce")
	if err != nil {
		return nil, err
	}
	d, err := e.Relay(ctx, Request{
		Requestor: "host:" + origin,
		Client:    client,
		Reason:    reason,
		Subject:   subject,
		Origin:    origin,
		Argv:      argv,
		Nonce:     signNonce,
	})
	if err != nil {
		return nil, err
	}
	switch d.Verdict {
	case VerdictOK:
		reply := map[string]any{"status": statusApproved, "nonce": nonce, "endpoint": endpoint}
		if d.Attestation != nil {
			reply["key_id"] = d.Attestation.KeyID
			reply["sig"] = d.Attestation.Sig
			reply["signed_by"] = d.Attestation.SignedBy
		}
		return reply, nil
	case VerdictDenied:
		return map[string]any{"status": statusDenied}, nil
	case VerdictUnavailable:
		return map[string]any{"status": statusUnavailable}, nil
	case VerdictFatal:
	}
	return nil, fmt.Errorf("consent.relay: unexpected verdict %v", d.Verdict)
}

// handlePresence answers consent.presence: this host's console session
// snapshot, the liveness read a peer's routed walk probe-gates on.
func (e *Engine) handlePresence(ctx context.Context, _ map[string]any) (any, error) {
	snap, err := e.Probe(ctx)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

func stringParam(params map[string]any, key string) (string, error) {
	v, ok := params[key]
	if !ok {
		return "", fmt.Errorf("missing param %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("param %q is %T, want string", key, v)
	}
	return s, nil
}

func optionalString(params map[string]any, key string) (string, error) {
	if _, ok := params[key]; !ok {
		return "", nil
	}
	return stringParam(params, key)
}

func stringsParam(params map[string]any, key string) ([]string, error) {
	v, ok := params[key]
	if !ok {
		return nil, nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("param %q is %T, want an array of strings", key, v)
	}
	out := make([]string, len(raw))
	for i, item := range raw {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("param %q element %d is %T, want string", key, i, item)
		}
		out[i] = s
	}
	return out, nil
}

func millisParam(params map[string]any, key string) (time.Duration, error) {
	v, ok := params[key]
	if !ok {
		return 0, nil
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("param %q is %T, want a number of milliseconds", key, v)
	}
	return time.Duration(f) * time.Millisecond, nil
}

func boolParam(params map[string]any, key string) (bool, error) {
	v, ok := params[key]
	if !ok {
		return false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("param %q is %T, want bool", key, v)
	}
	return b, nil
}
