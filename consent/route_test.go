package consent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/synckit/hostregistry"
)

// TestRouteApprovedBindsNonceAndEndpoint proves the happy path: a live peer's
// reply echoing the exact nonce and endpoint is returned bound, carrying the
// opaque attestation material and the answering peer.
func TestRouteApprovedBindsNonceAndEndpoint(t *testing.T) {
	peer := "you@desktop"
	nonce := "fixed-nonce-abc"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: signedReply(t, nonce, endpoint, "k1", "c2ln", peer)},
	}
	r := pinnedRouter(runner, nonce)

	reply, err := r.Route(context.Background(), []string{peer}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if reply.Peer != peer || reply.Nonce != nonce || reply.Endpoint != endpoint {
		t.Fatalf("Route bound %+v, want peer=%s nonce=%s endpoint=%s", reply, peer, nonce, endpoint)
	}
	if reply.KeyID != "k1" || reply.Sig != "c2ln" || reply.SignedBy != peer {
		t.Fatalf("Route dropped the attestation material: %+v", reply)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != peer {
		t.Fatalf("relay dials = %v, want only %s", asked, peer)
	}
}

// TestRouteNonceMismatchFailsClosed proves a reply whose nonce does not echo
// the one sent is rejected as a security failure (*AuthRequired), never
// retried against another candidate.
func TestRouteNonceMismatchFailsClosed(t *testing.T) {
	liar := "liar@box"
	next := "next@box"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{liar: livePresence, next: livePresence},
		relay: map[string]string{
			liar: approvedReply(t, "WRONG-nonce", endpoint),
			next: approvedReply(t, "the-real-nonce", endpoint),
		},
	}
	r := pinnedRouter(runner, "the-real-nonce")

	_, err := r.Route(context.Background(), []string{liar, next}, endpoint, testAttempt)
	var authErr *AuthRequired
	if !errors.As(err, &authErr) {
		t.Fatalf("nonce mismatch = %v, want *AuthRequired", err)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != liar {
		t.Fatalf("relay dials = %v, want only %s (an unbound approval is terminal)", asked, liar)
	}
}

// TestRouteEndpointMismatchFailsClosed proves a reply whose endpoint does not
// echo the one sent is rejected as *AuthRequired even when the nonce matches.
func TestRouteEndpointMismatchFailsClosed(t *testing.T) {
	peer := "you@desktop"
	nonce := "n1"

	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: approvedReply(t, nonce, "someone-else@host:sha256:beef")},
	}
	r := pinnedRouter(runner, nonce)

	_, err := r.Route(context.Background(), []string{peer}, "me@laptop:sha256:cafe", testAttempt)
	var authErr *AuthRequired
	if !errors.As(err, &authErr) {
		t.Fatalf("endpoint mismatch = %v, want *AuthRequired", err)
	}
}

// TestRouteNoLiveApproverFailsClosed proves a mesh with no live peer fails
// closed with *AuthRequired and never dials a relay leg.
func TestRouteNoLiveApproverFailsClosed(t *testing.T) {
	peer := "you@desktop"
	runner := &approverMesh{presence: map[string]string{peer: deadPresence}}
	r := pinnedRouter(runner, "n")

	_, err := r.Route(context.Background(), []string{peer}, "ep", testAttempt)
	var authErr *AuthRequired
	if !errors.As(err, &authErr) {
		t.Fatalf("no live peer = %v, want *AuthRequired", err)
	}
	if asked := runner.relayTargets(); len(asked) != 0 {
		t.Fatalf("relay dials = %v, want none", asked)
	}
}

// TestRouteFailsOverUnavailableApprover proves a live approver that answers
// unavailable is routed around and the next live approver's approval binds.
func TestRouteFailsOverUnavailableApprover(t *testing.T) {
	broken := "broken@box"
	healthy := "healthy@box"
	nonce := "failover-nonce"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{broken: livePresence, healthy: livePresence},
		relay: map[string]string{
			broken:  `{"status":"unavailable"}`,
			healthy: approvedReply(t, nonce, endpoint),
		},
	}
	r := pinnedRouter(runner, nonce)

	reply, err := r.Route(context.Background(), []string{broken, healthy}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route across an unavailable approver: %v", err)
	}
	if reply.Peer != healthy {
		t.Fatalf("Route bound %s, want %s", reply.Peer, healthy)
	}
	if asked := runner.relayTargets(); len(asked) != 2 || asked[0] != broken || asked[1] != healthy {
		t.Fatalf("relay dials = %v, want [%s %s]", asked, broken, healthy)
	}
}

// TestRouteFailsOverUnreachableApprover proves a transport failure (ssh exit
// 255) on the relay leg advances to the next live approver.
func TestRouteFailsOverUnreachableApprover(t *testing.T) {
	unreachable := "unreachable@box"
	healthy := "healthy@box"
	nonce := "transport-nonce"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		// unreachable answers presence live but has no relay reply scripted,
		// so its relay leg fails at the transport.
		presence: map[string]string{unreachable: livePresence, healthy: livePresence},
		relay:    map[string]string{healthy: approvedReply(t, nonce, endpoint)},
	}
	r := pinnedRouter(runner, nonce)

	reply, err := r.Route(context.Background(), []string{unreachable, healthy}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route across an unreachable approver: %v", err)
	}
	if reply.Peer != healthy {
		t.Fatalf("Route bound %s, want %s", reply.Peer, healthy)
	}
	if asked := runner.relayTargets(); len(asked) != 2 || asked[1] != healthy {
		t.Fatalf("relay dials = %v, want the failover to %s", asked, healthy)
	}
}

// probeErrMesh wraps an approverMesh, failing the presence leg for one target
// with a fixed error; every other call delegates to the embedded mesh.
type probeErrMesh struct {
	*approverMesh
	target string
	err    error
}

func (r *probeErrMesh) Run(ctx context.Context, target, cmd string, stdin []byte) (string, error) {
	if target == r.target && strings.Contains(cmd, "presence") {
		r.mu.Lock()
		r.calls = append(r.calls, runnerCall{target: target, cmd: cmd})
		r.mu.Unlock()
		return "", r.err
	}
	return r.approverMesh.Run(ctx, target, cmd, stdin)
}

// TestRouteFailsOverProbeErrorApprover proves a presence leg that fails with
// an SSHError — even a non-255 remote exit, the shape an approver-side hard
// RPC error produces — routes to the next candidate: a peer that cannot
// answer liveness is not a live approver.
func TestRouteFailsOverProbeErrorApprover(t *testing.T) {
	broken := "broken@box"
	healthy := "healthy@box"
	nonce := "probe-err-nonce"
	endpoint := "me@laptop:sha256:cafe"

	runner := &probeErrMesh{
		approverMesh: &approverMesh{
			presence: map[string]string{healthy: livePresence},
			relay:    map[string]string{healthy: approvedReply(t, nonce, endpoint)},
		},
		target: broken,
		err:    remoteExitFailure(t, broken),
	}
	r := pinnedRouter(runner, nonce)

	reply, err := r.Route(context.Background(), []string{broken, healthy}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route across a probe-erroring approver: %v", err)
	}
	if reply.Peer != healthy {
		t.Fatalf("Route bound %s, want %s", reply.Peer, healthy)
	}
	if probed := runner.probedTargets(); len(probed) != 2 || probed[0] != broken || probed[1] != healthy {
		t.Fatalf("presence probes = %v, want [%s %s]", probed, broken, healthy)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != healthy {
		t.Fatalf("relay dials = %v, want only %s", asked, healthy)
	}
}

// TestRouteAdvancesPastWedgedProbe proves a liveness probe timeout routes
// around the wedged candidate instead of failing the walk.
func TestRouteAdvancesPastWedgedProbe(t *testing.T) {
	wedged := "wedged@box"
	healthy := "healthy@box"
	nonce := "wedged-probe-nonce"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		wedgedProbe: wedged,
		presence:    map[string]string{healthy: livePresence},
		relay:       map[string]string{healthy: approvedReply(t, nonce, endpoint)},
	}
	r := pinnedRouter(runner, nonce)
	r.ProbeTimeout = 25 * time.Millisecond

	reply, err := r.Route(context.Background(), []string{wedged, healthy}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route across a wedged probe: %v", err)
	}
	if reply.Peer != healthy {
		t.Fatalf("Route bound %s, want %s", reply.Peer, healthy)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != healthy {
		t.Fatalf("relay dials = %v, want only %s (the wedged probe is routed around)", asked, healthy)
	}
}

// TestRouteAdvancesPastWedgedRelayLeg proves a relay leg that outruns
// RelayTimeout routes around the wedged approver.
func TestRouteAdvancesPastWedgedRelayLeg(t *testing.T) {
	slow := "slow@box"
	healthy := "healthy@box"
	nonce := "wedged-relay-nonce"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		wedgedRelay: slow,
		presence:    map[string]string{slow: livePresence, healthy: livePresence},
		relay:       map[string]string{healthy: approvedReply(t, nonce, endpoint)},
	}
	r := pinnedRouter(runner, nonce)
	r.RelayTimeout = 25 * time.Millisecond

	reply, err := r.Route(context.Background(), []string{slow, healthy}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route across a wedged relay leg: %v", err)
	}
	if reply.Peer != healthy {
		t.Fatalf("Route bound %s, want %s", reply.Peer, healthy)
	}
	if asked := runner.relayTargets(); len(asked) != 2 || asked[0] != slow || asked[1] != healthy {
		t.Fatalf("relay dials = %v, want [%s %s]", asked, slow, healthy)
	}
}

// TestRouteNon255SSHErrorIsFatal proves a relay-leg SSHError wrapping a real
// remote exit (not ssh's own exit-255 connection failure) propagates fatally:
// no later approver is asked.
func TestRouteNon255SSHErrorIsFatal(t *testing.T) {
	broken := "broken@box"
	next := "next@box"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{broken: livePresence, next: livePresence},
		relayErr: map[string]error{broken: remoteExitFailure(t, broken)},
		relay:    map[string]string{next: approvedReply(t, "n", endpoint)},
	}
	r := pinnedRouter(runner, "n")

	_, err := r.Route(context.Background(), []string{broken, next}, endpoint, testAttempt)
	if err == nil || !strings.Contains(err.Error(), "consent relay to") {
		t.Fatalf("non-255 SSHError = %v, want the fatal relay failure", err)
	}
	var sshErr *hostregistry.SSHError
	if !errors.As(err, &sshErr) {
		t.Fatalf("non-255 SSHError = %v, want the wrapped *hostregistry.SSHError", err)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != broken {
		t.Fatalf("relay dials = %v, want only %s (a real remote exit is fatal, not a skip)", asked, broken)
	}
}

// TestRouteMalformedPresenceIsFatal proves a presence reply that does not
// parse propagates as a fatal error — never silently routed around as
// peer-offline — so a protocol bug in a peer's daemon cannot hide behind the
// failover.
func TestRouteMalformedPresenceIsFatal(t *testing.T) {
	broken := "broken@box"
	healthy := "healthy@box"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{broken: `{"on_console": tru`, healthy: livePresence},
		relay:    map[string]string{healthy: approvedReply(t, "n", endpoint)},
	}
	r := pinnedRouter(runner, "n")

	_, err := r.Route(context.Background(), []string{broken, healthy}, endpoint, testAttempt)
	if err == nil || !strings.Contains(err.Error(), "parse presence") {
		t.Fatalf("malformed presence = %v, want the parse failure to propagate", err)
	}
	if asked := runner.relayTargets(); len(asked) != 0 {
		t.Fatalf("relay dials = %v, want none (a protocol failure is fatal, not a skip)", asked)
	}
}

// TestRouteMalformedReplyIsFatal proves a relay reply that does not parse
// propagates as a fatal error and never advances to the next approver.
func TestRouteMalformedReplyIsFatal(t *testing.T) {
	broken := "broken@box"
	next := "next@box"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{broken: livePresence, next: livePresence},
		relay: map[string]string{
			broken: `{not json`,
			next:   approvedReply(t, "n", endpoint),
		},
	}
	r := pinnedRouter(runner, "n")

	_, err := r.Route(context.Background(), []string{broken, next}, endpoint, testAttempt)
	if err == nil || !strings.Contains(err.Error(), "parse consent reply") {
		t.Fatalf("malformed relay reply = %v, want the parse failure to propagate", err)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != broken {
		t.Fatalf("relay dials = %v, want only %s (a protocol failure is fatal, not a skip)", asked, broken)
	}
}

// TestRouteUnexpectedStatusIsFatal proves a reply with an unknown status
// propagates fatally instead of being coerced into a verdict.
func TestRouteUnexpectedStatusIsFatal(t *testing.T) {
	peer := "weird@box"
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: `{"status":"maybe"}`},
	}
	r := pinnedRouter(runner, "n")

	_, err := r.Route(context.Background(), []string{peer}, "ep", testAttempt)
	if err == nil || !strings.Contains(err.Error(), `unexpected status "maybe"`) {
		t.Fatalf("unexpected status = %v, want the fatal status failure", err)
	}
}

// TestRouteDeniedIsTerminal proves a human denial short-circuits the failover:
// the error is *Denied and no later approver is ever asked.
func TestRouteDeniedIsTerminal(t *testing.T) {
	denier := "denier@box"
	next := "next@box"
	endpoint := "me@laptop:sha256:cafe"

	runner := &approverMesh{
		presence: map[string]string{denier: livePresence, next: livePresence},
		relay: map[string]string{
			denier: `{"status":"denied"}`,
			next:   approvedReply(t, "denied-nonce", endpoint),
		},
	}
	r := pinnedRouter(runner, "denied-nonce")

	_, err := r.Route(context.Background(), []string{denier, next}, endpoint, testAttempt)
	var denied *Denied
	if !errors.As(err, &denied) || denied.Peer != denier {
		t.Fatalf("denied consent = %v, want *Denied from %s", err, denier)
	}
	if asked := runner.relayTargets(); len(asked) != 1 || asked[0] != denier {
		t.Fatalf("relay dials = %v, want only the denier %s (a denial is terminal)", asked, denier)
	}
}

// nonceEchoRunner answers unavailable on the first relay leg and echoes the
// attempt's own stdin nonce on the second, recording every nonce it saw.
type nonceEchoRunner struct {
	endpoint string

	mu     sync.Mutex
	nonces []string
}

func (r *nonceEchoRunner) Run(_ context.Context, _, cmd string, stdin []byte) (string, error) {
	if strings.Contains(cmd, "presence") {
		return livePresence, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nonces = append(r.nonces, string(stdin))
	if len(r.nonces) == 1 {
		return `{"status":"unavailable"}`, nil
	}
	reply, err := json.Marshal(map[string]any{"status": "approved", "nonce": string(stdin), "endpoint": r.endpoint})
	return string(reply), err
}

// TestRouteMintsFreshNoncePerAttempt proves each attempt binds its own fresh
// nonce, minted inside the per-peer loop — never hoisted across the walk.
func TestRouteMintsFreshNoncePerAttempt(t *testing.T) {
	endpoint := "me@laptop:sha256:cafe"
	runner := &nonceEchoRunner{endpoint: endpoint}
	r := NewRouter(runner, PresenceCommand)

	reply, err := r.Route(context.Background(), []string{"first@box", "second@box"}, endpoint, testAttempt)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(runner.nonces) != 2 {
		t.Fatalf("relay attempts = %d, want 2", len(runner.nonces))
	}
	if runner.nonces[0] == runner.nonces[1] {
		t.Fatalf("both attempts bound nonce %q; each attempt must mint its own", runner.nonces[0])
	}
	if reply.Nonce != runner.nonces[1] {
		t.Fatalf("bound nonce %q, want the second attempt's %q", reply.Nonce, runner.nonces[1])
	}
}

// TestNewNonceShape proves nonces are 32-char URL-safe base64 of 24 bytes and
// never repeat across 200 mints.
func TestNewNonceShape(t *testing.T) {
	seen := map[string]int{}
	for range 200 {
		n, err := NewNonce()
		if err != nil {
			t.Fatalf("NewNonce: %v", err)
		}
		if len(n) != 32 {
			t.Fatalf("nonce %q has length %d, want 32 (url-safe base64 of 24 bytes)", n, len(n))
		}
		seen[n]++
	}
	if len(seen) != 200 {
		t.Fatalf("expected 200 distinct nonces, got %d (reuse detected)", len(seen))
	}
}

// TestLiveExcludesScreenSharedPeer proves Live treats a screen-shared peer as
// not live — its Touch ID prompt could be tapped by the remote viewer — while
// an on-console, unlocked, un-shared peer is live.
func TestLiveExcludesScreenSharedPeer(t *testing.T) {
	peer := "you@desktop"
	tests := []struct {
		name     string
		presence string
		want     bool
	}{
		{
			name:     "on-console unlocked un-shared is live",
			presence: `{"on_console":true,"locked":false,"console_user":"peer","screen_shared":false}`,
			want:     true,
		},
		{
			name:     "screen-shared peer is not live",
			presence: `{"on_console":true,"locked":false,"console_user":"peer","screen_shared":true}`,
			want:     false,
		},
		{
			name:     "locked peer is not live",
			presence: `{"on_console":true,"locked":true,"console_user":"peer","screen_shared":false}`,
			want:     false,
		},
		{
			name:     "off-console peer is not live",
			presence: `{"on_console":false,"locked":false,"console_user":"","screen_shared":false}`,
			want:     false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &approverMesh{presence: map[string]string{peer: tc.presence}}
			r := NewRouter(runner, PresenceCommand)
			got, err := r.Live(context.Background(), peer)
			if err != nil {
				t.Fatalf("Live: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Live = %v, want %v", got, tc.want)
			}
		})
	}
}

// blockingRunner parks every Run until its context dies, then reports the
// kill — the double for a wedged peer probe.
type blockingRunner struct{}

func (blockingRunner) Run(ctx context.Context, _, _ string, _ []byte) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

// TestLiveBoundedByProbeTimeout proves the liveness probe carries its own
// short bound: a wedged peer fails the probe in ProbeTimeout instead of
// riding a consent flight's whole window.
func TestLiveBoundedByProbeTimeout(t *testing.T) {
	r := NewRouter(blockingRunner{}, PresenceCommand)
	r.ProbeTimeout = 25 * time.Millisecond

	start := time.Now()
	live, err := r.Live(context.Background(), "wedged@desktop")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Live took %v; ProbeTimeout must bound the probe near %v", elapsed, r.ProbeTimeout)
	}
	if live {
		t.Fatalf("a wedged peer must not read as live")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Live = %v, want the probe deadline", err)
	}
}
