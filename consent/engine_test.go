package consent

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/synckit/presence"
)

func newTestEngine(t *testing.T, prompter Prompter, probe Probe, runner Runner, peers ...string) *Engine {
	t.Helper()
	return NewEngine(staticSelf("me@laptop"), prompter, probe, NewRouter(runner, PresenceCommand), staticResolve(peers...))
}

func decideReq(ttl time.Duration) Request {
	return Request{
		Requestor: "sid:501",
		Client:    "cc-sudo",
		Reason:    "run a privileged command",
		Subject:   "sha256:cafe",
		Argv:      []string{"dscacheutil", "-flushcache"},
		Nonce:     "root-nonce",
		TTL:       ttl,
	}
}

// verdictReq is decideReq without the attestation extension: the verdict-only
// shape cookiesync sends, the only shape the grant store serves.
func verdictReq(ttl time.Duration) Request {
	req := decideReq(ttl)
	req.Argv = nil
	req.Nonce = ""
	return req
}

// TestDecideGrantHitIsServedSilently proves a live grant answers a
// verdict-only request without touching the prompter or the mesh.
func TestDecideGrantHitIsServedSilently(t *testing.T) {
	prompter := &fakePrompter{}
	runner := &approverMesh{}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), runner, "you@desktop")
	e.Grants.Grant("sid:501", []string{"sha256:cafe"}, time.Hour)

	d, err := e.Decide(context.Background(), verdictReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Cached || d.Routed {
		t.Fatalf("Decide = %+v, want a cached local OK", d)
	}
	if d.GrantedUntil.IsZero() {
		t.Fatal("a cached decision must carry the grant expiry")
	}
	if len(prompter.prompts()) != 0 {
		t.Fatalf("a grant hit must not prompt, got %v", prompter.prompts())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("a grant hit must not touch the mesh, got %v", runner.calls)
	}
}

// TestDecideAttendedPromptsLocally proves an attended host prompts locally:
// the approval stamps self as signer, records no grant (attestation asks are
// never cached), and never routes.
func TestDecideAttendedPromptsLocally(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	runner := &approverMesh{}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), runner, "you@desktop")

	d, err := e.Decide(context.Background(), decideReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || d.Routed || d.Cached || d.ApprovedBy != "me@laptop" {
		t.Fatalf("Decide = %+v, want a local un-routed OK approved by me@laptop", d)
	}
	if d.Attestation == nil || d.Attestation.KeyID != "k1" || d.Attestation.SignedBy != "me@laptop" {
		t.Fatalf("attestation = %+v, want key k1 signed by me@laptop", d.Attestation)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("an attended approval must not touch the mesh, got %v", runner.calls)
	}
	if !d.GrantedUntil.IsZero() {
		t.Fatalf("GrantedUntil = %v, want none — attestation approvals never grant", d.GrantedUntil)
	}
	if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
		t.Fatal("an attestation approval must not record a grant — grants are verdict-only")
	}
	// The prompter saw the request verbatim: the opaque extension untouched.
	reqs := prompter.prompts()
	if len(reqs) != 1 || reqs[0].Nonce != "root-nonce" || len(reqs[0].Argv) != 2 {
		t.Fatalf("prompted requests = %+v, want the opaque argv+nonce passed through", reqs)
	}
}

// TestDecideVerdictOnlyApprovalGrants proves a verdict-only approval under a
// positive TTL records a grant the next call is served from silently.
func TestDecideVerdictOnlyApprovalGrants(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictOK}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	first, err := e.Decide(context.Background(), verdictReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if first.Verdict != VerdictOK || first.Cached || first.GrantedUntil.IsZero() {
		t.Fatalf("Decide = %+v, want an uncached OK with a grant window", first)
	}
	second, err := e.Decide(context.Background(), verdictReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if second.Verdict != VerdictOK || !second.Cached {
		t.Fatalf("Decide = %+v, want a cached OK from the grant", second)
	}
	if got := len(prompter.prompts()); got != 1 {
		t.Fatalf("prompts = %d, want 1 — the second call rides the grant", got)
	}
}

// TestDecideAttestationAlwaysPromptsFresh proves the grant cache is
// verdict-only: two attestation asks for one subject under a positive TTL both
// prompt and both return a fresh signature — a cached verdict would carry no
// signature over the second nonce — and neither records a grant.
func TestDecideAttestationAlwaysPromptsFresh(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	for _, nonce := range []string{"root-nonce-1", "root-nonce-2"} {
		req := decideReq(time.Hour)
		req.Nonce = nonce
		d, err := e.Decide(context.Background(), req)
		if err != nil {
			t.Fatalf("Decide(%s): %v", nonce, err)
		}
		if d.Verdict != VerdictOK || d.Cached || d.Attestation == nil || !d.GrantedUntil.IsZero() {
			t.Fatalf("Decide(%s) = %+v, want a fresh signed approval, never a cache hit", nonce, d)
		}
	}
	if got := len(prompter.prompts()); got != 2 {
		t.Fatalf("prompts = %d, want one fresh prompt per attestation ask", got)
	}
	if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
		t.Fatal("attestation approvals must not record grants")
	}
}

// TestDecideAttestationBypassesLiveGrant proves an attestation ask prompts
// fresh even while the requestor holds a live grant for the same subject.
func TestDecideAttestationBypassesLiveGrant(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})
	e.Grants.Grant("sid:501", []string{"sha256:cafe"}, time.Hour)

	d, err := e.Decide(context.Background(), decideReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || d.Cached || d.Attestation == nil {
		t.Fatalf("Decide = %+v, want a fresh signed approval despite the live grant", d)
	}
	if got := len(prompter.prompts()); got != 1 {
		t.Fatalf("prompts = %d, want the grant bypassed with one fresh prompt", got)
	}
}

// TestDecideZeroTTLPromptsEveryCall proves ttl 0 records no grant: every call
// prompts anew.
func TestDecideZeroTTLPromptsEveryCall(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictOK}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	for range 2 {
		d, err := e.Decide(context.Background(), decideReq(0))
		if err != nil {
			t.Fatalf("Decide: %v", err)
		}
		if d.Verdict != VerdictOK || d.Cached || !d.GrantedUntil.IsZero() {
			t.Fatalf("Decide = %+v, want an uncached OK with no grant window", d)
		}
	}
	if got := len(prompter.prompts()); got != 2 {
		t.Fatalf("prompts = %d, want one per call under ttl 0", got)
	}
}

// TestDecideDeniedLocallyIsDenied proves a local denial is terminal and grants
// nothing.
func TestDecideDeniedLocallyIsDenied(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictDenied}}
	runner := &approverMesh{}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), runner, "you@desktop")

	d, err := e.Decide(context.Background(), decideReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictDenied || d.Routed {
		t.Fatalf("Decide = %+v, want an un-routed denial", d)
	}
	if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
		t.Fatal("a denial must never grant")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("a local denial must not route, got %v", runner.calls)
	}
}

// TestDecideFatalPromptIsError proves a fatal — or unrecognized — local-prompt
// verdict surfaces as a fatal error: never approved, never downgraded to
// unavailable (LocalOnly included), and the router is never invoked.
func TestDecideFatalPromptIsError(t *testing.T) {
	tests := []struct {
		name      string
		verdict   Verdict
		localOnly bool
	}{
		{name: "fatal", verdict: VerdictFatal},
		{name: "fatal local-only", verdict: VerdictFatal, localOnly: true},
		{name: "unrecognized verdict", verdict: Verdict(42)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prompter := &fakePrompter{result: PromptResult{Verdict: tc.verdict}}
			runner := &approverMesh{presence: map[string]string{"you@desktop": livePresence}}
			e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), runner, "you@desktop")

			req := decideReq(time.Hour)
			req.LocalOnly = tc.localOnly
			_, err := e.Decide(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), "verdict") {
				t.Fatalf("Decide = %v, want the fatal prompt error", err)
			}
			if Classify(err) != VerdictFatal {
				t.Fatalf("Classify = %v, want VerdictFatal (the RPC layer must answer ok:false)", Classify(err))
			}
			if len(runner.calls) != 0 {
				t.Fatalf("a fatal prompt must never route, got %v", runner.calls)
			}
			if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
				t.Fatal("a fatal prompt must never grant")
			}
		})
	}
}

// TestDecideUnattendedRoutes proves an unattended host routes the gate: the
// relay payload carries the opaque extension, and the peer's signed reply
// comes back as the decision's attestation.
func TestDecideUnattendedRoutes(t *testing.T) {
	peer := "you@desktop"
	endpoint := "me@laptop:sha256:cafe"
	nonce := "echo-nonce"
	prompter := &fakePrompter{}
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: signedReply(t, nonce, endpoint, "peer-key", "cGVlcg", peer)},
	}
	e := newTestEngine(t, prompter, staticProbe(presence.SessionSnapshot{}), runner, peer)
	e.Router.Nonce = func() (string, error) { return nonce, nil }

	d, err := e.Decide(context.Background(), decideReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Routed || d.ApprovedBy != peer {
		t.Fatalf("Decide = %+v, want a routed OK approved by %s", d, peer)
	}
	if d.Attestation == nil || d.Attestation.KeyID != "peer-key" || d.Attestation.SignedBy != peer {
		t.Fatalf("attestation = %+v, want the peer's signature passed through opaquely", d.Attestation)
	}
	if len(prompter.prompts()) != 0 {
		t.Fatalf("a routing origin must not prompt locally, got %v", prompter.prompts())
	}
	if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
		t.Fatal("a routed attestation approval must not record a grant — grants are verdict-only")
	}

	stdins := runner.relayStdins()
	if len(stdins) != 1 {
		t.Fatalf("relay legs = %d, want 1", len(stdins))
	}
	var relay RelayRequest
	if err := json.Unmarshal([]byte(stdins[0]), &relay); err != nil {
		t.Fatalf("parse relay stdin: %v", err)
	}
	want := RelayRequest{
		Client:    "cc-sudo",
		Reason:    "run a privileged command",
		Subject:   "sha256:cafe",
		Nonce:     nonce,
		Endpoint:  endpoint,
		Origin:    "me@laptop",
		Argv:      []string{"dscacheutil", "-flushcache"},
		SignNonce: "root-nonce",
	}
	if relay.Client != want.Client || relay.Reason != want.Reason || relay.Subject != want.Subject ||
		relay.Nonce != want.Nonce || relay.Endpoint != want.Endpoint || relay.Origin != want.Origin ||
		relay.SignNonce != want.SignNonce || !slices.Equal(relay.Argv, want.Argv) {
		t.Fatalf("relay payload = %+v, want %+v", relay, want)
	}
}

// TestDecideLocalGateUnavailableRoutes proves an attended host whose local
// gate answers unavailable (a locked keybag race, exit 3) routes instead of
// failing.
func TestDecideLocalGateUnavailableRoutes(t *testing.T) {
	peer := "you@desktop"
	endpoint := "me@laptop:sha256:cafe"
	nonce := "fallback-nonce"
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictUnavailable}}
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: approvedReply(t, nonce, endpoint)},
	}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), runner, peer)
	e.Router.Nonce = func() (string, error) { return nonce, nil }

	d, err := e.Decide(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Routed {
		t.Fatalf("Decide = %+v, want a routed OK after the local gate answered unavailable", d)
	}
	if got := len(prompter.prompts()); got != 1 {
		t.Fatalf("local prompts = %d, want the one unavailable attempt", got)
	}
}

// TestDecideLocalOnlyNeverRoutes proves LocalOnly pins the gate here: an
// unattended host answers unavailable without touching the mesh.
func TestDecideLocalOnlyNeverRoutes(t *testing.T) {
	runner := &approverMesh{presence: map[string]string{"you@desktop": livePresence}}
	e := newTestEngine(t, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), runner, "you@desktop")

	req := decideReq(0)
	req.LocalOnly = true
	d, err := e.Decide(context.Background(), req)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictUnavailable || d.Routed {
		t.Fatalf("Decide = %+v, want an un-routed unavailable", d)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("LocalOnly must never touch the mesh, got %v", runner.calls)
	}
}

// TestDecideRoutedApprovedByIsAuthenticatedPeer proves ApprovedBy reflects the
// authenticated relay peer, never wire metadata: an approved reply with a
// correct echo, a claimed signed_by, and NO signature yields the peer the
// router actually bound and no attestation.
func TestDecideRoutedApprovedByIsAuthenticatedPeer(t *testing.T) {
	peer := "evil@peer"
	nonce := "echo-nonce"
	endpoint := "me@laptop:sha256:cafe"
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay: map[string]string{peer: relayReply(t, map[string]any{
			"status": "approved", "nonce": nonce, "endpoint": endpoint, "signed_by": "studio",
		})},
	}
	e := newTestEngine(t, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), runner, peer)
	e.Router.Nonce = func() (string, error) { return nonce, nil }

	d, err := e.Decide(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Routed || d.ApprovedBy != peer {
		t.Fatalf("Decide = %+v, want ApprovedBy %q — the authenticated peer, never the wire signed_by", d, peer)
	}
	if d.Attestation != nil {
		t.Fatalf("attestation = %+v, want none for a signature-less reply", d.Attestation)
	}
}

// TestDecideRoutedDenialIsDenied proves a peer's denial folds into a denied
// decision, not an error, and grants nothing.
func TestDecideRoutedDenialIsDenied(t *testing.T) {
	peer := "you@desktop"
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: `{"status":"denied"}`},
	}
	e := newTestEngine(t, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), runner, peer)

	d, err := e.Decide(context.Background(), decideReq(time.Hour))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictDenied || !d.Routed {
		t.Fatalf("Decide = %+v, want a routed denial", d)
	}
	if _, ok := e.Grants.Granted("sid:501", "sha256:cafe"); ok {
		t.Fatal("a routed denial must never grant")
	}
}

// TestDecideRouteExhaustedIsUnavailable proves a walk that binds no approval
// answers unavailable.
func TestDecideRouteExhaustedIsUnavailable(t *testing.T) {
	runner := &approverMesh{presence: map[string]string{"you@desktop": deadPresence}}
	e := newTestEngine(t, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), runner, "you@desktop")

	d, err := e.Decide(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictUnavailable || !d.Routed {
		t.Fatalf("Decide = %+v, want a routed unavailable", d)
	}
}

// TestDecideRouteForwardsCurrentIdentity proves Self resolves fresh per routed
// request: an engine built before the mesh bootstrap (empty identity) forwards
// the identity registered later — never the startup snapshot, whose empty
// origin the peer would sign as a local subject.
func TestDecideRouteForwardsCurrentIdentity(t *testing.T) {
	peer := "you@desktop"
	nonce := "echo-nonce"
	var mu sync.Mutex
	self := ""
	selfFn := func(context.Context) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		return self, nil
	}
	runner := &approverMesh{
		presence: map[string]string{peer: livePresence},
		relay:    map[string]string{peer: approvedReply(t, nonce, "late@laptop:sha256:cafe")},
	}
	e := NewEngine(selfFn, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), pinnedRouter(runner, nonce), staticResolve(peer))

	mu.Lock()
	self = "late@laptop"
	mu.Unlock()

	d, err := e.Decide(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Routed {
		t.Fatalf("Decide = %+v, want a routed OK", d)
	}
	stdins := runner.relayStdins()
	if len(stdins) != 1 {
		t.Fatalf("relay legs = %d, want 1", len(stdins))
	}
	var relay RelayRequest
	if err := json.Unmarshal([]byte(stdins[0]), &relay); err != nil {
		t.Fatalf("parse relay stdin: %v", err)
	}
	if relay.Origin != "late@laptop" || relay.Endpoint != "late@laptop:sha256:cafe" {
		t.Fatalf("relay payload = %+v, want the updated identity forwarded as origin and endpoint", relay)
	}
}

// TestDecideRouteEmptyIdentityFailsClosed proves a routed request never
// forwards the empty (local-reserved) origin: an unresolved identity at route
// time is fatal, and no relay leg fires.
func TestDecideRouteEmptyIdentityFailsClosed(t *testing.T) {
	peer := "you@desktop"
	runner := &approverMesh{presence: map[string]string{peer: livePresence}}
	e := NewEngine(staticSelf(""), &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), pinnedRouter(runner, "n"), staticResolve(peer))

	_, err := e.Decide(context.Background(), decideReq(0))
	if err == nil || !strings.Contains(err.Error(), "mesh identity") {
		t.Fatalf("Decide = %v, want the missing-identity refusal", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("an identity-less route must not touch the mesh, got %v", runner.calls)
	}
}

// serializingPrompter fails the test if two prompts ever overlap.
type serializingPrompter struct {
	t        *testing.T
	inFlight atomic.Int32
}

func (p *serializingPrompter) Prompt(_ context.Context, _ Request) (PromptResult, error) {
	if p.inFlight.Add(1) != 1 {
		p.t.Error("two consent prompts fired concurrently; the engine must serialize sheets")
	}
	time.Sleep(10 * time.Millisecond)
	p.inFlight.Add(-1)
	return PromptResult{Verdict: VerdictOK}, nil
}

// TestPromptsNeverStack proves Decide and Relay share one prompt mutex: two
// concurrent asks never stack two sheets.
func TestPromptsNeverStack(t *testing.T) {
	prompter := &serializingPrompter{t: t}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := e.Decide(context.Background(), decideReq(0)); err != nil {
				t.Errorf("Decide: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := e.Relay(context.Background(), decideReq(0)); err != nil {
				t.Errorf("Relay: %v", err)
			}
		}()
	}
	wg.Wait()
}

// TestRelayAppendsProvenanceForVerdictOnly proves the approver leg stamps
// "— requested from <origin>" server-side on a verdict-only prompt.
func TestRelayAppendsProvenanceForVerdictOnly(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictOK}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	req := Request{
		Requestor: "host:you@desktop",
		Reason:    "run a privileged command",
		Subject:   "sha256:cafe",
		Origin:    "you@desktop",
	}
	d, err := e.Relay(context.Background(), req)
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if d.Verdict != VerdictOK {
		t.Fatalf("Relay = %+v, want OK", d)
	}
	reqs := prompter.prompts()
	if len(reqs) != 1 {
		t.Fatalf("prompts = %d, want 1", len(reqs))
	}
	if want := "run a privileged command — requested from you@desktop"; reqs[0].Reason != want {
		t.Fatalf("prompted reason = %q, want %q", reqs[0].Reason, want)
	}
}

// TestRelaySignPathKeepsReasonAndOrigin proves an attestation-carrying relay
// leaves the reason untouched — the helper displays the argv it hashes and
// appends the provenance itself from Origin — and stamps SignedBy with self.
func TestRelaySignPathKeepsReasonAndOrigin(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	req := Request{
		Requestor: "host:you@desktop",
		Reason:    "run a privileged command",
		Subject:   "sha256:cafe",
		Origin:    "you@desktop",
		Argv:      []string{"rm", "-rf", "/tmp/x"},
		Nonce:     "origin-nonce",
	}
	d, err := e.Relay(context.Background(), req)
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if d.Attestation == nil || d.Attestation.SignedBy != "me@laptop" {
		t.Fatalf("attestation = %+v, want SignedBy me@laptop", d.Attestation)
	}
	reqs := prompter.prompts()
	if len(reqs) != 1 {
		t.Fatalf("prompts = %d, want 1", len(reqs))
	}
	if reqs[0].Reason != "run a privileged command" {
		t.Fatalf("prompted reason = %q, want it untouched on the sign path", reqs[0].Reason)
	}
	if reqs[0].Origin != "you@desktop" || reqs[0].Nonce != "origin-nonce" {
		t.Fatalf("prompted request = %+v, want origin and sign nonce passed through", reqs[0])
	}
}

// TestRelayUnattendedIsUnavailable proves an unattended approver answers
// unavailable without prompting — and never routes onward: the engine has no
// resolver or router wired, so any routing attempt would crash.
func TestRelayUnattendedIsUnavailable(t *testing.T) {
	prompter := &fakePrompter{}
	e := NewEngine(staticSelf("me@laptop"), prompter, staticProbe(presence.SessionSnapshot{}), nil, nil)

	d, err := e.Relay(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if d.Verdict != VerdictUnavailable || d.Routed {
		t.Fatalf("Relay = %+v, want an un-routed unavailable", d)
	}
	if len(prompter.prompts()) != 0 {
		t.Fatalf("an unattended approver must not prompt, got %v", prompter.prompts())
	}
}

// TestRelayProbeErrorIsUnavailable proves an approver-side probe failure is
// retryable by another mesh host, never fatal.
func TestRelayProbeErrorIsUnavailable(t *testing.T) {
	probe := func(_ context.Context) (presence.SessionSnapshot, error) {
		return presence.SessionSnapshot{}, context.DeadlineExceeded
	}
	e := NewEngine(staticSelf("me@laptop"), &fakePrompter{}, probe, nil, nil)

	d, err := e.Relay(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if d.Verdict != VerdictUnavailable {
		t.Fatalf("Relay = %+v, want unavailable on a broken probe", d)
	}
}

// TestRelayDeniedNeverRoutesOnward proves a relayed denial is the terminus —
// no router or resolver is consulted (none is wired; routing would crash).
func TestRelayDeniedNeverRoutesOnward(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictDenied}}
	e := NewEngine(staticSelf("me@laptop"), prompter, staticProbe(attendedSession(t)), nil, nil)

	d, err := e.Relay(context.Background(), decideReq(0))
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if d.Verdict != VerdictDenied || d.Routed {
		t.Fatalf("Relay = %+v, want a terminal denial", d)
	}
}
