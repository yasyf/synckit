package consent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/presence"
	"github.com/yasyf/synckit/rpc"
	"golang.org/x/sys/unix"
)

// TestRequestDerivesRequestorFromPeerSID drives consent.request over a real
// unix socket and proves the requestor is "sid:" + the caller's session id —
// server-derived, never a client param — and the frozen result shape holds.
func TestRequestDerivesRequestorFromPeerSID(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("peer session ids ride LOCAL_PEERPID, macOS-only")
	}
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})
	d := rpc.NewDispatcher()
	Register(d, e)

	// A short MkdirTemp dir, not t.TempDir(): the test's long name would
	// overflow the 104-byte sockaddr_un path limit.
	dir, err := os.MkdirTemp("", "consentsock")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	ln, err := rpc.Listen(context.Background(), sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rpc.NewServer(d).Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-done
	})

	client := rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild})
	defer func() { _ = client.Close() }()
	resp, err := client.Call(context.Background(), &rpc.Request{
		Method: MethodRequest,
		Params: map[string]any{
			"client":  "cc-sudo",
			"reason":  "run a privileged command",
			"subject": "sha256:cafe",
			"argv":    []any{"dscacheutil", "-flushcache"},
			"nonce":   "root-nonce",
			"ttl_ms":  60000.0,
		},
	})
	if err != nil {
		t.Fatalf("call consent.request: %v", err)
	}
	if !resp.OK {
		t.Fatalf("consent.request failed: %s", resp.Error)
	}

	sid, err := unix.Getsid(os.Getpid())
	if err != nil {
		t.Fatalf("getsid: %v", err)
	}
	reqs := prompter.prompts()
	if len(reqs) != 1 {
		t.Fatalf("prompts = %d, want 1", len(reqs))
	}
	if want := "sid:" + strconv.Itoa(sid); reqs[0].Requestor != want {
		t.Fatalf("requestor = %q, want the server-derived %q", reqs[0].Requestor, want)
	}
	if reqs[0].TTL != time.Minute {
		t.Fatalf("ttl = %v, want 1m from ttl_ms", reqs[0].TTL)
	}

	var result map[string]any
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["verdict"] != "approved" || result["routed"] != false || result["cached"] != false {
		t.Fatalf("result = %v, want an un-routed uncached approval", result)
	}
	if _, ok := result["granted_until"]; ok {
		t.Fatalf("result = %v, want no granted_until — an attestation approval never grants", result)
	}
	att, ok := result["attestation"].(map[string]any)
	if !ok {
		t.Fatalf("result = %v, want the attestation object", result)
	}
	if att["key_id"] != "k1" || att["sig"] != "c2ln" || att["signed_by"] != "me@laptop" {
		t.Fatalf("attestation = %v, want key k1 sig c2ln signed by me@laptop", att)
	}
}

// TestRequestWithoutPeerSIDFails proves consent.request refuses a connection
// whose session id is unknown instead of trusting anything client-supplied.
func TestRequestWithoutPeerSIDFails(t *testing.T) {
	e := newTestEngine(t, &fakePrompter{}, staticProbe(attendedSession(t)), &approverMesh{})

	_, err := e.handleRequest(context.Background(), map[string]any{
		"client": "cc-sudo", "reason": "r", "subject": "s",
	})
	if err == nil || !strings.Contains(err.Error(), "peer session id unavailable") {
		t.Fatalf("handleRequest on a bare ctx = %v, want the missing-peercred refusal", err)
	}
}

func relayParams() map[string]any {
	return map[string]any{
		"client":     "cc-sudo",
		"reason":     "run a privileged command",
		"subject":    "sha256:cafe",
		"nonce":      "echo-nonce",
		"endpoint":   "you@desktop:sha256:cafe",
		"origin":     "you@desktop",
		"argv":       []any{"dscacheutil", "-flushcache"},
		"sign_nonce": "origin-nonce",
	}
}

// TestRelayHandlerEchoesVerbatim proves an approved consent.relay reply echoes
// the request's exact nonce + endpoint and carries the signature material.
func TestRelayHandlerEchoesVerbatim(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{
		Verdict:     VerdictOK,
		Attestation: &Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	got, err := e.handleRelay(context.Background(), relayParams())
	if err != nil {
		t.Fatalf("handleRelay: %v", err)
	}
	reply := got.(map[string]any)
	if reply["status"] != "approved" || reply["nonce"] != "echo-nonce" || reply["endpoint"] != "you@desktop:sha256:cafe" {
		t.Fatalf("reply = %v, want the verbatim nonce+endpoint echo", reply)
	}
	if reply["key_id"] != "k1" || reply["sig"] != "c2ln" || reply["signed_by"] != "me@laptop" {
		t.Fatalf("reply = %v, want the signature material", reply)
	}
	reqs := prompter.prompts()
	if len(reqs) != 1 || reqs[0].Requestor != "host:you@desktop" {
		t.Fatalf("prompted requests = %+v, want requestor host:you@desktop", reqs)
	}
	if reqs[0].Origin != "you@desktop" {
		t.Fatalf("prompted origin = %q, want the sent origin bound through to the prompter", reqs[0].Origin)
	}
	if reqs[0].Nonce != "origin-nonce" || !slices.Equal(reqs[0].Argv, []string{"dscacheutil", "-flushcache"}) {
		t.Fatalf("prompted request = %+v, want the opaque sign material passed through", reqs[0])
	}
}

// TestRelayHandlerRequiresClient proves the frozen relay shape's required
// client is enforced fail-closed: an omitted or empty client is rejected
// before any prompt fires.
func TestRelayHandlerRequiresClient(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"omitted", func(p map[string]any) { delete(p, "client") }},
		{"empty", func(p map[string]any) { p["client"] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prompter := &fakePrompter{result: PromptResult{Verdict: VerdictOK}}
			e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

			params := relayParams()
			tc.mutate(params)
			_, err := e.handleRelay(context.Background(), params)
			if err == nil || !strings.Contains(err.Error(), "client") {
				t.Fatalf("handleRelay = %v, want the missing-client refusal", err)
			}
			if got := prompter.prompts(); len(got) != 0 {
				t.Fatalf("a client-less relay must be rejected before any prompt, got %v", got)
			}
		})
	}
}

// TestRelayHandlerDeniedCarriesStatusOnly proves a denial replies with the
// bare status — no echo material a requestor could mistake for a binding.
func TestRelayHandlerDeniedCarriesStatusOnly(t *testing.T) {
	prompter := &fakePrompter{result: PromptResult{Verdict: VerdictDenied}}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})

	got, err := e.handleRelay(context.Background(), relayParams())
	if err != nil {
		t.Fatalf("handleRelay: %v", err)
	}
	reply := got.(map[string]any)
	if len(reply) != 1 || reply["status"] != "denied" {
		t.Fatalf("reply = %v, want exactly {status: denied}", reply)
	}
}

// TestRelayHandlerUnattendedIsUnavailable proves an unattended approver
// answers the bare unavailable status without prompting.
func TestRelayHandlerUnattendedIsUnavailable(t *testing.T) {
	prompter := &fakePrompter{}
	e := NewEngine(staticSelf("me@laptop"), prompter, staticProbe(presence.SessionSnapshot{}), nil, nil)

	got, err := e.handleRelay(context.Background(), relayParams())
	if err != nil {
		t.Fatalf("handleRelay: %v", err)
	}
	reply := got.(map[string]any)
	if len(reply) != 1 || reply["status"] != "unavailable" {
		t.Fatalf("reply = %v, want exactly {status: unavailable}", reply)
	}
	if len(prompter.prompts()) != 0 {
		t.Fatalf("an unattended approver must not prompt, got %v", prompter.prompts())
	}
}

// TestPresenceHandlerAnswersWireSnapshot proves consent.presence returns the
// probe's snapshot, which serializes with the wire keys Live parses.
func TestPresenceHandlerAnswersWireSnapshot(t *testing.T) {
	e := NewEngine(staticSelf("me@laptop"), &fakePrompter{}, staticProbe(attendedSession(t)), nil, nil)

	got, err := e.handlePresence(context.Background(), nil)
	if err != nil {
		t.Fatalf("handlePresence: %v", err)
	}
	payload, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	for _, key := range []string{`"on_console":true`, `"locked":false`, `"console_user":"` + currentUser(t) + `"`, `"screen_shared":false`} {
		if !strings.Contains(string(payload), key) {
			t.Fatalf("presence payload %s missing %s", payload, key)
		}
	}
}

// parkedPrompter blocks every prompt until released.
type parkedPrompter struct {
	entered   chan struct{}
	release   chan struct{}
	onceEnter sync.Once
}

func (p *parkedPrompter) Prompt(_ context.Context, _ Request) (PromptResult, error) {
	p.onceEnter.Do(func() { close(p.entered) })
	<-p.release
	return PromptResult{Verdict: VerdictOK}, nil
}

// TestConsentMethodsDispatchConcurrently proves the consent methods ride plain
// Register: a parked consent.relay prompt does not queue consent.presence
// behind an exclusive mutex — the wedge RegisterExclusive would cause.
func TestConsentMethodsDispatchConcurrently(t *testing.T) {
	prompter := &parkedPrompter{entered: make(chan struct{}), release: make(chan struct{})}
	e := newTestEngine(t, prompter, staticProbe(attendedSession(t)), &approverMesh{})
	d := rpc.NewDispatcher()
	Register(d, e)

	relayDone := make(chan *rpc.Response, 1)
	go func() {
		relayDone <- d.Dispatch(context.Background(), &rpc.Request{Method: MethodRelay, Params: relayParams()})
	}()
	<-prompter.entered

	resp := d.Dispatch(context.Background(), &rpc.Request{Method: MethodPresence, Params: map[string]any{}})
	if !resp.OK {
		t.Fatalf("consent.presence while a relay prompt is parked: %s", resp.Error)
	}

	close(prompter.release)
	if resp := <-relayDone; !resp.OK {
		t.Fatalf("parked consent.relay: %s", resp.Error)
	}
}
