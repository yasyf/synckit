package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/synckit/consent"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/presence"
	"github.com/yasyf/synckit/rpc"
)

// fakeGate scripts the local consent gate: it records every prompted request and
// answers with the scripted result, standing in for the signed authkit helper so
// a headless CI runner never execs a helper or LAContext.
type fakeGate struct {
	result consent.PromptResult

	mu   sync.Mutex
	reqs []consent.Request
}

func (g *fakeGate) Prompt(_ context.Context, req consent.Request) (consent.PromptResult, error) {
	g.mu.Lock()
	g.reqs = append(g.reqs, req)
	g.mu.Unlock()
	return g.result, nil
}

func (g *fakeGate) prompts() []consent.Request {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]consent.Request(nil), g.reqs...)
}

// recordingRunner counts the ssh legs the router attempts; a routed relay must
// never fan out from an approver, so tests assert this stays zero.
type recordingRunner struct {
	mu    sync.Mutex
	calls int
}

func (r *recordingRunner) Run(context.Context, string, string, []byte) (string, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return "", errors.New("recordingRunner: no relay reply scripted")
}

func (r *recordingRunner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func staticProbe(snap presence.SessionSnapshot) consent.Probe {
	return func(context.Context) (presence.SessionSnapshot, error) { return snap, nil }
}

func staticResolve(peers ...string) consent.ResolveFunc {
	return func(context.Context) ([]string, error) { return peers, nil }
}

func staticSelf(name string) consent.SelfFunc {
	return func(context.Context) (string, error) { return name, nil }
}

func currentUser(t *testing.T) string {
	t.Helper()
	me, err := user.Current()
	if err != nil {
		t.Fatalf("resolve current user: %v", err)
	}
	return me.Username
}

// attendedSnapshot is a snapshot presence.Attended reports true for: the current
// user on console, unlocked, un-shared.
func attendedSnapshot(t *testing.T) presence.SessionSnapshot {
	t.Helper()
	return presence.SessionSnapshot{OnConsole: true, ConsoleUser: currentUser(t)}
}

// shortConfigHome points hostregistry.Mesh at a fresh, short config dir. A short
// base, not t.TempDir(): Mesh.SockPath() must fit the 104-byte sockaddr_un limit,
// which the deep t.TempDir() path would overflow.
func shortConfigHome(t *testing.T) {
	t.Helper()
	base, err := os.MkdirTemp("", "sk")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	t.Setenv("XDG_CONFIG_HOME", base)
	if err := os.MkdirAll(filepath.Join(base, hostregistry.MeshName), 0o700); err != nil {
		t.Fatalf("mkdir mesh config dir: %v", err)
	}
}

// serveConsentEngine binds engine onto a dispatcher and serves it on the mesh
// socket the consent CLI subcommands dial, returning once the listener is bound.
func serveConsentEngine(t *testing.T, engine *consent.Engine) {
	t.Helper()
	shortConfigHome(t)
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		t.Fatalf("sockpath: %v", err)
	}
	ln, err := rpc.Listen(sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d := rpc.NewDispatcher()
	consent.Register(d, engine)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rpc.Serve(ctx, ln, d)
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-done
	})
}

// TestConsentRequestCLIApprovesOverSocket drives `synckitd consent request`
// through a real unix socket and proves the printed decision is an un-routed
// approval carrying the attestation the gate returned, signed by this host.
func TestConsentRequestCLIApprovesOverSocket(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("consent.request derives the requestor from LOCAL_PEERPID, darwin-only")
	}
	gate := &fakeGate{result: consent.PromptResult{
		Verdict:     consent.VerdictOK,
		Attestation: &consent.Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	engine := consent.NewEngine(staticSelf("me@self"), gate, staticProbe(attendedSnapshot(t)),
		consent.NewRouter(&recordingRunner{}, consent.PresenceCommand), staticResolve())
	serveConsentEngine(t, engine)

	var out bytes.Buffer
	cmd := newConsentRequestCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--client", "cc-sudo", "--reason", "flush the cache", "--subject", "sha256:cafe",
		"--argv", "dscacheutil", "--argv", "-flushcache", "--nonce", "root-nonce",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("consent request: %v", err)
	}

	var res map[string]any
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("parse output %q: %v", out.String(), err)
	}
	if res["verdict"] != "approved" || res["routed"] != false {
		t.Fatalf("result = %v, want an un-routed approval", res)
	}
	att, ok := res["attestation"].(map[string]any)
	if !ok || att["key_id"] != "k1" || att["sig"] != "c2ln" || att["signed_by"] != "me@self" {
		t.Fatalf("attestation = %v, want key k1 sig c2ln signed by me@self", res["attestation"])
	}
	reqs := gate.prompts()
	if len(reqs) != 1 || reqs[0].Nonce != "root-nonce" || strings.Join(reqs[0].Argv, " ") != "dscacheutil -flushcache" {
		t.Fatalf("prompted = %+v, want the argv and nonce carried through", reqs)
	}
}

// TestConsentRelayCLIForwardsAndEchoes drives `synckitd consent relay` with a
// request on stdin and proves the reply echoes the request's exact nonce +
// endpoint, carries the signature material signed by this host, and prompts the
// origin as its requestor with the opaque sign material passed through.
func TestConsentRelayCLIForwardsAndEchoes(t *testing.T) {
	gate := &fakeGate{result: consent.PromptResult{
		Verdict:     consent.VerdictOK,
		Attestation: &consent.Attestation{KeyID: "k1", Sig: "c2ln"},
	}}
	engine := consent.NewEngine(staticSelf("me@peer"), gate, staticProbe(attendedSnapshot(t)),
		consent.NewRouter(&recordingRunner{}, consent.PresenceCommand), staticResolve())
	serveConsentEngine(t, engine)

	stdin := `{"client":"cc-sudo","reason":"flush","subject":"sha256:cafe","nonce":"echo-nonce",` +
		`"endpoint":"you@origin:sha256:cafe","origin":"you@origin",` +
		`"argv":["dscacheutil","-flushcache"],"sign_nonce":"origin-nonce"}`
	var out bytes.Buffer
	cmd := newConsentRelayCmd()
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("consent relay: %v", err)
	}

	var reply map[string]any
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
		t.Fatalf("parse reply %q: %v", out.String(), err)
	}
	if reply["status"] != "approved" || reply["nonce"] != "echo-nonce" || reply["endpoint"] != "you@origin:sha256:cafe" {
		t.Fatalf("reply = %v, want the verbatim nonce+endpoint echo", reply)
	}
	if reply["key_id"] != "k1" || reply["sig"] != "c2ln" || reply["signed_by"] != "me@peer" {
		t.Fatalf("reply = %v, want the signature material signed by me@peer", reply)
	}
	reqs := gate.prompts()
	if len(reqs) != 1 || reqs[0].Requestor != "host:you@origin" {
		t.Fatalf("prompted = %+v, want one prompt as host:you@origin", reqs)
	}
	if reqs[0].Nonce != "origin-nonce" || strings.Join(reqs[0].Argv, " ") != "dscacheutil -flushcache" {
		t.Fatalf("prompted request = %+v, want the opaque sign material passed through", reqs[0])
	}
}

// TestConsentRelayCLIDeniedIsTerminal proves a human denial rides back as the
// bare {status: denied} — no echo material — so the origin's router treats it as
// terminal rather than routing around to another approver.
func TestConsentRelayCLIDeniedIsTerminal(t *testing.T) {
	gate := &fakeGate{result: consent.PromptResult{Verdict: consent.VerdictDenied}}
	engine := consent.NewEngine(staticSelf("me@peer"), gate, staticProbe(attendedSnapshot(t)),
		consent.NewRouter(&recordingRunner{}, consent.PresenceCommand), staticResolve())
	serveConsentEngine(t, engine)

	stdin := `{"reason":"r","subject":"s","nonce":"n","endpoint":"e","origin":"you@origin"}`
	var out bytes.Buffer
	cmd := newConsentRelayCmd()
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("consent relay: %v", err)
	}

	var reply map[string]any
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
		t.Fatalf("parse reply %q: %v", out.String(), err)
	}
	if len(reply) != 1 || reply["status"] != "denied" {
		t.Fatalf("reply = %v, want exactly {status: denied}", reply)
	}
}

// TestConsentRelayCLINeverRoutesOnward proves the approver leg is the routed
// gate's terminus: it prompts locally and never fans out an ssh leg, and it
// ignores a caller-injected route_to a hostile origin might try to smuggle in.
func TestConsentRelayCLINeverRoutesOnward(t *testing.T) {
	runner := &recordingRunner{}
	gate := &fakeGate{result: consent.PromptResult{Verdict: consent.VerdictOK}}
	engine := consent.NewEngine(staticSelf("me@peer"), gate, staticProbe(attendedSnapshot(t)),
		consent.NewRouter(runner, consent.PresenceCommand), staticResolve("other@peer2"))
	serveConsentEngine(t, engine)

	stdin := `{"reason":"r","subject":"s","nonce":"n","endpoint":"e","origin":"you@origin","route_to":"attacker@evil"}`
	var out bytes.Buffer
	cmd := newConsentRelayCmd()
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("consent relay: %v", err)
	}
	if n := runner.count(); n != 0 {
		t.Fatalf("approver leg made %d ssh calls, want 0 — the relay is the terminus and never routes onward", n)
	}
}

// TestConsentRelayCLINeverExits255 proves the ssh-invoked relay leg never
// surfaces a per-peer failure as a non-zero (and thus never a 255) exit: an
// unreachable daemon and a malformed request both resolve to an unavailable
// reply with a nil error, so synckit's exit-255 ssh failover never double-prompts.
func TestConsentRelayCLINeverExits255(t *testing.T) {
	shortConfigHome(t) // point at a config dir whose socket has no daemon bound

	tests := []struct {
		name  string
		stdin string
	}{
		{"unreachable daemon", `{"reason":"r","subject":"s","nonce":"n","endpoint":"e","origin":"o"}`},
		{"malformed request", "not json at all"},
		{"empty request", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := runRelay(context.Background(), strings.NewReader(tt.stdin), &out); err != nil {
				t.Fatalf("runRelay surfaced an error (would exit non-zero, risking 255): %v", err)
			}
			var reply map[string]any
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
				t.Fatalf("parse reply %q: %v", out.String(), err)
			}
			if reply["status"] != "unavailable" {
				t.Fatalf("reply = %v, want unavailable so the origin routes around", reply)
			}
		})
	}
}

// TestConsentPresenceCLIPrintsWireSnapshot proves `synckitd consent presence`
// prints the console snapshot in exactly the wire shape a peer's router
// unmarshals into presence.SessionSnapshot when probe-gating a candidate.
func TestConsentPresenceCLIPrintsWireSnapshot(t *testing.T) {
	snap := presence.SessionSnapshot{OnConsole: true, ConsoleUser: currentUser(t)}
	engine := consent.NewEngine(staticSelf("me@self"), &fakeGate{}, staticProbe(snap),
		consent.NewRouter(&recordingRunner{}, consent.PresenceCommand), staticResolve())
	serveConsentEngine(t, engine)

	var out bytes.Buffer
	cmd := newConsentPresenceCmd()
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("consent presence: %v", err)
	}
	var got presence.SessionSnapshot
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("parse snapshot %q: %v", out.String(), err)
	}
	if !got.OnConsole || got.Locked || got.ConsoleUser != currentUser(t) {
		t.Fatalf("snapshot = %+v, want the wire snapshot the probe returned", got)
	}
}

// TestConsentDispatchesUnderParkedExclusive proves the consent methods ride plain
// Register: a parked exclusive reconcile holds the shared mutex, yet consent.
// presence still dispatches — the wedge RegisterExclusive would have caused with
// a 10-minute Touch ID prompt.
func TestConsentDispatchesUnderParkedExclusive(t *testing.T) {
	orig := buildConsentEngine
	t.Cleanup(func() { buildConsentEngine = orig })
	buildConsentEngine = func() *consent.Engine {
		return consent.NewEngine(staticSelf("me@self"), &fakeGate{}, staticProbe(presence.SessionSnapshot{}),
			consent.NewRouter(&recordingRunner{}, consent.PresenceCommand), staticResolve())
	}

	d := rpc.NewDispatcher()
	entered := make(chan struct{})
	release := make(chan struct{})
	d.RegisterExclusive("reconcile", func(context.Context, map[string]any) (any, error) {
		close(entered)
		<-release
		return nil, nil
	})
	registerConsent(d)

	go d.Dispatch(context.Background(), &rpc.Request{Method: "reconcile"})
	<-entered

	resp := d.Dispatch(context.Background(), &rpc.Request{Method: consent.MethodPresence, Params: map[string]any{}})
	close(release)
	if !resp.OK {
		t.Fatalf("consent.presence while reconcile parked behind the exclusive mutex: %s", resp.Error)
	}
}
