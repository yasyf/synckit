package consent

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/presence"
)

// Fixed presence replies: a live approver and a locked (dead) one.
const (
	livePresence = `{"on_console":true,"locked":false,"console_user":"peer","screen_shared":false}`
	deadPresence = `{"on_console":true,"locked":true,"console_user":"peer"}`
)

// approvedReply composes a peer's approved relay reply binding nonce+endpoint.
func approvedReply(t *testing.T, nonce, endpoint string) string {
	t.Helper()
	return relayReply(t, map[string]any{"status": "approved", "nonce": nonce, "endpoint": endpoint})
}

// signedReply is approvedReply plus the attestation extension fields.
func signedReply(t *testing.T, nonce, endpoint, keyID, sig, signedBy string) string {
	t.Helper()
	return relayReply(t, map[string]any{
		"status": "approved", "nonce": nonce, "endpoint": endpoint,
		"key_id": keyID, "sig": sig, "signed_by": signedBy,
	})
}

func relayReply(t *testing.T, fields map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal relay reply: %v", err)
	}
	return string(payload)
}

// runnerCall records one remote invocation an approverMesh saw.
type runnerCall struct {
	target string
	cmd    string
	stdin  string
}

// approverMesh scripts a mesh of approvers keyed by target: presence replies,
// relay replies or errors, and wedged legs that park until the ctx dies. A
// target with no scripted presence is dead; one with no scripted relay reply
// fails at the ssh transport.
type approverMesh struct {
	presence    map[string]string
	relay       map[string]string
	relayErr    map[string]error
	wedgedProbe string
	wedgedRelay string

	mu    sync.Mutex
	calls []runnerCall
}

func (r *approverMesh) Run(ctx context.Context, target, cmd string, stdin []byte) (string, error) {
	r.mu.Lock()
	r.calls = append(r.calls, runnerCall{target: target, cmd: cmd, stdin: string(stdin)})
	r.mu.Unlock()
	if strings.Contains(cmd, "presence") {
		if target == r.wedgedProbe {
			<-ctx.Done()
			return "", ctx.Err()
		}
		if reply, ok := r.presence[target]; ok {
			return reply, nil
		}
		return deadPresence, nil
	}
	if target == r.wedgedRelay {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if err, ok := r.relayErr[target]; ok {
		return "", err
	}
	if reply, ok := r.relay[target]; ok {
		return reply, nil
	}
	return "", sshTransportFailure(target)
}

// probedTargets lists the targets that received a presence probe, in order.
func (r *approverMesh) probedTargets() []string {
	return r.targetsFor("presence")
}

// relayTargets lists the targets that received a relay leg, in order.
func (r *approverMesh) relayTargets() []string {
	return r.targetsFor("relay")
}

func (r *approverMesh) targetsFor(verb string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var targets []string
	for _, c := range r.calls {
		if strings.Contains(c.cmd, verb) {
			targets = append(targets, c.target)
		}
	}
	return targets
}

// relayStdins lists the stdin payloads of every relay leg, in order.
func (r *approverMesh) relayStdins() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stdins []string
	for _, c := range r.calls {
		if strings.Contains(c.cmd, "relay") {
			stdins = append(stdins, c.stdin)
		}
	}
	return stdins
}

var (
	exit255Once sync.Once
	exit255Err  error
)

// sshTransportFailure fabricates ExecSSH's connection-failure shape: an
// *hostregistry.SSHError wrapping a real exit-255 *exec.ExitError.
func sshTransportFailure(addr string) error {
	exit255Once.Do(func() {
		exit255Err = exec.Command("/bin/sh", "-c", "exit 255").Run()
	})
	return &hostregistry.SSHError{Addr: addr, Stderr: "connect refused", Err: exit255Err}
}

// remoteExitFailure fabricates a real remote command failure: an SSHError
// wrapping an exit-1 *exec.ExitError, the shape a remote hard RPC error takes.
func remoteExitFailure(t *testing.T, addr string) error {
	t.Helper()
	exit1 := exec.Command("/bin/sh", "-c", "exit 1").Run()
	var exitErr *exec.ExitError
	if !errors.As(exit1, &exitErr) {
		t.Fatalf("fabricate exit-1: %v", exit1)
	}
	return &hostregistry.SSHError{Addr: addr, Stderr: "remote command failed", Err: exit1}
}

// pinnedRouter builds a Router over runner with a fixed nonce.
func pinnedRouter(runner Runner, nonce string) *Router {
	r := NewRouter(runner, PresenceCommand)
	r.Nonce = func() (string, error) { return nonce, nil }
	return r
}

// testAttempt is the relay composition router tests use: the relay command
// with the bare nonce as stdin.
func testAttempt(_, nonce string) (string, []byte, error) {
	return RelayCommand, []byte(nonce), nil
}

// staticProbe returns a fixed session snapshot.
func staticProbe(snap presence.SessionSnapshot) Probe {
	return func(_ context.Context) (presence.SessionSnapshot, error) { return snap, nil }
}

// attendedSession is a snapshot presence.Attended reports true for: the
// current user on console, unlocked, un-shared.
func attendedSession(t *testing.T) presence.SessionSnapshot {
	t.Helper()
	return presence.SessionSnapshot{OnConsole: true, ConsoleUser: currentUser(t)}
}

func currentUser(t *testing.T) string {
	t.Helper()
	me, err := user.Current()
	if err != nil {
		t.Fatalf("resolve current user: %v", err)
	}
	return me.Username
}

// fakePrompter scripts the local gate: it records every request and answers
// with the scripted result or error.
type fakePrompter struct {
	result PromptResult
	err    error

	mu   sync.Mutex
	reqs []Request
}

func (p *fakePrompter) Prompt(_ context.Context, req Request) (PromptResult, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, req)
	p.mu.Unlock()
	return p.result, p.err
}

func (p *fakePrompter) prompts() []Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Request(nil), p.reqs...)
}

// staticResolve resolves a fixed candidate list.
func staticResolve(peers ...string) ResolveFunc {
	return func(_ context.Context) ([]string, error) { return peers, nil }
}
