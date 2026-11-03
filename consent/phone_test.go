package consent

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/yasyf/synckit/presence"
)

type fakePhones struct {
	answers map[string]func() (*Reply, error)
	asked   []string
	asks    []PhoneAsk
}

func (f *fakePhones) Ask(_ context.Context, phone Phone, ask PhoneAsk) (*Reply, error) {
	f.asked = append(f.asked, phone.Name)
	f.asks = append(f.asks, ask)
	return f.answers[phone.Name]()
}

func phoneReq(phones ...string) Request {
	req := decideReq(0)
	req.Approvers = []string{}
	for _, name := range phones {
		req.Phones = append(req.Phones, Phone{Name: name, NodeID: "n" + name, PushToken: "t", TransportKey: "k"})
	}
	return req
}

func unattendedEngine(t *testing.T, runner Runner, phones PhoneTransport, peers ...string) *Engine {
	t.Helper()
	e := newTestEngine(t, &fakePrompter{}, staticProbe(presence.SessionSnapshot{}), runner, peers...)
	e.Phones = phones
	return e
}

func approves(keyID, sig string) func() (*Reply, error) {
	return func() (*Reply, error) { return &Reply{Status: statusApproved, KeyID: keyID, Sig: sig}, nil }
}

// TestRouteOffersOnlyTheNamedApprovers proves argv and nonce never reach a
// mesh peer the requester did not name as an approver.
func TestRouteOffersOnlyTheNamedApprovers(t *testing.T) {
	runner := &approverMesh{presence: map[string]string{"stranger@box": livePresence}}
	e := unattendedEngine(t, runner, &fakePhones{}, "stranger@box")

	d, err := e.Decide(context.Background(), phoneReq())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictUnavailable {
		t.Fatalf("Decide = %+v, want unavailable", d)
	}
	if got := runner.probedTargets(); len(got) != 0 {
		t.Fatalf("probed %v, want no peer outside the approvers", got)
	}
}

// TestAPhoneApprovesOnceNoMacDoes proves the phone is asked with the
// requester's own nonce and argv, and its approval comes back signed by it.
func TestAPhoneApprovesOnceNoMacDoes(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){"yphone": approves("kid", "sig")}}
	e := unattendedEngine(t, &approverMesh{}, phones)

	d, err := e.Decide(context.Background(), phoneReq("yphone"))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || !d.Routed || d.ApprovedBy != "yphone" {
		t.Fatalf("Decide = %+v, want a routed approval by yphone", d)
	}
	want := Attestation{KeyID: "kid", Sig: "sig", SignedBy: "yphone"}
	if d.Attestation == nil || *d.Attestation != want {
		t.Fatalf("attestation = %+v, want %+v", d.Attestation, want)
	}
	wantAsk := PhoneAsk{Origin: "me@laptop", Argv: []string{"dscacheutil", "-flushcache"}, Nonce: "root-nonce"}
	if len(phones.asks) != 1 || phones.asks[0].Origin != wantAsk.Origin || phones.asks[0].Nonce != wantAsk.Nonce ||
		!slices.Equal(phones.asks[0].Argv, wantAsk.Argv) {
		t.Fatalf("asks = %+v, want [%+v]", phones.asks, wantAsk)
	}
}

// TestAPhoneDenialIsTerminal proves no later phone is asked past a denial.
func TestAPhoneDenialIsTerminal(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){
		"yphone": func() (*Reply, error) { return nil, &Denied{Peer: "yphone"} },
		"ipad":   approves("kid", "sig"),
	}}
	e := unattendedEngine(t, &approverMesh{}, phones)

	d, err := e.Decide(context.Background(), phoneReq("yphone", "ipad"))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictDenied {
		t.Fatalf("Decide = %+v, want denied", d)
	}
	if !slices.Equal(phones.asked, []string{"yphone"}) {
		t.Fatalf("asked %v, want only yphone", phones.asked)
	}
}

// TestAnUnansweredPhoneMovesToTheNext proves a phone that never answered
// approves nothing and the walk moves on.
func TestAnUnansweredPhoneMovesToTheNext(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){
		"yphone": func() (*Reply, error) { return nil, &AuthRequired{Msg: "no answer"} },
		"ipad":   approves("kid", "sig"),
	}}
	e := unattendedEngine(t, &approverMesh{}, phones)

	d, err := e.Decide(context.Background(), phoneReq("yphone", "ipad"))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictOK || d.ApprovedBy != "ipad" {
		t.Fatalf("Decide = %+v, want approval by ipad", d)
	}
}

// TestNoPhoneAnswerIsUnavailable proves silence from every phone is no
// approval.
func TestNoPhoneAnswerIsUnavailable(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){
		"yphone": func() (*Reply, error) { return nil, &AuthRequired{Msg: "no answer"} },
	}}
	e := unattendedEngine(t, &approverMesh{}, phones)

	d, err := e.Decide(context.Background(), phoneReq("yphone"))
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictUnavailable || d.Attestation != nil {
		t.Fatalf("Decide = %+v, want unavailable with no attestation", d)
	}
}

// TestAVerdictOnlyRequestNeverReachesAPhone proves a bare yes-or-no ask,
// whose answer would carry no signature, is never parked for a phone.
func TestAVerdictOnlyRequestNeverReachesAPhone(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){"yphone": approves("kid", "sig")}}
	e := unattendedEngine(t, &approverMesh{}, phones)
	req := phoneReq("yphone")
	req.Argv = nil
	req.Nonce = ""

	d, err := e.Decide(context.Background(), req)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if d.Verdict != VerdictUnavailable || len(phones.asked) != 0 {
		t.Fatalf("Decide = %+v asked %v, want unavailable and no phone asked", d, phones.asked)
	}
}

// TestAPhoneTransportFailureIsFatal proves a broken transport is an error,
// never a routable unavailable.
func TestAPhoneTransportFailureIsFatal(t *testing.T) {
	phones := &fakePhones{answers: map[string]func() (*Reply, error){
		"yphone": func() (*Reply, error) { return nil, errors.New("boom") },
	}}
	e := unattendedEngine(t, &approverMesh{}, phones)

	if _, err := e.Decide(context.Background(), phoneReq("yphone")); err == nil {
		t.Fatal("Decide error = nil, want the transport failure")
	}
}
