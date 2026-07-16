package authkit

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/synckit/consent"
)

func signReq() consent.Request {
	return consent.Request{
		Reason: "run a privileged command",
		Argv:   []string{"rm", "-rf", "/tmp/x"},
		Nonce:  "root-nonce",
		Origin: "you@desktop",
	}
}

func TestPromptVerdictOnlyMapsExitCodes(t *testing.T) {
	tests := []struct {
		name    string
		exit    int
		want    consent.Verdict
		wantErr bool
	}{
		{"approved", 0, consent.VerdictOK, false},
		{"denied", 1, consent.VerdictDenied, false},
		{"unavailable", 2, consent.VerdictUnavailable, false},
		{"screen-locked routes as unavailable", 3, consent.VerdictUnavailable, false},
		{"caller-rejected is fatal, never routed around", 4, consent.VerdictFatal, true},
		{"unknown exit is fatal", 9, consent.VerdictFatal, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := Gate{Bridge: Bridge{Binary: fakeHelper(t, fmt.Sprintf("exit %d", tc.exit))}}
			res, err := g.Prompt(context.Background(), consent.Request{Reason: "r"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if res.Verdict != tc.want {
				t.Fatalf("verdict = %v, want %v", res.Verdict, tc.want)
			}
			if res.Attestation != nil {
				t.Fatalf("verdict-only prompt returned attestation %+v", res.Attestation)
			}
		})
	}
}

func TestPromptVerdictOnlyPassesReasonEnvAndSubcommand(t *testing.T) {
	out := filepath.Join(t.TempDir(), "seen")
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t, `printf '%s|%s' "$1" "$AUTHKIT_REASON" > `+out)}}

	if _, err := g.Prompt(context.Background(), consent.Request{Reason: "sync them for claude"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	seen, err := os.ReadFile(out) //nolint:gosec // a temp path this test composed
	if err != nil {
		t.Fatalf("read seen: %v", err)
	}
	if want := "consent|sync them for claude"; string(seen) != want {
		t.Fatalf("helper saw %q, want %q", seen, want)
	}
}

func TestPromptSignPathFeedsArgvNonceAndProvenance(t *testing.T) {
	out := filepath.Join(t.TempDir(), "stdin")
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t,
		`[ "$1" = consent-sign ] || exit 9
cat > `+out+`
printf '{"key_id":"k1","sig":"c2ln"}'`)}}

	res, err := g.Prompt(context.Background(), signReq())
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.Verdict != consent.VerdictOK {
		t.Fatalf("verdict = %v, want OK", res.Verdict)
	}
	if res.Attestation == nil || res.Attestation.KeyID != "k1" || res.Attestation.Sig != "c2ln" {
		t.Fatalf("attestation = %+v, want key k1 sig c2ln", res.Attestation)
	}
	if res.Attestation.SignedBy != "" {
		t.Fatalf("SignedBy = %q; the engine stamps the signer, never the gate", res.Attestation.SignedBy)
	}

	payload, err := os.ReadFile(out) //nolint:gosec // a temp path this test composed
	if err != nil {
		t.Fatalf("read stdin capture: %v", err)
	}
	var sent struct {
		Nonce         string   `json:"nonce"`
		Argv          []string `json:"argv"`
		RequestedFrom string   `json:"requested_from"`
	}
	if err := json.Unmarshal(payload, &sent); err != nil {
		t.Fatalf("parse stdin capture %q: %v", payload, err)
	}
	if sent.Nonce != "root-nonce" || sent.RequestedFrom != "you@desktop" {
		t.Fatalf("stdin = %+v, want the nonce and requested_from passed through", sent)
	}
	if strings.Join(sent.Argv, " ") != "rm -rf /tmp/x" {
		t.Fatalf("argv = %v, want the exact command", sent.Argv)
	}
	if strings.Contains(string(payload), "reason") {
		t.Fatalf("consent-sign stdin %q carries a reason; the helper must display only the argv it hashes", payload)
	}
}

func TestPromptSignPathScreenLockedIsUnavailable(t *testing.T) {
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t, "exit 3")}}

	res, err := g.Prompt(context.Background(), signReq())
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.Verdict != consent.VerdictUnavailable || res.Attestation != nil {
		t.Fatalf("result = %+v, want a bare unavailable so the engine routes", res)
	}
}

func TestPromptSignPathCallerRejectedIsFatal(t *testing.T) {
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t, `printf 'caller pin failed' >&2; exit 4`)}}

	res, err := g.Prompt(context.Background(), signReq())
	if err == nil || !strings.Contains(err.Error(), "rejected the caller") {
		t.Fatalf("exit 4 = %v, want the fatal caller-rejection error", err)
	}
	if res.Verdict != consent.VerdictFatal || res.Attestation != nil {
		t.Fatalf("result = %+v, want a bare fatal verdict — never unavailable-routes-around", res)
	}
}

func TestPromptSignPathDeniedHasNoAttestation(t *testing.T) {
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t, "exit 1")}}

	res, err := g.Prompt(context.Background(), signReq())
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if res.Verdict != consent.VerdictDenied || res.Attestation != nil {
		t.Fatalf("result = %+v, want a bare denial", res)
	}
}

func TestPromptSignPathGarbageOutputIsFatal(t *testing.T) {
	g := Gate{Bridge: Bridge{Binary: fakeHelper(t, `printf 'not json'`)}}

	_, err := g.Prompt(context.Background(), signReq())
	if err == nil || !strings.Contains(err.Error(), "parse consent-sign output") {
		t.Fatalf("garbage consent-sign output = %v, want the parse failure", err)
	}
}
