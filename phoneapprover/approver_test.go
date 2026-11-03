package phoneapprover

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/yasyf/synckit/consent"
)

type phoneDevice struct {
	key *ecdsa.PrivateKey
}

func newPhoneDevice(t *testing.T) phoneDevice {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return phoneDevice{key: key}
}

func (d phoneDevice) transportKey(t *testing.T) string {
	t.Helper()
	raw, err := d.key.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// do plays one phone request from the push goroutine, so it reports with
// t.Errorf and a zero response rather than stopping a goroutine it does not own.
func (d phoneDevice) do(t *testing.T, method, url, id string, body []byte) *http.Response {
	t.Helper()
	failed := &http.Response{Body: http.NoBody}
	digest := sha256.Sum256(authMessage(method, id, body))
	sig, err := ecdsa.SignASN1(rand.Reader, d.key, digest[:])
	if err != nil {
		t.Errorf("sign: %v", err)
		return failed
	}
	req, err := http.NewRequest(method, url+PathPrefix+id, bytes.NewReader(body))
	if err != nil {
		t.Errorf("request: %v", err)
		return failed
	}
	req.Header.Set("Authorization", authScheme+" "+base64.StdEncoding.EncodeToString(sig))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, url, err)
		return failed
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// harness runs an Approver whose push hands the notification to phone, which
// plays the phone's side from its own goroutine; done closes when it returns.
func harness(t *testing.T, node string, phone func(n Notification)) (*Approver, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	return &Approver{
		Listen: func(context.Context) (net.Listener, string, error) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, "", err
			}
			return ln, "http://" + ln.Addr().String(), nil
		},
		WhoIs: func(context.Context, string) (string, error) { return node, nil },
		Push: func(_ context.Context, _ string, n Notification) error {
			go func() {
				defer close(done)
				phone(n)
			}()
			return nil
		},
		Window: 2 * time.Second,
	}, done
}

func enrolled(t *testing.T, device phoneDevice) consent.Phone {
	t.Helper()
	return consent.Phone{Name: "yphone", NodeID: "nPHONE", PushToken: "token", TransportKey: device.transportKey(t)}
}

var ask = consent.PhoneAsk{Origin: "yasyf-forge", Argv: []string{"/usr/bin/dscacheutil", "-flushcache"}, Nonce: "root-nonce"}

func TestThePhoneFetchesTheExactRequestAndItsApprovalComesBackSigned(t *testing.T) {
	device := newPhoneDevice(t)
	fetched := make(chan map[string]any, 1)
	a, done := harness(t, "nPHONE", func(n Notification) {
		resp := device.do(t, http.MethodGet, n.URL, n.ID, nil)
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		fetched <- got
		body, _ := json.Marshal(map[string]string{"id": n.ID, "nonce": "root-nonce", "verdict": "approved", "key_id": "kid", "sig": "sig"})
		device.do(t, http.MethodPost, n.URL, n.ID, body)
	})

	reply, err := a.Ask(context.Background(), enrolled(t, device), ask)
	<-done
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	want := consent.Reply{Status: "approved", Nonce: "root-nonce", KeyID: "kid", Sig: "sig", SignedBy: "yphone"}
	if *reply != want {
		t.Fatalf("reply = %+v, want %+v", *reply, want)
	}
	got := <-fetched
	argv, _ := got["argv"].([]any)
	if got["nonce"] != "root-nonce" || len(argv) != 2 || argv[0] != "/usr/bin/dscacheutil" || argv[1] != "-flushcache" {
		t.Fatalf("fetched %v, want the exact argv and nonce", got)
	}
}

func TestAnotherTailnetNodeReadsNothing(t *testing.T) {
	device := newPhoneDevice(t)
	status := make(chan int, 1)
	a, done := harness(t, "nOTHER", func(n Notification) {
		status <- device.do(t, http.MethodGet, n.URL, n.ID, nil).StatusCode
	})

	_, err := a.Ask(context.Background(), enrolled(t, device), ask)
	<-done
	if consent.Classify(err) != consent.VerdictUnavailable {
		t.Fatalf("Ask error = %v, want unavailable", err)
	}
	if got := <-status; got != http.StatusForbidden {
		t.Fatalf("GET from another node = %d, want 403", got)
	}
}

func TestAKeyOtherThanTheEnrolledTransportKeyReadsNothing(t *testing.T) {
	device, impostor := newPhoneDevice(t), newPhoneDevice(t)
	status := make(chan int, 1)
	a, done := harness(t, "nPHONE", func(n Notification) {
		status <- impostor.do(t, http.MethodGet, n.URL, n.ID, nil).StatusCode
	})

	_, err := a.Ask(context.Background(), enrolled(t, device), ask)
	<-done
	if consent.Classify(err) != consent.VerdictUnavailable {
		t.Fatalf("Ask error = %v, want unavailable", err)
	}
	if got := <-status; got != http.StatusUnauthorized {
		t.Fatalf("GET under another key = %d, want 401", got)
	}
}

func TestAnAnswerMustEchoTheNonceAndADenialIsFinal(t *testing.T) {
	device := newPhoneDevice(t)
	status := make(chan int, 1)
	a, done := harness(t, "nPHONE", func(n Notification) {
		stale, _ := json.Marshal(map[string]string{"id": n.ID, "nonce": "old-nonce", "verdict": "approved", "key_id": "kid", "sig": "sig"})
		status <- device.do(t, http.MethodPost, n.URL, n.ID, stale).StatusCode
		deny, _ := json.Marshal(map[string]string{"id": n.ID, "nonce": "root-nonce", "verdict": "denied"})
		device.do(t, http.MethodPost, n.URL, n.ID, deny)
	})

	_, err := a.Ask(context.Background(), enrolled(t, device), ask)
	<-done
	var denied *consent.Denied
	if !errors.As(err, &denied) || denied.Peer != "yphone" {
		t.Fatalf("Ask error = %v, want a denial from yphone", err)
	}
	if got := <-status; got != http.StatusBadRequest {
		t.Fatalf("POST with another nonce = %d, want 400", got)
	}
}

func TestAFailedPushIsUnavailable(t *testing.T) {
	device := newPhoneDevice(t)
	a, _ := harness(t, "nPHONE", nil)
	a.Push = func(context.Context, string, Notification) error { return errors.New("no apns key") }

	if _, err := a.Ask(context.Background(), enrolled(t, device), ask); consent.Classify(err) != consent.VerdictUnavailable {
		t.Fatalf("Ask error = %v, want unavailable", err)
	}
}

// TestAuthMessageMatchesTheSwiftTransportAuth pins the cross-language bytes:
// sha256(canonical("cc-sudo-phone-v1", "GET", "r1", base64(sha256(""))))).
func TestAuthMessageMatchesTheSwiftTransportAuth(t *testing.T) {
	sum := sha256.Sum256(authMessage(http.MethodGet, "r1", nil))
	if got, want := hex.EncodeToString(sum[:]), "035290e99c0bbc2406511f7550660c0cea50ba6bd638cd39ec5fda259ad3b0cf"; got != want {
		t.Fatalf("sha256(authMessage) = %s, want %s", got, want)
	}
}
