package phoneapprover

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeAPNsDir(t *testing.T) (string, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "AuthKey.p8"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	config := `{"team_id":"TEAM123456","key_id":"KEY1234567","topic":"com.yasyf.cc-sudo.approver"}`
	if err := os.WriteFile(filepath.Join(dir, "apns.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, key
}

func TestPushSendsATimeSensitiveAlertNamingOnlyTheMac(t *testing.T) {
	dir, key := writeAPNsDir(t)
	type seen struct {
		path    string
		header  http.Header
		payload map[string]any
	}
	got := make(chan seen, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		got <- seen{path: r.URL.Path, header: r.Header.Clone(), payload: payload}
	}))
	defer server.Close()
	now := time.Unix(1_800_000_000, 0)
	a := &APNs{Dir: dir, Endpoint: server.URL, Client: server.Client(), Now: func() time.Time { return now }}

	if err := a.Push(context.Background(), "devicetoken", Notification{Mac: "yasyf-forge", ID: "r1", URL: "https://forge.ts.net:4433"}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	s := <-got
	if s.path != "/3/device/devicetoken" {
		t.Fatalf("path = %q", s.path)
	}
	for header, want := range map[string]string{"apns-topic": "com.yasyf.cc-sudo.approver", "apns-push-type": "alert", "apns-priority": "10"} {
		if s.header.Get(header) != want {
			t.Fatalf("%s = %q, want %q", header, s.header.Get(header), want)
		}
	}
	aps, _ := s.payload["aps"].(map[string]any)
	alert, _ := aps["alert"].(map[string]any)
	if aps["interruption-level"] != "time-sensitive" || alert["body"] != "Approval waiting on yasyf-forge" {
		t.Fatalf("aps = %v", aps)
	}
	if s.payload["mac"] != "yasyf-forge" || s.payload["id"] != "r1" || s.payload["url"] != "https://forge.ts.net:4433" {
		t.Fatalf("payload = %v", s.payload)
	}
	verifyProviderToken(t, strings.TrimPrefix(s.header.Get("authorization"), "bearer "), &key.PublicKey, now)
}

func TestAPushApplesRefusalIsAnError(t *testing.T) {
	dir, _ := writeAPNsDir(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"reason":"BadDeviceToken"}`, http.StatusBadRequest)
	}))
	defer server.Close()
	a := &APNs{Dir: dir, Endpoint: server.URL, Client: server.Client(), Now: time.Now}

	err := a.Push(context.Background(), "devicetoken", Notification{Mac: "m", ID: "r", URL: "u"})
	if err == nil || !strings.Contains(err.Error(), "BadDeviceToken") {
		t.Fatalf("Push error = %v, want Apple's reason", err)
	}
}

func TestAMissingKeyIsAnErrorNamingTheFile(t *testing.T) {
	a := &APNs{Dir: t.TempDir(), Endpoint: "https://unused", Client: http.DefaultClient, Now: time.Now}
	err := a.Push(context.Background(), "t", Notification{})
	if err == nil || !strings.Contains(err.Error(), "apns.json") {
		t.Fatalf("Push error = %v, want the missing config named", err)
	}
}

func verifyProviderToken(t *testing.T, token string, key *ecdsa.PublicKey, now time.Time) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token %q is not a JWS", token)
	}
	var header, claims map[string]any
	for i, dst := range []*map[string]any{&header, &claims} {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil || json.Unmarshal(raw, dst) != nil {
			t.Fatalf("segment %d does not decode", i)
		}
	}
	if header["alg"] != "ES256" || header["kid"] != "KEY1234567" || claims["iss"] != "TEAM123456" || claims["iat"] != float64(now.Unix()) {
		t.Fatalf("header %v claims %v", header, claims)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		t.Fatalf("signature is not 64 raw bytes")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	r, s := new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])
	if !ecdsa.Verify(key, digest[:], r, s) {
		t.Fatal("provider token signature does not verify")
	}
}
