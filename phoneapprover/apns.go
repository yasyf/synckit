package phoneapprover

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// APNsEndpoint is Apple's production push host; TestFlight and App Store
// builds both register production tokens.
const APNsEndpoint = "https://api.push.apple.com"

// tokenLifetime keeps one provider token for well under Apple's hour and
// above its 20-minute floor between refreshes.
const tokenLifetime = 30 * time.Minute

// APNs sends time-sensitive alert pushes with a token-based provider key read
// from Dir: apns.json ({"team_id", "key_id", "topic"}) and AuthKey.p8, the key
// Apple issued for that one topic. Neither ever lives in a repo. It loads on
// first push, so the key can be added without restarting synckitd.
type APNs struct {
	Dir      string
	Endpoint string
	Client   *http.Client
	Now      func() time.Time

	mu      sync.Mutex
	config  *apnsConfig
	token   string
	tokenAt time.Time
}

type apnsConfig struct {
	TeamID string `json:"team_id"`
	KeyID  string `json:"key_id"`
	Topic  string `json:"topic"`
	key    *ecdsa.PrivateKey
}

// Push sends n to the device token.
func (a *APNs) Push(ctx context.Context, deviceToken string, n Notification) error {
	config, token, err := a.credentials()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"aps": map[string]any{
			"alert":              map[string]string{"title": "cc-sudo", "body": "Approval waiting on " + n.Mac},
			"interruption-level": "time-sensitive",
			"sound":              "default",
		},
		"mac": n.Mac,
		"id":  n.ID,
		"url": n.URL,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint+"/3/device/"+deviceToken, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "bearer "+token)
	req.Header.Set("apns-topic", config.Topic)
	req.Header.Set("apns-push-type", "alert")
	req.Header.Set("apns-priority", "10")
	resp, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("apns: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("apns: %s: %s", resp.Status, bytes.TrimSpace(reason))
	}
	return nil
}

func (a *APNs) credentials() (*apnsConfig, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.config == nil {
		config, err := loadAPNsConfig(a.Dir)
		if err != nil {
			return nil, "", err
		}
		a.config = config
	}
	now := a.Now()
	if a.token == "" || now.Sub(a.tokenAt) >= tokenLifetime {
		token, err := providerToken(a.config, now)
		if err != nil {
			return nil, "", err
		}
		a.token, a.tokenAt = token, now
	}
	return a.config, a.token, nil
}

func loadAPNsConfig(dir string) (*apnsConfig, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("apns config: %w", err)
	}
	defer func() { _ = root.Close() }()
	raw, err := root.ReadFile("apns.json")
	if err != nil {
		return nil, fmt.Errorf("apns config: %s: %w", filepath.Join(dir, "apns.json"), err)
	}
	var config apnsConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("apns config: %w", err)
	}
	pemBytes, err := root.ReadFile("AuthKey.p8")
	if err != nil {
		return nil, fmt.Errorf("apns key: %s: %w", filepath.Join(dir, "AuthKey.p8"), err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("apns key: %s is not PEM", filepath.Join(dir, "AuthKey.p8"))
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("apns key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("apns key: %T, want an ECDSA P-256 key", parsed)
	}
	config.key = key
	return &config, nil
}

// providerToken is Apple's ES256 JWT: {alg, kid} . {iss: team, iat}, signed
// with the raw 64-byte r‖s form JWS requires.
func providerToken(config *apnsConfig, now time.Time) (string, error) {
	segment := func(v any) (string, error) {
		raw, err := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(raw), err
	}
	header, err := segment(map[string]string{"alg": "ES256", "kid": config.KeyID})
	if err != nil {
		return "", err
	}
	claims, err := segment(map[string]any{"iss": config.TeamID, "iat": now.Unix()})
	if err != nil {
		return "", err
	}
	signing := header + "." + claims
	digest := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, config.key, digest[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
