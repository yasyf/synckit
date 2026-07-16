package consent

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// NewNonce mints a fresh routed-consent nonce: URL-safe base64 of 24 random
// bytes, matching the shape of the Python secrets.token_urlsafe(32) (which
// also encodes 24 bytes, since token_urlsafe's argument is the byte count, not
// the char count). A fresh nonce per attempt binds each approval to exactly
// one request.
func NewNonce() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate consent nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
