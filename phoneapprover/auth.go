package phoneapprover

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// authScheme and authVersion mirror cc-sudo's CCSudoPhone.TransportAuth: the
// phone signs canonical(authVersion, method, id, base64(sha256(body))) with its
// transport key and sends it as "Authorization: CCSudoPhone <base64 DER>".
const (
	authScheme  = "CCSudoPhone"
	authVersion = "cc-sudo-phone-v1"
)

// canonical mirrors authkit's Subject.canonicalEncoding: each string as an
// 8-byte big-endian byte count followed by its UTF-8 bytes.
func canonical(parts ...string) []byte {
	var out []byte
	for _, part := range parts {
		out = binary.BigEndian.AppendUint64(out, uint64(len(part)))
		out = append(out, part...)
	}
	return out
}

func authMessage(method, id string, body []byte) []byte {
	sum := sha256.Sum256(body)
	return canonical(authVersion, method, id, base64.StdEncoding.EncodeToString(sum[:]))
}

// authorized reports whether header carries a transport-key signature over
// this method, request id and body.
func authorized(transportKey, header, method, id string, body []byte) bool {
	scheme, encoded, ok := strings.Cut(header, " ")
	if !ok || scheme != authScheme {
		return false
	}
	keyBytes, err := base64.StdEncoding.DecodeString(transportKey)
	if err != nil {
		return false
	}
	key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), keyBytes)
	if err != nil {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(authMessage(method, id, body))
	return ecdsa.VerifyASN1(key, digest[:], sig)
}
