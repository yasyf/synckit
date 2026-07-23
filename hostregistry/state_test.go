package hostregistry

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDecodeEnvelopeAcceptsOnlyExactV1Contract(t *testing.T) {
	valid := fmt.Sprintf(`{"schema":{"identity":%q,"version":1,"fingerprint":%q},"host_registry":{"self":"","hosts":[],"addrs":{}},"synckit":{}}`, Mesh.State.Identity, Mesh.State.Fingerprint)
	tests := map[string]string{
		"old flat":             `{"self":"old@host","hosts":[],"addrs":{}}`,
		"missing product":      strings.Replace(valid, `,"synckit":{}`, "", 1),
		"extra top level":      strings.TrimSuffix(valid, "}") + `,"foreign":{}}`,
		"wrong identity":       strings.Replace(valid, Mesh.State.Identity, "wrong-state-v1", 1),
		"wrong version":        strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"wrong fingerprint":    strings.Replace(valid, Mesh.State.Fingerprint, strings.Repeat("0", 64), 1),
		"extra schema field":   strings.Replace(valid, `"version":1`, `"version":1,"extra":true`, 1),
		"missing host field":   strings.Replace(valid, `"self":"",`, "", 1),
		"null host collection": strings.Replace(valid, `"hosts":[]`, `"hosts":null`, 1),
		"extra product field":  strings.Replace(valid, `"synckit":{}`, `"synckit":{"extra":true}`, 1),
		"trailing value":       valid + `{}`,
	}

	if _, err := Mesh.decodeEnvelope([]byte(valid)); err != nil {
		t.Fatalf("decode exact v1 envelope: %v", err)
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Mesh.decodeEnvelope([]byte(encoded)); !errors.Is(err, ErrStateSchema) {
				t.Fatalf("decode = %v, want ErrStateSchema", err)
			}
		})
	}
}

func TestStateContractRejectsReservedProductNamespace(t *testing.T) {
	for _, namespace := range []string{"schema", "host_registry"} {
		contract := Mesh.State
		contract.ProductNamespace = namespace
		if err := (Config{State: contract}).validateContract(); !errors.Is(err, ErrStateSchema) {
			t.Fatalf("validate namespace %q = %v, want ErrStateSchema", namespace, err)
		}
	}
}

func TestDecodeExactJSONRejectsDuplicateKeysAtAnyDepth(t *testing.T) {
	for _, encoded := range []string{
		`{"schema":{},"schema":{}}`,
		`{"outer":{"value":1,"value":2}}`,
		`[{"value":1,"value":2}]`,
	} {
		var value any
		if err := DecodeExactJSON([]byte(encoded), &value); err == nil || !strings.Contains(err.Error(), "duplicate object key") {
			t.Fatalf("DecodeExactJSON(%s) = %v, want duplicate-key error", encoded, err)
		}
	}
}
