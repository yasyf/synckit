package hostregistry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/yasyf/daemonkit/daemon"
)

// StateSchemaVersion is the only persisted state version accepted.
const StateSchemaVersion uint64 = 1

var (
	// ErrStateMissing means exact state has not been explicitly initialized.
	ErrStateMissing = errors.New("state.json is missing; initialize exact schema v1 state explicitly")
	// ErrStateSchema means state does not exactly match the configured v1 contract.
	ErrStateSchema = errors.New("state.json schema mismatch; manually recreate exact schema v1 state")
)

// StateContract defines the only whole-file schema a Config accepts.
type StateContract struct {
	Identity         string
	Fingerprint      string
	ProductNamespace string
	InitialProduct   json.RawMessage
	ValidateProduct  func(json.RawMessage) error
}

type schemaDescriptor struct {
	Identity    string `json:"identity"`
	Version     uint64 `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

type hostRegistryState struct {
	Self  string        `json:"self"`
	Hosts []SSHHostFact `json:"hosts"`
}

type stateEnvelope struct {
	Schema  schemaDescriptor
	Host    hostRegistryState
	Product json.RawMessage
}

// SchemaFingerprint returns the lowercase SHA-256 of identity, a NUL separator,
// and the exact canonical schema declaration.
func SchemaFingerprint(identity, declaration string) string {
	sum := sha256.Sum256([]byte(identity + "\x00" + declaration))
	return hex.EncodeToString(sum[:])
}

// WithStateContract returns c bound to the exact persisted-state contract.
func (c Config) WithStateContract(contract StateContract) Config {
	c.State = contract
	return c
}

// InitializeState creates a complete v1 state file when it is absent. An
// existing file must already satisfy the exact contract and is never repaired.
func (c Config) InitializeState(ctx context.Context) error {
	return c.WithLock(ctx, func() error {
		path, err := c.Path()
		if err != nil {
			return err
		}
		if _, err := os.Stat(path); err == nil {
			_, err = c.readEnvelope()
			return err
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat state %s: %w", path, err)
		}
		if err := c.validateContract(); err != nil {
			return err
		}
		env := stateEnvelope{
			Schema:  schemaDescriptor{Identity: c.State.Identity, Version: StateSchemaVersion, Fingerprint: c.State.Fingerprint},
			Host:    hostRegistryState{Hosts: []SSHHostFact{}},
			Product: slices.Clone(c.State.InitialProduct),
		}
		return c.writeEnvelope(env)
	})
}

// LoadProduct reads the validated product namespace payload.
func (c Config) LoadProduct() (json.RawMessage, error) {
	env, err := c.readEnvelope()
	if err != nil {
		return nil, err
	}
	return slices.Clone(env.Product), nil
}

// UpdateProduct replaces the validated product payload under the state lock.
func (c Config) UpdateProduct(ctx context.Context, fn func(json.RawMessage) (json.RawMessage, error)) error {
	return c.WithLock(ctx, func() error { return c.UpdateProductUnlocked(fn) })
}

// UpdateProductUnlocked is UpdateProduct for a caller already holding the lock.
func (c Config) UpdateProductUnlocked(fn func(json.RawMessage) (json.RawMessage, error)) error {
	env, err := c.readEnvelope()
	if err != nil {
		return err
	}
	next, err := fn(slices.Clone(env.Product))
	if err != nil {
		return err
	}
	if err := c.State.ValidateProduct(next); err != nil {
		return fmt.Errorf("%w: validate %s: %w", ErrStateSchema, c.State.ProductNamespace, err)
	}
	env.Product = slices.Clone(next)
	return c.writeEnvelope(env)
}

func (c Config) readEnvelope() (stateEnvelope, error) {
	path, err := c.Path()
	if err != nil {
		return stateEnvelope{}, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // fixed product state path
	if errors.Is(err, os.ErrNotExist) {
		return stateEnvelope{}, ErrStateMissing
	}
	if err != nil {
		return stateEnvelope{}, fmt.Errorf("read state %s: %w", path, err)
	}
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return stateEnvelope{}, fmt.Errorf("parse state %s: %w", path, err)
	}
	return env, nil
}

func (c Config) decodeEnvelope(data []byte) (stateEnvelope, error) {
	if err := c.validateContract(); err != nil {
		return stateEnvelope{}, err
	}
	top, err := exactObject(data, []string{"schema", "host_registry", c.State.ProductNamespace})
	if err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: envelope: %w", ErrStateSchema, err)
	}
	var schema schemaDescriptor
	if err := DecodeExactJSON(top["schema"], &schema); err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: schema: %w", ErrStateSchema, err)
	}
	if schema.Identity != c.State.Identity || schema.Version != StateSchemaVersion || schema.Fingerprint != c.State.Fingerprint {
		return stateEnvelope{}, fmt.Errorf("%w: got identity=%q version=%d fingerprint=%q", ErrStateSchema, schema.Identity, schema.Version, schema.Fingerprint)
	}
	if _, err := exactObject(top["host_registry"], []string{"self", "hosts"}); err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: host_registry: %w", ErrStateSchema, err)
	}
	var host hostRegistryState
	if err := DecodeExactJSON(top["host_registry"], &host); err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: host_registry: %w", ErrStateSchema, err)
	}
	if err := validateHostRegistry(host); err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: host_registry: %w", ErrStateSchema, err)
	}
	product := slices.Clone(top[c.State.ProductNamespace])
	if err := c.State.ValidateProduct(product); err != nil {
		return stateEnvelope{}, fmt.Errorf("%w: %s: %w", ErrStateSchema, c.State.ProductNamespace, err)
	}
	return stateEnvelope{Schema: schema, Host: host, Product: product}, nil
}

func (c Config) writeEnvelope(env stateEnvelope) error {
	if err := validateHostRegistry(env.Host); err != nil {
		return fmt.Errorf("encode host_registry: %w", err)
	}
	if err := c.State.ValidateProduct(env.Product); err != nil {
		return fmt.Errorf("encode %s: %w", c.State.ProductNamespace, err)
	}
	data, err := json.Marshal(map[string]any{
		"schema":                 env.Schema,
		"host_registry":          env.Host,
		c.State.ProductNamespace: json.RawMessage(env.Product),
	})
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	path, err := c.Path()
	if err != nil {
		return err
	}
	return daemon.WriteFileDurable(path, append(data, '\n'), 0o600)
}

func (c Config) validateContract() error {
	s := c.State
	if s.Identity == "" || s.ProductNamespace == "" || len(s.Fingerprint) != sha256.Size*2 || len(s.InitialProduct) == 0 || s.ValidateProduct == nil {
		return fmt.Errorf("%w: incomplete state contract", ErrStateSchema)
	}
	if s.ProductNamespace == "schema" || s.ProductNamespace == "host_registry" {
		return fmt.Errorf("%w: reserved product namespace %q", ErrStateSchema, s.ProductNamespace)
	}
	if _, err := hex.DecodeString(s.Fingerprint); err != nil || s.Fingerprint != string(bytes.ToLower([]byte(s.Fingerprint))) {
		return fmt.Errorf("%w: fingerprint must be lowercase SHA-256", ErrStateSchema)
	}
	if err := s.ValidateProduct(s.InitialProduct); err != nil {
		return fmt.Errorf("%w: invalid initial %s: %w", ErrStateSchema, s.ProductNamespace, err)
	}
	return nil
}

func validateHostRegistry(host hostRegistryState) error {
	if host.Hosts == nil {
		return errors.New("hosts must be present")
	}
	if host.Self != "" {
		if _, _, err := splitSSHIdentity(host.Self); err != nil {
			return fmt.Errorf("self: %w", err)
		}
	}
	seen := map[string]struct{}{}
	for i, fact := range host.Hosts {
		validated, err := NewSSHHostFact(fact.Identity, fact.SynckitdPath, fact.Addresses)
		if err != nil {
			return fmt.Errorf("hosts[%d]: %w", i, err)
		}
		if !equalSSHHostFact(validated, fact) {
			return fmt.Errorf("hosts[%d]: non-canonical host fact", i)
		}
		if _, ok := seen[fact.Identity]; ok {
			return fmt.Errorf("hosts contains duplicate %q", fact.Identity)
		}
		seen[fact.Identity] = struct{}{}
	}
	return nil
}

func exactObject(data []byte, keys []string) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := DecodeExactJSON(data, &raw); err != nil {
		return nil, err
	}
	if raw == nil || len(raw) != len(keys) {
		return nil, fmt.Errorf("keys do not exactly match %v", keys)
	}
	for _, key := range keys {
		if _, ok := raw[key]; !ok {
			return nil, fmt.Errorf("missing key %q", key)
		}
	}
	return raw, nil
}

// DecodeExactJSON decodes one JSON value, rejecting unknown struct fields,
// duplicate object keys at any depth, and trailing values.
func DecodeExactJSON(data []byte, dst any) error {
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("object is not terminated")
		}
	case '[':
		for dec.More() {
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("array is not terminated")
		}
	default:
		return errors.New("unexpected closing JSON delimiter")
	}
	return nil
}
