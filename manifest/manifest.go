// Package manifest defines the JSON manifest a synckit consumer registers under
// ~/.config/synckit/manifests/, plus discovery and validation.
//
// A consumer describes its binary, watch debounce, and one resident or spawned
// service. Spawned services receive only the fixed rpc-serve-v1 command; remote
// service traffic enters through synckitd's fixed bridge.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/internal/serviceidentity"
)

// Manifest is a consumer's registration: its binary, watch spec, and the typed
// service block synckitd drives to converge the consumer's registry.
type Manifest struct {
	Name    string      `json:"name"`
	Binary  string      `json:"binary"`
	Brew    string      `json:"brew,omitempty"`
	Watch   WatchSpec   `json:"watch"`
	Service ServiceSpec `json:"service"`
	Helper  *HelperSpec `json:"helper,omitempty"`
}

// WatchSpec configures the watch debounce window.
type WatchSpec struct {
	Debounce codec.Duration `json:"debounce"`
}

// ServiceSpec selects one exact local presentation of the fixed v1 service.
type ServiceSpec struct {
	Kind   string `json:"kind"`
	Socket string `json:"socket,omitempty"`
}

// SessionType is a launchd session name accepted by a resident helper.
type SessionType string

const (
	// SessionTypeAqua is the graphical login session.
	SessionTypeAqua SessionType = "Aqua"
	// SessionTypeBackground is the background user session.
	SessionTypeBackground SessionType = "Background"
	// SessionTypeLoginWindow is the login-window session.
	SessionTypeLoginWindow SessionType = "LoginWindow"
	// SessionTypeStandardIO is a standard-I/O login session.
	SessionTypeStandardIO SessionType = "StandardIO"
	// SessionTypeSystem is the system session.
	SessionTypeSystem SessionType = "System"
)

// HelperSpec describes a privileged helper the consumer ships alongside its agent.
type HelperSpec struct {
	Command     string      `json:"command"`
	SessionType SessionType `json:"session_type,omitempty"`
}

func validServiceKind(kind string) bool {
	return kind == "resident" || kind == "spawned"
}

func validName(name string) bool {
	return serviceidentity.ValidateName(name) == nil
}

func validSessionType(value SessionType) bool {
	switch value {
	case "", SessionTypeAqua, SessionTypeBackground, SessionTypeLoginWindow, SessionTypeStandardIO, SessionTypeSystem:
		return true
	default:
		return false
	}
}

// Validate reports the first missing or invalid required field, naming the field.
func (m Manifest) Validate() error {
	switch {
	case !validName(m.Name):
		return fmt.Errorf("manifest: field %q must be a canonical lowercase service name, got %q", "name", m.Name)
	case m.Binary == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "binary")
	case !validServiceKind(m.Service.Kind):
		return fmt.Errorf("manifest %q: field %q must be one of resident or spawned, got %q", m.Name, "service.kind", m.Service.Kind)
	case m.Service.Kind == "resident" && m.Service.Socket == "":
		return fmt.Errorf("manifest %q: field %q is required when kind is resident", m.Name, "service.socket")
	case m.Service.Kind == "spawned" && (!filepath.IsAbs(m.Binary) || filepath.Clean(m.Binary) != m.Binary):
		return fmt.Errorf("manifest %q: field %q must be exact and absolute when kind is spawned", m.Name, "binary")
	case m.Service.Kind == "spawned" && m.Service.Socket != "":
		return fmt.Errorf("manifest %q: field %q must be empty when kind is spawned", m.Name, "service.socket")
	case m.Helper != nil && m.Helper.Command == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "helper.command")
	case m.Helper != nil && !validSessionType(m.Helper.SessionType):
		return fmt.Errorf("manifest %q: field %q has unsupported value %q", m.Name, "helper.session_type", m.Helper.SessionType)
	}
	return nil
}

// Load reads, unmarshals, and validates the manifest at path.
func Load(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is a manifest file under the fixed ~/.config/synckit/manifests dir, not user-supplied.
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	var m Manifest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest %q: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return nil, fmt.Errorf("decode manifest %q: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("manifest %q: %w", path, err)
	}
	return &m, nil
}

// Discover loads every *.json manifest in dir (ignoring dotfiles and non-.json
// entries), returning them sorted by Name. A missing dir yields an empty slice.
func Discover(dir string) ([]Manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read manifests dir %q: %w", dir, err)
	}
	var manifests []Manifest
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") || filepath.Ext(name) != ".json" {
			continue
		}
		m, err := Load(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, *m)
	}
	slices.SortFunc(manifests, func(a, b Manifest) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := 1; i < len(manifests); i++ {
		if manifests[i-1].Name == manifests[i].Name {
			return nil, fmt.Errorf("duplicate manifest name %q", manifests[i].Name)
		}
	}
	return manifests, nil
}
