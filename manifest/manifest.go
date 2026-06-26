// Package manifest defines the JSON manifest a synckit consumer registers under
// ~/.config/synckit/manifests/, plus discovery and validation.
//
// A consumer describes its binary, watch backend, and a typed service block.
// synckitd starts the consumer's RPC server with the service's serve args and
// drives reconcile/sync/state over that typed RPC transport rather than rendering
// argv templates, so no shell or string interpolation is ever involved.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/synckit/codec"
)

// Manifest is a consumer's registration: its binary, watch spec, and the typed
// service block synckitd drives to converge the consumer's registry.
type Manifest struct {
	Name    string       `json:"name"`
	Binary  string       `json:"binary"`
	Brew    string       `json:"brew,omitempty"`
	Watch   WatchSpec    `json:"watch"`
	Service ServiceSpec  `json:"service"`
	Launchd *LaunchdSpec `json:"launchd,omitempty"`
	Helper  *HelperSpec  `json:"helper,omitempty"`
}

// WatchSpec configures the watch backend and debounce window.
type WatchSpec struct {
	Backend  string         `json:"backend"`
	Debounce codec.Duration `json:"debounce"`
}

// ServiceSpec describes how synckitd starts and reaches the consumer's RPC
// server. Transport is "socket" or "stdio", ServeArgs is the argv that starts the
// server, and Sock is the resident unix-socket path, required only when Transport
// is "socket".
type ServiceSpec struct {
	Transport string   `json:"transport"`
	ServeArgs []string `json:"serve_args"`
	Sock      string   `json:"sock,omitempty"`
}

// LaunchdSpec overrides launchd defaults for the consumer's agent.
type LaunchdSpec struct {
	SessionType string `json:"session_type,omitempty"`
}

// HelperSpec describes a privileged helper the consumer ships alongside its agent.
type HelperSpec struct {
	Command     string `json:"command"`
	SessionType string `json:"session_type,omitempty"`
	Label       string `json:"label"`
}

func validBackend(b string) bool {
	return b == "fsnotify" || b == "watchman"
}

func validTransport(t string) bool {
	return t == "socket" || t == "stdio"
}

// Validate reports the first missing or invalid required field, naming the field.
func (m Manifest) Validate() error {
	switch {
	case m.Name == "":
		return fmt.Errorf("manifest: field %q is required", "name")
	case m.Binary == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "binary")
	case !validBackend(m.Watch.Backend):
		return fmt.Errorf("manifest %q: field %q must be one of fsnotify or watchman, got %q", m.Name, "watch.backend", m.Watch.Backend)
	case !validTransport(m.Service.Transport):
		return fmt.Errorf("manifest %q: field %q must be one of socket or stdio, got %q", m.Name, "service.transport", m.Service.Transport)
	case len(m.Service.ServeArgs) == 0:
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "service.serve_args")
	case m.Service.Transport == "socket" && m.Service.Sock == "":
		return fmt.Errorf("manifest %q: field %q is required when transport is socket", m.Name, "service.sock")
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
	if err := json.Unmarshal(raw, &m); err != nil {
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
	return manifests, nil
}
