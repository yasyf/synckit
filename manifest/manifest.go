// Package manifest defines the JSON manifest a synckit consumer registers under
// ~/.config/synckit/manifests/, plus discovery, validation, and action rendering.
//
// A consumer describes how it lists watch items and how synckitd invokes its
// reconcile/sync/state actions. Action strings are whitespace-split into argv and
// each field is text/template-rendered, so argv boundaries stay fixed and a shell
// is never involved.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"

	"github.com/yasyf/synckit/codec"
)

// Manifest is a consumer's registration: its binary, watch spec, and action
// templates synckitd invokes to converge the consumer's registry.
type Manifest struct {
	Name    string       `json:"name"`
	Binary  string       `json:"binary"`
	Brew    string       `json:"brew,omitempty"`
	Watch   WatchSpec    `json:"watch"`
	Actions ActionSpec   `json:"actions"`
	Launchd *LaunchdSpec `json:"launchd,omitempty"`
	Helper  *HelperSpec  `json:"helper,omitempty"`
}

// WatchSpec configures the watch backend, debounce window, and the command that
// lists the consumer's watch items.
type WatchSpec struct {
	Backend  string         `json:"backend"`
	Debounce codec.Duration `json:"debounce"`
	ListCmd  string         `json:"list_cmd"`
}

// ActionSpec holds the four action templates synckitd renders to argv: reconcile,
// sync, fetch (read-only registry JSON), and apply (merged registry JSON on stdin).
type ActionSpec struct {
	Reconcile string `json:"reconcile"`
	Sync      string `json:"sync"`
	Fetch     string `json:"fetch"`
	Apply     string `json:"apply"`
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

// WatchItem is one unit the consumer tracks: an id, the directories to watch, and
// a fingerprint that changes when the item's contents change.
type WatchItem struct {
	ID          string   `json:"id"`
	WatchDirs   []string `json:"watch_dirs"`
	Fingerprint string   `json:"fingerprint"`
}

// ActionVars are the template variables available to action and list-command
// templates.
type ActionVars struct {
	Peer   string
	Origin string
	ID     string
}

func validBackend(b string) bool {
	return b == "fsnotify" || b == "watchman"
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
	case m.Watch.ListCmd == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "watch.list_cmd")
	case m.Actions.Reconcile == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "actions.reconcile")
	case m.Actions.Sync == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "actions.sync")
	case m.Actions.Fetch == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "actions.fetch")
	case m.Actions.Apply == "":
		return fmt.Errorf("manifest %q: field %q is required", m.Name, "actions.apply")
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

// Render splits action on whitespace into argv fields, then text/template-renders
// each field with vars, keeping argv boundaries fixed without ever building a
// shell string.
func Render(action string, vars any) ([]string, error) {
	fields := strings.Fields(action)
	argv := make([]string, len(fields))
	for i, f := range fields {
		tmpl, err := template.New("action").Option("missingkey=error").Parse(f)
		if err != nil {
			return nil, fmt.Errorf("parse action field %q: %w", f, err)
		}
		var sb strings.Builder
		if err := tmpl.Execute(&sb, vars); err != nil {
			return nil, fmt.Errorf("render action field %q: %w", f, err)
		}
		argv[i] = sb.String()
	}
	return argv, nil
}
