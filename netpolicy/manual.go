package netpolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/synckit/hostregistry"
)

const manualFileName = "netpolicy.json"

// Manual is the operator's persisted network override. Metered marks every
// network this host joins as metered, pausing bulk transfer the OS would
// otherwise allow.
type Manual struct {
	Metered bool `json:"metered"`
}

// ManualPath returns netpolicy.json under the shared synckit state directory.
func ManualPath() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit state directory: %w", err)
	}
	return filepath.Join(dir, manualFileName), nil
}

// LoadManual reads the override at path; an absent file is the zero Manual.
func LoadManual(path string) (Manual, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is synckit's own state file.
	if errors.Is(err, fs.ErrNotExist) {
		return Manual{}, nil
	}
	if err != nil {
		return Manual{}, fmt.Errorf("read manual network setting %s: %w", path, err)
	}
	var m Manual
	if err := json.Unmarshal(data, &m); err != nil {
		return Manual{}, fmt.Errorf("decode manual network setting %s: %w", path, err)
	}
	return m, nil
}

// SaveManual durably replaces the override at path, creating its directory.
func SaveManual(path string, m Manual) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create manual network setting dir: %w", err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode manual network setting: %w", err)
	}
	if err := durable.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write manual network setting %s: %w", path, err)
	}
	return nil
}

type fileStamp struct {
	exists  bool
	size    int64
	modTime int64
}

type manualSource struct {
	path   string
	mu     sync.Mutex
	stamp  fileStamp
	loaded bool
	value  bool
}

func newManualSource(path string) *manualSource {
	return &manualSource{path: path}
}

func (s *manualSource) metered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stamp fileStamp
	info, err := os.Stat(s.path)
	switch {
	case err == nil:
		stamp = fileStamp{exists: true, size: info.Size(), modTime: info.ModTime().UnixNano()}
	case !errors.Is(err, fs.ErrNotExist):
		slog.Warn("netpolicy: manual network setting unreadable; treating network as metered", "path", s.path, "err", err)
		s.loaded = false
		return true
	}
	if s.loaded && stamp == s.stamp {
		return s.value
	}
	m, err := LoadManual(s.path)
	if err != nil {
		slog.Warn("netpolicy: manual network setting unreadable; treating network as metered", "path", s.path, "err", err)
		m.Metered = true
	}
	s.stamp, s.loaded, s.value = stamp, true, m.Metered
	return s.value
}
