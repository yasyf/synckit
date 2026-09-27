package netpolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/synckit/hostregistry"
)

const (
	manualFileName = "netpolicy.json"
	manualDirPerm  = 0o700
)

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
	if err := os.MkdirAll(filepath.Dir(path), manualDirPerm); err != nil {
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
	info       os.FileInfo
	unreadable bool
}

func statStamp(path string) (fileStamp, error) {
	info, err := os.Stat(path)
	switch {
	case err == nil:
		return fileStamp{info: info}, nil
	case errors.Is(err, fs.ErrNotExist):
		return fileStamp{}, nil
	}
	return fileStamp{unreadable: true}, err
}

func (a fileStamp) same(b fileStamp) bool {
	if a.info == nil || b.info == nil {
		return a.info == nil && b.info == nil && a.unreadable == b.unreadable
	}
	return os.SameFile(a.info, b.info) && a.info.Size() == b.info.Size() && a.info.ModTime().Equal(b.info.ModTime())
}

type manualSource struct {
	path   string
	stamp  fileStamp
	loaded bool
	value  bool
}

func newManualSource(path string) *manualSource {
	return &manualSource{path: path}
}

func (s *manualSource) refresh() bool {
	stamp, err := statStamp(s.path)
	if s.loaded && stamp.same(s.stamp) {
		return false
	}
	s.stamp, s.loaded = stamp, true
	if err == nil {
		var m Manual
		m, err = LoadManual(s.path)
		s.value = m.Metered
	}
	if err != nil {
		slog.Warn("netpolicy: manual network setting unreadable; treating network as metered", "path", s.path, "err", err)
		s.value = true
	}
	return true
}
