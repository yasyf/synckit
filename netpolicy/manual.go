package netpolicy

import (
	"crypto/rand"
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
	editMarkSuffix = ".edit"
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

// SaveManual durably replaces the override at path, creating its directory,
// then rewrites a random edit mark beside it. The mark outlives the
// override's deletion, so a Monitor that finds the override absent still
// detects a save that the deletion undid before the Monitor's next read.
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
	mark := editMarkPath(path)
	if err := durable.WriteFile(mark, []byte(rand.Text()+"\n"), 0o600); err != nil {
		return fmt.Errorf("write manual network setting edit mark %s: %w", mark, err)
	}
	return nil
}

func editMarkPath(path string) string {
	return path + editMarkSuffix
}

type editMark struct {
	token      string
	unreadable bool
}

func readEditMark(path string) (editMark, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is synckit's own state file.
	switch {
	case err == nil:
		return editMark{token: string(data)}, nil
	case errors.Is(err, fs.ErrNotExist):
		return editMark{}, nil
	}
	return editMark{unreadable: true}, fmt.Errorf("read manual network setting edit mark %s: %w", path, err)
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

func (a fileStamp) absent() bool {
	return a.info == nil && !a.unreadable
}

func (a fileStamp) same(b fileStamp) bool {
	if a.info == nil || b.info == nil {
		return a.info == nil && b.info == nil && a.unreadable == b.unreadable
	}
	return os.SameFile(a.info, b.info) && a.info.Size() == b.info.Size() && a.info.ModTime().Equal(b.info.ModTime())
}

type manualSource struct {
	path     string
	markPath string
	stamp    fileStamp
	mark     editMark
	loaded   bool
	value    bool
}

func newManualSource(path string) *manualSource {
	return &manualSource{path: path, markPath: editMarkPath(path)}
}

func (s *manualSource) refresh() bool {
	stamp, err := statStamp(s.path)
	var mark editMark
	if stamp.absent() {
		mark, err = readEditMark(s.markPath)
	}
	if s.loaded && stamp.same(s.stamp) && mark == s.mark {
		return false
	}
	s.stamp, s.mark, s.loaded = stamp, mark, true
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
