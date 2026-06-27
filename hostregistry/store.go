// Package hostregistry is the standalone, tool-agnostic host registry: it detects
// how peers reach this machine, runs commands locally and over ssh, discovers
// candidate hosts on the network, verifies their install, and persists the
// self/hosts identity into a shared state.json under a cross-process flock.
//
// A Config names the owning tool (Config.Name), which selects the per-tool config
// directory (~/.config/<name>), and the binary it install-probes over ssh
// (Config.Binary, "command -v <binary>" / "<binary> --version"). Identity-free
// helpers — discovery, the Runner, DetectSelf, ShellQuote — stay free functions.
//
// The registry owns only the host-identity slice of state.json (self, hosts); the
// owning tool's other keys are preserved byte-for-byte across an Update, so two
// packages can share one locked file.
package hostregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

const (
	stateFile = "state.json"
	lockFile  = "reconcile.lock"
	sockFile  = "rpc.sock"

	lockRetryDelay = 200 * time.Millisecond
)

// ErrLockBusy is returned when the reconcile lock is held past the caller's deadline.
var ErrLockBusy = errors.New("reconcile lock held by another process")

// Config names the owning tool, which selects its per-tool config directory and
// the verify/install probes. The host-registry methods that read or write
// state.json or shell a tool name hang off it.
type Config struct {
	// Name is the tool's CLI/config identity: it selects ~/.config/<Name>.
	Name string
	// Binary is the binary name probed over ssh to decide a host is bootstrapped
	// ("command -v <Binary>" / "<Binary> --version"), distinct from Name: the
	// shared mesh's config dir is named "synckit" but the installed daemon is
	// "synckitd".
	Binary string
}

// Dir returns the config directory under XDG_CONFIG_HOME or ~/.config.
func (c Config) Dir() (string, error) {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, c.Name), nil
}

// Path returns the absolute path to the state.json file.
func (c Config) Path() (string, error) {
	dir, err := c.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, stateFile), nil
}

// SockPath returns the absolute path to the daemon's RPC unix socket.
func (c Config) SockPath() (string, error) {
	dir, err := c.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, sockFile), nil
}

// WithLock runs fn while holding an exclusive flock on the reconcile lock file,
// giving up with ErrLockBusy once ctx is done so a contended acquire fails fast
// instead of blocking on a wedged holder. Every cross-package writer of
// state.json acquires this one canonical lock so writers stay serialized.
func (c Config) WithLock(ctx context.Context, fn func() error) error {
	dir, err := c.Dir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create state dir %s: %w", dir, err)
	}
	lock := flock.New(filepath.Join(dir, lockFile))
	locked, err := lock.TryLockContext(ctx, lockRetryDelay)
	if !locked {
		return fmt.Errorf("%w: %w", ErrLockBusy, err)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

// save writes raw to state.json atomically: a temp file in the state dir renamed
// over the target.
func (c Config) save(raw []byte) error {
	dir, err := c.Dir()
	if err != nil {
		return err
	}
	path, err := c.Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create state dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, "state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename state into place: %w", err)
	}
	return nil
}

// readRaw reads state.json as a key-ordered-agnostic map of raw JSON values,
// returning an empty map when the file does not yet exist.
func (c Config) readRaw() (map[string]json.RawMessage, error) {
	path, err := c.Path()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is the owning tool's own state file under the fixed config dir, not user-supplied.
	if errors.Is(err, os.ErrNotExist) {
		return map[string]json.RawMessage{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state %s: %w", path, err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	return raw, nil
}

// UpdateRaw is the one foreign-key-preserving writer of state.json: it acquires
// the shared reconcile lock, reads the whole file as raw JSON keys, runs fn to
// mutate only the keys the caller owns, then writes every key back. Keys fn
// never touches survive byte-for-byte, so two packages can share one file
// without clobbering each other's slice of it. encoding/json sorts map keys, so
// the output key order is stable across writes.
func (c Config) UpdateRaw(ctx context.Context, fn func(raw map[string]json.RawMessage) error) error {
	return c.WithLock(ctx, func() error {
		return c.UpdateRawUnlocked(fn)
	})
}

// UpdateRawUnlocked is UpdateRaw without acquiring the reconcile lock: read the
// whole file as raw JSON keys, run fn to mutate only the keys the caller owns, then
// write every key back. It is for callers that ALREADY hold the lock — an
// orchestration that wraps a whole multi-step pass in WithLock and must read and
// write state.json inside it without re-entering the (non-reentrant) flock. It
// shares save and readRaw with UpdateRaw, so there is still exactly one reader and
// one atomic writer of the file.
func (c Config) UpdateRawUnlocked(fn func(raw map[string]json.RawMessage) error) error {
	raw, err := c.readRaw()
	if err != nil {
		return err
	}
	if err := fn(raw); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	return c.save(append(encoded, '\n'))
}
