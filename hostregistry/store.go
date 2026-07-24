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
// Every state.json has one exact schema envelope: schema identity, host registry,
// and one product namespace. Unknown or legacy shapes fail closed.
package hostregistry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

const (
	stateFile    = "state.json"
	lockFile     = "reconcile.lock"
	sockFile     = "rpc.sock"
	lockDeadline = 30 * time.Second
)

// ErrLockBusy is returned when the reconcile lock is held past the caller's deadline.
// It aliases proc.ErrLockBusy so it is one sentinel across the daemonkit boundary;
// downstream tools alias it in turn (var ErrLockBusy = hostregistry.ErrLockBusy) and
// match with errors.Is.
var ErrLockBusy = proc.ErrLockBusy

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
	// DirEnv names an environment variable that, when set to a non-empty value,
	// pins Dir to that value verbatim — an absolute directory used as-is, with no
	// Name suffix appended. It lets a per-tool env var (e.g. COOKIESYNC_CONFIG_DIR)
	// override that one tool's config dir without redirecting other tools sharing
	// the process, notably the shared hostregistry.Mesh whose own Config leaves
	// DirEnv empty. The variable is read live on each Dir call.
	DirEnv string
	// State is the exact whole-file schema contract for state.json.
	State StateContract
}

// Dir returns the config directory: the DirEnv override verbatim when that env
// var is set, otherwise XDG_CONFIG_HOME or ~/.config joined with Name.
func (c Config) Dir() (string, error) {
	if c.DirEnv != "" {
		if override := os.Getenv(c.DirEnv); override != "" {
			return override, nil
		}
	}
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

// KnownHostsPath returns the sole OpenSSH trust file for this Synckit state.
func (c Config) KnownHostsPath() (string, error) {
	dir, err := c.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "known_hosts"), nil
}

// RefreshKnownHosts snapshots OpenSSH's authenticated user trust store into
// Synckit's sole private runtime trust file.
func (c Config) RefreshKnownHosts() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve SSH home: %w", err)
	}
	source := filepath.Join(home, ".ssh", "known_hosts")
	data, err := os.ReadFile(source) //nolint:gosec // source is the current user's fixed OpenSSH trust path
	if err != nil {
		return fmt.Errorf("read SSH known_hosts: %w", err)
	}
	destination, err := c.KnownHostsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return fmt.Errorf("create Synckit state directory: %w", err)
	}
	if err := dkdaemon.WriteFileDurable(destination, data, 0o600); err != nil {
		return fmt.Errorf("write Synckit known_hosts: %w", err)
	}
	return ValidateKnownHosts(destination)
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
	lock, err := (proc.FileLockSpec{
		Path:     filepath.Join(dir, lockFile),
		Mode:     proc.FileLockExclusive,
		Deadline: lockDeadline,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLockBusy, err)
	}
	return errors.Join(fn(), lock.Close())
}
