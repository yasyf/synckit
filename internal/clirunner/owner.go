// Package clirunner owns Synckit's crash-recoverable CLI process scope.
package clirunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/durable"
)

const (
	lockWait     = 30 * time.Second
	settleBudget = 30 * time.Second
	lockName     = "cli-processes.lock"
	recordName   = "cli-processes.db"
)

// WithOwned owns one crash-recoverable CLI process scope in directory while run
// executes: the scope reclaims whatever a prior generation left behind, and
// settles everything run started before returning.
func WithOwned(ctx context.Context, directory string, run func(*daemonkit.Owned) error) (err error) {
	if ctx == nil || run == nil {
		return errors.New("cli process owner: context and callback are required")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == string(filepath.Separator) {
		return fmt.Errorf("cli process owner: directory %q must be absolute, clean, and non-root", directory)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create CLI process directory: %w", err)
	}
	openCtx, cancelOpen := context.WithTimeout(ctx, lockWait)
	defer cancelOpen()
	lock, err := durable.AcquireLock(openCtx, filepath.Join(directory, lockName))
	if err != nil {
		return fmt.Errorf("acquire CLI process owner: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()

	owned, err := daemonkit.OwnProcesses(openCtx, filepath.Join(directory, recordName))
	if err != nil {
		return fmt.Errorf("own CLI processes: %w", err)
	}
	defer func(closeBase context.Context) {
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(closeBase), settleBudget)
		defer cancel()
		err = errors.Join(err, owned.Close(settleCtx))
	}(ctx)
	return run(owned)
}
