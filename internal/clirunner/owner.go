// Package clirunner owns Synckit's crash-recoverable CLI process scope.
package clirunner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	workerLimit = 64
	lockWait    = 30 * time.Second
)

// WithPool owns the single crash-recoverable CLI process pool for directory
// while run executes. The pool never escapes this internal ownership boundary.
func WithPool(ctx context.Context, directory string, run func(*supervise.Pool) error) (err error) {
	if ctx == nil {
		return errors.New("cli process owner: context is required")
	}
	if run == nil {
		return errors.New("cli process owner: callback is required")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == string(filepath.Separator) {
		return fmt.Errorf("cli process owner: directory %q must be absolute, clean, and non-root", directory)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create CLI process directory: %w", err)
	}
	lock, err := (proc.FileLockSpec{
		Path:     filepath.Join(directory, "cli-processes.lock"),
		Mode:     proc.FileLockExclusive,
		Deadline: lockWait,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire CLI process owner: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()

	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return fmt.Errorf("generate CLI process owner identity: %w", err)
	}
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "cli-processes.db")},
		Generation: hex.EncodeToString(generation[:]),
	}
	pool, err := supervise.NewPool(workerLimit, reaper)
	if err != nil {
		return err
	}
	defer func() {
		pool.Close()
		pool.Cancel()
		err = errors.Join(err, pool.Wait(context.WithoutCancel(ctx)))
	}()
	if err := pool.Recover(ctx); err != nil {
		return fmt.Errorf("recover CLI processes: %w", err)
	}
	if _, err := reaper.RecoverReapReceipts(ctx, proc.RecoveryTask, func(context.Context, proc.ReapReceipt) error {
		return nil
	}); err != nil {
		return fmt.Errorf("recover CLI process receipts: %w", err)
	}
	return run(pool)
}
