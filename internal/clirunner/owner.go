// Package clirunner owns Synckit's crash-recoverable CLI process scope.
package clirunner

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

const (
	workerLimit = 64
	lockWait    = 30 * time.Second
)

var workerConfig = worker.Config{
	Capacity: workerLimit, QueueCapacity: workerLimit,
	MaxTotalRun: 12 * time.Minute, MaxStdinBytes: 16 << 20,
	MaxStdoutBytes: 16 << 20, MaxStderrBytes: 1 << 20,
}

// WithPool owns one bounded disposable-command pool while run executes.
func WithPool(ctx context.Context, directory string, run func(*worker.Pool) error) error {
	return withOwner(ctx, directory, false, func(workers *worker.Pool, _ *proc.Manager) error { return run(workers) })
}

// WithRuntime owns bounded workers and long-lived children while run executes.
func WithRuntime(ctx context.Context, directory string, run func(*worker.Pool, *proc.Manager) error) error {
	return withOwner(ctx, directory, true, run)
}

func withOwner(ctx context.Context, directory string, includeChildren bool, run func(*worker.Pool, *proc.Manager) error) (err error) {
	if ctx == nil || run == nil {
		return errors.New("cli process owner: context and callback are required")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == string(filepath.Separator) {
		return fmt.Errorf("cli process owner: directory %q must be absolute, clean, and non-root", directory)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create CLI process directory: %w", err)
	}
	lock, err := (proc.FileLockSpec{
		Path: filepath.Join(directory, "cli-processes.lock"), Mode: proc.FileLockExclusive, Deadline: lockWait,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire CLI process owner: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()

	generation, err := newGeneration()
	if err != nil {
		return err
	}
	workers, err := worker.NewPool(workerConfig, reaper(filepath.Join(directory, "cli-workers.db"), generation))
	if err != nil {
		return err
	}
	claim, err := workers.ClaimRuntime(trust.VerifierWorkerBudgets())
	if err != nil {
		return err
	}
	var children *proc.Manager
	if includeChildren {
		children, err = proc.NewManager(workerLimit, reaper(filepath.Join(directory, "cli-children.db"), generation))
		if err != nil {
			return errors.Join(err, claim.Close(context.WithoutCancel(ctx)))
		}
		if err := children.ClaimRuntime(); err != nil {
			return errors.Join(err, claim.Close(context.WithoutCancel(ctx)))
		}
	}
	defer func(closeBase context.Context) {
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(closeBase), 30*time.Second)
		defer cancel()
		if children != nil {
			err = errors.Join(err, children.Shutdown(settleCtx))
		}
		err = errors.Join(err, claim.Close(settleCtx))
	}(ctx)
	if children != nil {
		if err := children.Recover(ctx); err != nil {
			return err
		}
	}
	if err := claim.Recover(ctx); err != nil {
		return err
	}
	if err := claim.Activate(); err != nil {
		return err
	}
	return run(claim.Product(), children)
}

func newGeneration() (proc.OwnerGeneration, error) {
	var value proc.OwnerGeneration
	if _, err := rand.Read(value[:]); err != nil {
		return proc.OwnerGeneration{}, fmt.Errorf("generate CLI process owner identity: %w", err)
	}
	return value, nil
}

func reaper(path string, generation proc.OwnerGeneration) *proc.Reaper {
	return &proc.Reaper{Store: &proc.FileStore{Path: path}, Generation: generation}
}
