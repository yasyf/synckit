package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/synckit/hostregistry"
)

const processWorkerLimit = 64

type processOwner struct {
	reaper *proc.Reaper
	pool   *supervise.Pool
}

func newProcessOwner(path string) (*processOwner, error) {
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, fmt.Errorf("generate process owner identity: %w", err)
	}
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: path},
		Generation: hex.EncodeToString(generation[:]),
	}
	pool, err := supervise.NewPool(processWorkerLimit, reaper)
	if err != nil {
		return nil, err
	}
	return &processOwner{reaper: reaper, pool: pool}, nil
}

func (o *processOwner) recover(ctx context.Context) error {
	if err := o.pool.Recover(ctx); err != nil {
		return err
	}
	_, err := o.reaper.RecoverReapReceipts(ctx, proc.RecoveryTask, func(context.Context, proc.ReapReceipt) error {
		return nil
	})
	return err
}

func (o *processOwner) Close() { o.pool.Close() }

func (o *processOwner) Cancel() { o.pool.Cancel() }

func (o *processOwner) Wait(ctx context.Context) error { return o.pool.Wait(ctx) }

func withCLIProcessOwner(ctx context.Context, run func(*supervise.Pool) error) (err error) {
	dir, err := hostregistryDir()
	if err != nil {
		return err
	}
	lock, err := (proc.FileLockSpec{
		Path: filepath.Join(dir, "cli-processes.lock"),
		Mode: proc.FileLockExclusive,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire CLI process owner: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()

	owner, err := newProcessOwner(filepath.Join(dir, "cli-processes.db"))
	if err != nil {
		return err
	}
	defer func() {
		owner.Close()
		owner.Cancel()
		err = errors.Join(err, owner.Wait(context.WithoutCancel(ctx)))
	}()
	if err := owner.recover(ctx); err != nil {
		return fmt.Errorf("recover CLI processes: %w", err)
	}
	return run(owner.pool)
}

func withCLIExecRunner(ctx context.Context, run func(hostregistry.Runner) error) error {
	return withCLIProcessOwner(ctx, func(pool *supervise.Pool) error {
		return run(hostregistry.NewExecRunner(pool))
	})
}

func hostregistryDir() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit state directory: %w", err)
	}
	return dir, nil
}
