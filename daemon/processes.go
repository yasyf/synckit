package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/clirunner"
)

const processWorkerLimit = 64

type processOwner struct {
	workers  *worker.Pool
	children *proc.Manager
}

func newProcessOwner(directory string) (*processOwner, error) {
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return nil, fmt.Errorf("resolve process owner identity: %w", err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: processWorkerLimit, QueueCapacity: processWorkerLimit,
		MaxTotalRun: 12 * time.Minute, MaxStdinBytes: 16 << 20,
		MaxStdoutBytes: 16 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(directory, "process-workers.db")}, Generation: generation,
	})
	if err != nil {
		return nil, err
	}
	children, err := proc.NewManager(processWorkerLimit, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(directory, "process-children.db")}, Generation: generation,
	})
	if err != nil {
		return nil, err
	}
	return &processOwner{workers: workers, children: children}, nil
}

func withCLIProcessOwner(ctx context.Context, run func(*worker.Pool, *proc.Manager) error) error {
	dir, err := hostregistryDir()
	if err != nil {
		return err
	}
	return clirunner.WithRuntime(ctx, dir, run)
}

func withCLIExecRunner(ctx context.Context, run func(hostregistry.Runner) error) error {
	return hostregistry.WithExecRunner(ctx, run)
}

func hostregistryDir() (string, error) {
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit state directory: %w", err)
	}
	return dir, nil
}
