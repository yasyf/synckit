package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func testDaemonOwner(ctx context.Context, t *testing.T) (*worker.Pool, *proc.Manager) {
	t.Helper()
	owner, err := newProcessOwner(t.TempDir())
	if err != nil {
		t.Fatalf("new process owner: %v", err)
	}
	if err := owner.children.ClaimRuntime(); err != nil {
		t.Fatalf("claim children: %v", err)
	}
	if err := owner.children.Recover(ctx); err != nil {
		t.Fatalf("recover children: %v", err)
	}
	claim, err := owner.workers.ClaimRuntime()
	if err != nil {
		t.Fatalf("claim workers: %v", err)
	}
	if err := claim.Recover(ctx); err != nil {
		t.Fatalf("recover workers: %v", err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatalf("activate workers: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := owner.children.Shutdown(ctx); err != nil {
			t.Errorf("shutdown children: %v", err)
		}
		if err := claim.Close(ctx); err != nil {
			t.Errorf("close workers: %v", err)
		}
	})
	return owner.workers, owner.children
}

func testDaemonPool(ctx context.Context, t *testing.T) *worker.Pool {
	workers, _ := testDaemonOwner(ctx, t)
	return workers
}

func testDaemonChildren(ctx context.Context, t *testing.T) *proc.Manager {
	_, children := testDaemonOwner(ctx, t)
	return children
}

func TestProcessOwnerUsesDistinctDurableWorkerAndChildStores(t *testing.T) {
	directory := t.TempDir()
	owner, err := newProcessOwner(directory)
	if err != nil {
		t.Fatalf("new process owner: %v", err)
	}
	if owner.workers == nil || owner.children == nil {
		t.Fatal("process owner omitted workers or children")
	}
	if filepath.Join(directory, "process-workers.db") == filepath.Join(directory, "process-children.db") {
		t.Fatal("worker and child stores overlap")
	}
}
