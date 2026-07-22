package daemon

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func testDaemonPool(ctx context.Context, t *testing.T) *supervise.Pool {
	t.Helper()
	owner, err := newProcessOwner(filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatalf("new process owner: %v", err)
	}
	t.Cleanup(func() {
		owner.Close()
		owner.Cancel()
		if err := owner.Wait(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("wait for process owner: %v", err)
		}
	})
	return owner.pool
}

func TestProcessOwnerRecoversAndAcknowledgesTaskReceipts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	store := &proc.FileStore{Path: path}
	stale := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           999999,
		StartTime:     "1",
		Boot:          "retired-boot",
		Comm:          "retired-task",
		Generation:    "retired-generation",
		ProcessGroup:  true,
		SessionID:     999999,
	}
	if err := store.Add(t.Context(), stale); err != nil {
		t.Fatalf("seed stale process: %v", err)
	}

	owner, err := newProcessOwner(path)
	if err != nil {
		t.Fatalf("new process owner: %v", err)
	}
	defer func() {
		owner.Close()
		owner.Cancel()
		_ = owner.Wait(context.Background())
	}()
	if err := owner.recover(t.Context()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatalf("load records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("records = %+v, want none", records)
	}
	page, err := owner.reaper.ReapReceipts(t.Context(), proc.RecoveryTask, proc.ReapReceiptCursor{}, 1)
	if err != nil {
		t.Fatalf("load receipts: %v", err)
	}
	if len(page.Receipts) != 0 || page.Floor.Sequence != 1 {
		t.Fatalf("receipt page = %+v, want acknowledged floor 1 with no pending receipts", page)
	}
}
