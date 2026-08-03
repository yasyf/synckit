package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

// testProcessScope opens one durable process-ownership scope over a fresh record
// path, settled at test end. It is the honest production value both serve and
// the reconcile CLI run against, not a stub. The scope outlives t.Context(),
// which is cancelled before cleanup runs, so its budgets are minted here.
//
//nolint:contextcheck // the settle must outlive the test context that cleanup runs after.
func testProcessScope(t *testing.T) *daemonkit.Owned {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owned, err := daemonkit.OwnProcesses(ctx, filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatalf("own processes: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := owned.Close(closeCtx); err != nil {
			t.Errorf("close process scope: %v", err)
		}
	})
	return owned
}

// shortDaemonHome points every daemonkit-derived path — socket, lock, state
// dir, owner record — at a fresh root under /tmp. Neither t.TempDir() nor the
// default TMPDIR leaves room under the 104-byte sockaddr_un limit once a synckit
// label and daemon.sock are joined onto it.
func shortDaemonHome(t *testing.T) string {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	t.Setenv("DAEMONKIT_HOME", base)
	return base
}
