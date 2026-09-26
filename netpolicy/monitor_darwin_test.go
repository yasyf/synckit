package netpolicy

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPathMonitorReportsAndCloses(t *testing.T) {
	for i := range 3 {
		m, err := NewMonitor(filepath.Join(t.TempDir(), manualFileName))
		if err != nil {
			t.Fatalf("NewMonitor: %v", err)
		}
		got, changed := m.Current()
		if got.ObservedAt.IsZero() {
			select {
			case <-changed:
			case <-time.After(5 * time.Second):
				t.Fatal("no path update within 5s")
			}
			got, _ = m.Current()
		}
		if got.ObservedAt.IsZero() || (got.Status != StatusConnected && got.Status != StatusDisconnected) {
			t.Fatalf("Current() = %+v, want an observed connected or disconnected path", got)
		}
		t.Logf("run %d: %+v verdict-as-local=%+v", i, got, Evaluate(got, connected))
		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := m.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
}
