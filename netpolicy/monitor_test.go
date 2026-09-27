package netpolicy

import (
	"path/filepath"
	"testing"
	"time"
)

func TestObservedEpochCountsRestrictedPathUpdates(t *testing.T) {
	tests := []struct {
		name      string
		updates   []State
		wantDelta uint64
	}{
		{"unrestricted updates", []State{connected, {Status: StatusConnected, ObservedAt: time.Now()}}, 0},
		{"coalesced cellular", []State{{Status: StatusConnected, Cellular: true}, connected}, 1},
		{"coalesced expensive", []State{{Status: StatusConnected, Expensive: true}, connected}, 1},
		{"coalesced constrained", []State{{Status: StatusConnected, Constrained: true}, connected}, 1},
		{"coalesced disconnected", []State{{Status: StatusDisconnected}, connected}, 1},
		{"coalesced unknown", []State{{Status: StatusUnknown}, connected}, 1},
		{"coalesced cellular then constrained", []State{{Status: StatusConnected, Cellular: true}, {Status: StatusConnected, Constrained: true}, connected}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newFakeMonitor(t, connected)
			start, changed := m.Current()
			for _, update := range tt.updates {
				m.publish(update)
			}
			select {
			case <-changed:
			default:
				t.Fatal("path updates left the start channel open")
			}
			for range 3 {
				now, _ := m.Current()
				if !now.Unrestricted() || now.RestrictedEpoch-start.RestrictedEpoch != tt.wantDelta {
					t.Fatalf("Current() = %+v, want unrestricted with epoch %d+%d", now, start.RestrictedEpoch, tt.wantDelta)
				}
			}
		})
	}
}

func TestObservedEpochCountsManualEdits(t *testing.T) {
	metered, unmetered := Manual{Metered: true}, Manual{}
	tests := []struct {
		name        string
		before      []Manual
		between     []Manual
		wantMetered bool
		wantDelta   uint64
	}{
		{"metered on", nil, []Manual{metered}, true, 1},
		{"metered pulse", nil, []Manual{metered, unmetered}, false, 1},
		{"pulse over an unmetered file", []Manual{unmetered}, []Manual{metered, unmetered}, false, 1},
		{"unmetered rewrite may hide a pulse", []Manual{unmetered}, []Manual{unmetered}, false, 1},
		{"clearing an observed mark", []Manual{metered}, []Manual{unmetered}, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, path := newFakeMonitor(t, connected)
			for _, manual := range tt.before {
				saveManual(t, path, manual)
			}
			start, changed := m.Current()
			began := time.Now()
			for _, manual := range tt.between {
				saveManual(t, path, manual)
			}
			t.Logf("edits took %v", time.Since(began))
			select {
			case <-changed:
			case <-time.After(2 * time.Second):
				t.Fatal("manual edits never closed the start channel")
			}
			now, _ := m.Current()
			if now.ManualMetered != tt.wantMetered || now.RestrictedEpoch-start.RestrictedEpoch != tt.wantDelta {
				t.Fatalf("Current() = %+v from %+v, want metered %v with epoch delta %d", now, start, tt.wantMetered, tt.wantDelta)
			}
		})
	}
}

func TestObservedSweepPublishesUnwatchedEdit(t *testing.T) {
	prev := manualSweepInterval
	manualSweepInterval = 10 * time.Millisecond
	t.Cleanup(func() { manualSweepInterval = prev })
	m, path := newFakeMonitor(t, connected)
	if err := m.watcher.Remove(filepath.Dir(path)); err != nil {
		t.Fatalf("drop the directory watch: %v", err)
	}
	start, changed := m.Current()
	saveManual(t, path, Manual{Metered: true})
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("the sweep never published the edit")
	}
	now, _ := m.Current()
	if !now.ManualMetered || now.RestrictedEpoch != start.RestrictedEpoch+1 {
		t.Fatalf("Current() = %+v from %+v, want metered with the epoch advanced by 1", now, start)
	}
}

func TestObservedCurrentCatchesPulseWithoutWatch(t *testing.T) {
	m, path := newFakeMonitor(t, connected)
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	start, changed := m.Current()
	saveManual(t, path, Manual{Metered: true})
	saveManual(t, path, Manual{})
	select {
	case <-changed:
		t.Fatal("closed watch still published the pulse")
	default:
	}
	now, _ := m.Current()
	if !now.Unrestricted() || now.RestrictedEpoch != start.RestrictedEpoch+1 {
		t.Fatalf("Current() = %+v from %+v, want unrestricted with the epoch advanced by 1", now, start)
	}
	select {
	case <-changed:
	default:
		t.Fatal("the read that caught the pulse left the start channel open")
	}
}

func TestObservedEpochStableWhileUnrestricted(t *testing.T) {
	m, path := newFakeMonitor(t, connected)
	saveManual(t, path, Manual{})
	start, _ := m.Current()
	for i := range 20 {
		m.publish(State{Status: StatusConnected, ObservedAt: time.Now()})
		if now, _ := m.Current(); now.RestrictedEpoch != start.RestrictedEpoch {
			t.Fatalf("read %d: epoch %d, want %d", i, now.RestrictedEpoch, start.RestrictedEpoch)
		}
	}
}
