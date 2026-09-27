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

type manualEdit func(t *testing.T, path string)

func saveEdit(m Manual) manualEdit {
	return func(t *testing.T, path string) { saveManual(t, path, m) }
}

func TestObservedEpochCountsManualEdits(t *testing.T) {
	metered, unmetered := saveEdit(Manual{Metered: true}), saveEdit(Manual{})
	tests := []struct {
		name        string
		before      []manualEdit
		between     []manualEdit
		wantMetered bool
		wantDelta   uint64
	}{
		{"metered on", nil, []manualEdit{metered}, true, 1},
		{"metered pulse", nil, []manualEdit{metered, unmetered}, false, 1},
		{"metered pulse deleted", nil, []manualEdit{metered, removeManual}, false, 1},
		{"pulse over an unmetered file", []manualEdit{unmetered}, []manualEdit{metered, unmetered}, false, 1},
		{"pulse deleted over a deleted file", []manualEdit{unmetered, removeManual}, []manualEdit{metered, removeManual}, false, 1},
		{"unmetered rewrite may hide a pulse", []manualEdit{unmetered}, []manualEdit{unmetered}, false, 1},
		{"clearing an observed mark", []manualEdit{metered}, []manualEdit{unmetered}, false, 0},
		{"deleting an observed mark", []manualEdit{metered}, []manualEdit{removeManual}, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, path := newFakeMonitor(t, connected)
			for _, edit := range tt.before {
				edit(t, path)
			}
			start, changed := m.Current()
			began := time.Now()
			for _, edit := range tt.between {
				edit(t, path)
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
	metered := saveEdit(Manual{Metered: true})
	tests := []struct {
		name        string
		edits       []manualEdit
		wantMetered bool
	}{
		{"metered on", []manualEdit{metered}, true},
		{"metered pulse deleted", []manualEdit{metered, removeManual}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, path := newFakeMonitor(t, connected)
			if err := m.watcher.Remove(filepath.Dir(path)); err != nil {
				t.Fatalf("drop the directory watch: %v", err)
			}
			m.mu.Lock()
			start, changed := m.stateLocked(), m.changed
			for _, edit := range tt.edits {
				edit(t, path)
			}
			m.mu.Unlock()
			select {
			case <-changed:
			case <-time.After(2 * time.Second):
				t.Fatal("the sweep never published the edit")
			}
			now, _ := m.Current()
			if now.ManualMetered != tt.wantMetered || now.RestrictedEpoch != start.RestrictedEpoch+1 {
				t.Fatalf("Current() = %+v from %+v, want metered %v with the epoch advanced by 1", now, start, tt.wantMetered)
			}
		})
	}
}

func TestObservedCurrentCatchesPulseWithoutWatch(t *testing.T) {
	tests := []struct {
		name  string
		pulse []manualEdit
	}{
		{"cleared by an unmetered save", []manualEdit{saveEdit(Manual{Metered: true}), saveEdit(Manual{})}},
		{"cleared by deletion", []manualEdit{saveEdit(Manual{Metered: true}), removeManual}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, path := newFakeMonitor(t, connected)
			if err := m.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			start, changed := m.Current()
			for _, edit := range tt.pulse {
				edit(t, path)
			}
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
		})
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
