package netpolicy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManualRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", manualFileName)
	got, err := LoadManual(path)
	if err != nil {
		t.Fatalf("LoadManual(absent): %v", err)
	}
	if got != (Manual{}) {
		t.Fatalf("LoadManual(absent) = %+v, want zero", got)
	}
	for _, want := range []Manual{{Metered: true}, {Metered: false}} {
		if err := SaveManual(path, want); err != nil {
			t.Fatalf("SaveManual(%+v): %v", want, err)
		}
		got, err := LoadManual(path)
		if err != nil {
			t.Fatalf("LoadManual: %v", err)
		}
		if got != want {
			t.Fatalf("LoadManual = %+v, want %+v", got, want)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("perm = %o, want 600", perm)
	}
}

func TestManualPathUnderMeshDir(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/cfg")
	got, err := ManualPath()
	if err != nil {
		t.Fatalf("ManualPath: %v", err)
	}
	if want := "/cfg/synckit/netpolicy.json"; got != want {
		t.Errorf("ManualPath() = %q, want %q", got, want)
	}
}

func TestManualSourceTracksEdits(t *testing.T) {
	path := filepath.Join(t.TempDir(), manualFileName)
	src := newManualSource(path)
	steps := []struct {
		name  string
		write func(t *testing.T)
		want  bool
	}{
		{"absent", func(*testing.T) {}, false},
		{"metered", func(t *testing.T) { saveManual(t, path, Manual{Metered: true}) }, true},
		{"unmetered", func(t *testing.T) { saveManual(t, path, Manual{}) }, false},
		{"corrupt fails closed", func(t *testing.T) { writeRaw(t, path, "{not json") }, true},
		{"repaired", func(t *testing.T) { saveManual(t, path, Manual{}) }, false},
		{"removed", func(t *testing.T) {
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}, false},
	}
	for _, step := range steps {
		step.write(t)
		if got := src.metered(); got != step.want {
			t.Fatalf("%s: metered() = %v, want %v", step.name, got, step.want)
		}
	}
}

func saveManual(t *testing.T, path string, m Manual) {
	t.Helper()
	if err := SaveManual(path, m); err != nil {
		t.Fatalf("SaveManual: %v", err)
	}
}

func writeRaw(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}
