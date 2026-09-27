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
	unchanged := func(*testing.T) {}
	steps := []struct {
		name        string
		write       func(t *testing.T)
		wantChanged bool
		wantMetered bool
	}{
		{"absent", unchanged, true, false},
		{"absent again", unchanged, false, false},
		{"metered", func(t *testing.T) { saveManual(t, path, Manual{Metered: true}) }, true, true},
		{"metered again", unchanged, false, true},
		{"unmetered", func(t *testing.T) { saveManual(t, path, Manual{}) }, true, false},
		{"unmetered rewritten", func(t *testing.T) { saveManual(t, path, Manual{}) }, true, false},
		{"corrupt fails closed", func(t *testing.T) { writeRaw(t, path, "{not json") }, true, true},
		{"corrupt again", unchanged, false, true},
		{"repaired", func(t *testing.T) { saveManual(t, path, Manual{}) }, true, false},
		{"removed", func(t *testing.T) {
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}, true, false},
	}
	for _, step := range steps {
		step.write(t)
		if changed := src.refresh(); changed != step.wantChanged || src.value != step.wantMetered {
			t.Fatalf("%s: refresh() = %v with metered %v, want %v with metered %v", step.name, changed, src.value, step.wantChanged, step.wantMetered)
		}
	}
}

func TestManualSourceUnreadableFailsClosed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads through directory permissions")
	}
	dir := filepath.Join(t.TempDir(), "locked")
	path := filepath.Join(dir, manualFileName)
	saveManual(t, path, Manual{})
	src := newManualSource(path)
	if !src.refresh() || src.value {
		t.Fatalf("readable: metered %v, want false", src.value)
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, manualDirPerm) })
	steps := []struct {
		name        string
		wantChanged bool
	}{
		{"unreadable", true},
		{"still unreadable", false},
	}
	for _, step := range steps {
		if changed := src.refresh(); changed != step.wantChanged || !src.value {
			t.Fatalf("%s: refresh() = %v with metered %v, want %v with metered true", step.name, changed, src.value, step.wantChanged)
		}
	}
	if err := os.Chmod(dir, manualDirPerm); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if !src.refresh() || src.value {
		t.Fatalf("readable again: metered %v, want false", src.value)
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
