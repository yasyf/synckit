package meshtrust

import (
	"os"
	"path/filepath"
	"testing"
)

func writeExecutable(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir %q: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil { //nolint:gosec // G306: exec.LookPath only accepts an executable file.
		t.Fatalf("write %q: %v", path, err)
	}
	return path
}

func TestTailscaleBinary(t *testing.T) {
	dir := t.TempDir()
	pathDir := filepath.Join(dir, "bin")
	onPath := writeExecutable(t, filepath.Join(pathDir, "tailscale"))
	appBundle := writeExecutable(t, filepath.Join(dir, "Tailscale.app", "Contents", "MacOS", "Tailscale"))
	brew := writeExecutable(t, filepath.Join(dir, "homebrew", "bin", "tailscale"))
	absent := filepath.Join(dir, "absent", "tailscale")

	empty := filepath.Join(dir, "empty")
	if err := os.MkdirAll(empty, 0o750); err != nil {
		t.Fatalf("mkdir %q: %v", empty, err)
	}

	tests := []struct {
		name      string
		path      string
		fallbacks []string
		want      string
		wantErr   bool
	}{
		{"prefers PATH over the fallbacks", pathDir, []string{appBundle, brew}, onPath, false},
		{"takes the first present fallback off PATH", empty, []string{appBundle, brew}, appBundle, false},
		{"skips a fallback that is not installed", empty, []string{absent, brew}, brew, false},
		{"errors when nothing is installed", empty, []string{absent}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PATH", tt.path)
			restore := tailscaleFallbacks
			tailscaleFallbacks = tt.fallbacks
			t.Cleanup(func() { tailscaleFallbacks = restore })

			got, err := tailscaleBinary()
			if (err != nil) != tt.wantErr {
				t.Fatalf("tailscaleBinary() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("tailscaleBinary() = %q, want %q", got, tt.want)
			}
		})
	}
}
