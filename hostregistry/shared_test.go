package hostregistry

import "testing"

func TestRegisteredManifestPath(t *testing.T) {
	tests := []struct {
		name          string
		xdgConfigHome string
		manifest      string
		wantDir       string
		wantPath      string
	}{
		{"xdg config home", "/xdg", "cci", "/xdg/synckit/manifests", "/xdg/synckit/manifests/cci.json"},
		{"home config", "", "cookiesync", "/home/owner/.config/synckit/manifests", "/home/owner/.config/synckit/manifests/cookiesync.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", "/home/owner")
			t.Setenv("XDG_CONFIG_HOME", tt.xdgConfigHome)
			dir, err := ManifestsDir()
			if err != nil {
				t.Fatal(err)
			}
			if dir != tt.wantDir {
				t.Errorf("ManifestsDir() = %q, want %q", dir, tt.wantDir)
			}
			path, err := RegisteredManifestPath(tt.manifest)
			if err != nil {
				t.Fatal(err)
			}
			if path != tt.wantPath {
				t.Errorf("RegisteredManifestPath(%q) = %q, want %q", tt.manifest, path, tt.wantPath)
			}
		})
	}
}

func TestRegisteredManifestPathRefusesNonBaseNames(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	for _, name := range []string{"", "nested/cci", "../cci", `nested\cci`} {
		t.Run(name, func(t *testing.T) {
			if path, err := RegisteredManifestPath(name); err == nil {
				t.Fatalf("RegisteredManifestPath(%q) = %q, want an error", name, path)
			}
		})
	}
}
