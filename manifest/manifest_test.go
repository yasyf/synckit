package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
)

func cookiesyncManifest() Manifest {
	return Manifest{
		Name:   "cookiesync",
		Binary: "cookiesync",
		Brew:   "yasyf/tap/cookiesync",
		Watch: WatchSpec{
			Backend:  "fsnotify",
			Debounce: codec.Duration(2 * time.Second),
			ListCmd:  "cookiesync list --json",
		},
		Actions: ActionSpec{
			Reconcile: "cookiesync reconcile --origin {{.Origin}}",
			Sync:      "cookiesync sync --origin {{.Origin}}",
			Fetch:     "cookiesync state get-json",
			Apply:     "cookiesync state apply-json",
		},
		Launchd: &LaunchdSpec{SessionType: "Aqua"},
		Helper: &HelperSpec{
			Command:     "cookiesync-helper",
			SessionType: "Aqua",
			Label:       "com.yasyf.cookiesync.helper",
		},
	}
}

func reposyncManifest() Manifest {
	return Manifest{
		Name:   "reposync",
		Binary: "reposync",
		Watch: WatchSpec{
			Backend:  "watchman",
			Debounce: codec.Duration(500 * time.Millisecond),
			ListCmd:  "reposync list --json",
		},
		Actions: ActionSpec{
			Reconcile: "reposync reconcile",
			Sync:      "reposync sync",
			Fetch:     "reposync state get-json",
			Apply:     "reposync state apply-json",
		},
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   Manifest
	}{
		{"cookiesync with helper", cookiesyncManifest()},
		{"reposync without helper", reposyncManifest()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got Manifest
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(got, tt.in) {
				t.Errorf("round-trip mismatch\n got: %+v\nwant: %+v", got, tt.in)
			}
			if err := got.Validate(); err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Manifest)
		wantErr bool
	}{
		{"valid", func(*Manifest) {}, false},
		{"missing name", func(m *Manifest) { m.Name = "" }, true},
		{"missing binary", func(m *Manifest) { m.Binary = "" }, true},
		{"empty backend", func(m *Manifest) { m.Watch.Backend = "" }, true},
		{"unknown backend", func(m *Manifest) { m.Watch.Backend = "inotify" }, true},
		{"missing list_cmd", func(m *Manifest) { m.Watch.ListCmd = "" }, true},
		{"missing reconcile", func(m *Manifest) { m.Actions.Reconcile = "" }, true},
		{"missing sync", func(m *Manifest) { m.Actions.Sync = "" }, true},
		{"missing fetch", func(m *Manifest) { m.Actions.Fetch = "" }, true},
		{"missing apply", func(m *Manifest) { m.Actions.Apply = "" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := cookiesyncManifest()
			tt.mutate(&m)
			err := m.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRender(t *testing.T) {
	tests := []struct {
		name   string
		action string
		vars   ActionVars
		want   []string
	}{
		{
			name:   "sync with peer",
			action: "sync --origin {{.Peer}}",
			vars:   ActionVars{Peer: "u@h"},
			want:   []string{"sync", "--origin", "u@h"},
		},
		{
			name:   "spaces in value stay one field",
			action: "reconcile --origin {{.Origin}} --id {{.ID}}",
			vars:   ActionVars{Origin: "host1", ID: "profile-a"},
			want:   []string{"reconcile", "--origin", "host1", "--id", "profile-a"},
		},
		{
			name:   "no templating",
			action: "state get-json",
			vars:   ActionVars{},
			want:   []string{"state", "get-json"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.action, tt.vars)
			if err != nil {
				t.Fatalf("Render() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Render() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRenderMissingKey(t *testing.T) {
	if _, err := Render("sync --origin {{.Nope}}", ActionVars{}); err == nil {
		t.Fatal("Render() with unknown key = nil error, want error")
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cookiesync.json")
	data, err := json.Marshal(cookiesyncManifest())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Name != "cookiesync" {
		t.Errorf("Load().Name = %q, want cookiesync", got.Name)
	}
}

func TestLoadInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{"name":"x"}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() of incomplete manifest = nil error, want error")
	}
}

func TestDiscover(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, m Manifest) {
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	write("reposync.json", reposyncManifest())
	write("cookiesync.json", cookiesyncManifest())
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o600); err != nil {
		t.Fatalf("write txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".hidden.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write dotfile: %v", err)
	}

	got, err := Discover(dir)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Discover() len = %d, want 2", len(got))
	}
	if got[0].Name != "cookiesync" || got[1].Name != "reposync" {
		t.Errorf("Discover() order = [%q %q], want [cookiesync reposync]", got[0].Name, got[1].Name)
	}
}

func TestDiscoverMissingDir(t *testing.T) {
	got, err := Discover(filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("Discover() of missing dir error = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("Discover() of missing dir len = %d, want 0", len(got))
	}
}
