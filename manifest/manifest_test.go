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
			Debounce: codec.Duration(2 * time.Second),
		},
		Service: ServiceSpec{
			Kind:   "resident",
			Socket: "~/.config/cookiesync/rpc.sock",
		},
		Helper: &HelperSpec{
			Command:     "cookiesync-helper",
			SessionType: SessionTypeAqua,
		},
	}
}

func reposyncManifest() Manifest {
	return Manifest{
		Name:   "reposync",
		Binary: "/opt/homebrew/bin/reposync",
		Watch: WatchSpec{
			Debounce: codec.Duration(500 * time.Millisecond),
		},
		Service: ServiceSpec{Kind: "spawned"},
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
		{"spawned without socket", func(m *Manifest) {
			m.Binary = "/opt/homebrew/bin/cookiesync"
			m.Service.Kind = "spawned"
			m.Service.Socket = ""
		}, false},
		{"missing name", func(m *Manifest) { m.Name = "" }, true},
		{"unsafe name", func(m *Manifest) { m.Name = "../cookiesync" }, true},
		{"uppercase name", func(m *Manifest) { m.Name = "CookieSync" }, true},
		{"missing binary", func(m *Manifest) { m.Binary = "" }, true},
		{"missing kind", func(m *Manifest) { m.Service.Kind = "" }, true},
		{"invalid kind", func(m *Manifest) { m.Service.Kind = "http" }, true},
		{"missing socket with resident kind", func(m *Manifest) { m.Service.Socket = "" }, true},
		{"relative spawned binary", func(m *Manifest) { m.Service.Kind = "spawned"; m.Service.Socket = "" }, true},
		{"spawned socket", func(m *Manifest) { m.Binary = "/bin/cookiesync"; m.Service.Kind = "spawned" }, true},
		{"missing helper command", func(m *Manifest) { m.Helper.Command = "" }, true},
		{"invalid helper session", func(m *Manifest) { m.Helper.SessionType = SessionType("Console") }, true},
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

func TestLoadRejectsRemovedAndUnknownFields(t *testing.T) {
	tests := map[string]string{
		"removed launchd":      `{"name":"svc","binary":"svc","watch":{"debounce":"1s"},"service":{"transport":"stdio","serve_args":["serve"]},"launchd":{"session_type":"Aqua"}}`,
		"removed helper label": `{"name":"svc","binary":"svc","watch":{"debounce":"1s"},"service":{"transport":"stdio","serve_args":["serve"]},"helper":{"command":"helper","label":"helper"}}`,
		"unknown root field":   `{"name":"svc","binary":"svc","watch":{"debounce":"1s"},"service":{"transport":"stdio","serve_args":["serve"]},"unexpected":true}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatalf("write manifest: %v", err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("Load accepted a removed or unknown field")
			}
		})
	}
}

func TestLoadRejectsTrailingJSONValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	payload := `{"name":"svc","binary":"svc","watch":{"debounce":"1s"},"service":{"transport":"stdio","serve_args":["serve"]}} {}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted multiple JSON values")
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

func TestDiscoverRejectsDuplicateServiceName(t *testing.T) {
	dir := t.TempDir()
	data, err := json.Marshal(cookiesyncManifest())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, filename := range []string{"one.json", "two.json"} {
		if err := os.WriteFile(filepath.Join(dir, filename), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", filename, err)
		}
	}
	if _, err := Discover(dir); err == nil {
		t.Fatal("Discover accepted duplicate service names")
	}
}
