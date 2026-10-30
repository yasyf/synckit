package registration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/rpctest"
	"github.com/yasyf/synckit/internal/servespec"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
)

func TestRegisterManifestReloadsOverAuthenticatedRPC(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "accepted"
		if reject {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) {
			registrationHome(t)
			m := manifest.Manifest{Name: "stub", Binary: "stub", Watch: manifest.WatchSpec{Debounce: codec.Duration(10 * time.Millisecond)}, Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: strings.Repeat("a", 64)}}
			type observation struct {
				pid       int
				params    int
				installed *manifest.Manifest
				err       error
			}
			observed := make(chan observation, 1)
			dispatcher := rpc.NewDispatcher()
			dispatcher.Register("reload", func(ctx context.Context, params map[string]any) (any, error) {
				pid, _ := rpc.PeerPID(ctx)
				path, err := hostregistry.RegisteredPath(m.Name)
				var installed *manifest.Manifest
				if err == nil {
					installed, err = manifest.Load(path)
				}
				observed <- observation{pid: pid, params: len(params), installed: installed, err: err}
				if reject {
					return nil, errors.New("reload rejected")
				}
				return true, nil
			})
			server, err := rpctest.Start(t.Context(), string(servespec.Label), dispatcher)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.Close(); err != nil {
					t.Errorf("close server: %v", err)
				}
			})
			if err := RegisterManifest(t.Context(), m); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-observed:
				if got.pid != os.Getpid() || got.params != 0 || got.err != nil || !reflect.DeepEqual(got.installed, &m) {
					t.Fatalf("reload observation = %+v, want authenticated PID %d and published manifest %+v", got, os.Getpid(), m)
				}
			case <-time.After(time.Second):
				t.Fatal("registration did not issue authenticated reload")
			}
		})
	}
}

func TestRegisterManifestAtomicallyPreservesOptionsWithoutDaemon(t *testing.T) {
	registrationHome(t)
	m := manifest.Manifest{
		Name: "cci", Binary: "cci", Brew: "yasyf/tap/cci",
		Watch:   manifest.WatchSpec{Debounce: codec.Duration(17 * time.Second)},
		Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: strings.Repeat("a", 64)},
		Helper:  &manifest.HelperSpec{Command: "mesh helper", SessionType: manifest.SessionTypeAqua},
	}
	ctx := context.Background()
	if err := RegisterManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	path, err := hostregistry.RegisteredPath(m.Name)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.OpenInRoot(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = old.Close() }()
	m.Service.SchemaFingerprint = strings.Repeat("b", 64)
	if err := RegisterManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, err := manifest.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got, m) {
		t.Fatalf("registered manifest = %+v, want %+v", *got, m)
	}
	raw, err := io.ReadAll(old)
	if err != nil {
		t.Fatal(err)
	}
	var previous manifest.Manifest
	if err := json.Unmarshal(raw, &previous); err != nil {
		t.Fatal(err)
	}
	if previous.Service.SchemaFingerprint != strings.Repeat("a", 64) {
		t.Fatalf("previous open manifest changed: %+v", previous)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("registered permissions = %o", info.Mode().Perm())
	}
}

func TestRegisterManifestRejectsInvalidWithoutWriting(t *testing.T) {
	registrationHome(t)
	for _, name := range []string{"../cci", "cci"} {
		t.Run(name, func(t *testing.T) {
			if err := RegisterManifest(context.Background(), manifest.Manifest{Name: name}); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	dir, err := hostregistry.ManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("invalid registration created manifest directory: %v", err)
	}
}

func TestRegisterManifestReportsPublicationFailure(t *testing.T) {
	registrationHome(t)
	dir, err := hostregistry.ManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cci.json")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	m := manifest.Manifest{Name: "cci", Binary: "cci", Watch: manifest.WatchSpec{Debounce: codec.Duration(time.Second)}, Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: strings.Repeat("a", 64)}}
	if err := RegisterManifest(context.Background(), m); err == nil {
		t.Fatal("registration replaced a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "cci.json" || !entries[0].IsDir() {
		t.Fatalf("failed publication left entries: %v", entries)
	}
}

func TestUnregisterManifestPreservesOtherRegistrations(t *testing.T) {
	registrationHome(t)
	m := manifest.Manifest{Name: "cci", Binary: "cci", Watch: manifest.WatchSpec{Debounce: codec.Duration(time.Second)}, Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: strings.Repeat("a", 64)}}
	if err := RegisterManifest(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	other := m
	other.Name = "other"
	if err := RegisterManifest(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../cci", "cci/other"} {
		if err := UnregisterManifest(t.Context(), name); err == nil {
			t.Fatalf("invalid name %q accepted", name)
		}
	}
	path, err := hostregistry.RegisteredPath(m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := UnregisterManifest(t.Context(), m.Name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unregistered path = %v", err)
	}
	otherPath, err := hostregistry.RegisteredPath(other.Name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manifest.Load(otherPath)
	if err != nil || !reflect.DeepEqual(got, &other) {
		t.Fatalf("other registration = %+v, %v; want %+v", got, err, other)
	}
}

func registrationHome(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	base, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	t.Setenv("DAEMONKIT_HOME", base)
}
