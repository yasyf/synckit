package helperruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/rpc"
)

type testWorkers struct{}

func (*testWorkers) Close()                     {}
func (*testWorkers) Cancel()                    {}
func (*testWorkers) Wait(context.Context) error { return nil }

type testCloser struct{}

func (*testCloser) Close() error { return nil }

func TestConfigExposesOnlyProductOwnedInputs(t *testing.T) {
	configType := reflect.TypeFor[Config]()
	want := []string{"App", "Socket", "Server", "Workers", "State", "Resources", "Activate", "Drain"}
	if configType.NumField() != len(want) {
		t.Fatalf("Config fields = %d, want %d", configType.NumField(), len(want))
	}
	for i, name := range want {
		if got := configType.Field(i).Name; got != name {
			t.Fatalf("Config field %d = %q, want %q", i, got, name)
		}
	}
	appType := reflect.TypeFor[App]()
	if appType.NumField() != 2 || appType.Field(0).Name != "Name" || appType.Field(1).Name != "RuntimeBuild" {
		t.Fatalf("App fields = %#v", appType)
	}
}

func TestNewOwnsProtocolHealthAdmissionAndStopAuthority(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	bin := t.TempDir()
	alias := filepath.Join(bin, "synckitd")
	if err := os.Symlink(executable, alias); err != nil {
		t.Fatalf("symlink synckitd: %v", err)
	}
	t.Setenv("PATH", bin)

	server := rpc.NewServer(rpc.NewDispatcher())
	workers := &testWorkers{}
	state := &testCloser{}
	resources := &testCloser{}
	activate := func(dkdaemon.Activation) error { return nil }
	drainOwner := func() error { return nil }
	var captured wire.RuntimeConfig
	previous := newRuntime
	newRuntime = func(config wire.RuntimeConfig) (*dkdaemon.Runtime, error) {
		captured = config
		return nil, nil
	}
	t.Cleanup(func() { newRuntime = previous })

	runtime, err := New(Config{
		App:    App{Name: "cookiesync", RuntimeBuild: "v4.5.6"},
		Socket: filepath.Join(t.TempDir(), "helper.sock"), Server: server,
		Workers: workers, State: state, Resources: resources, Activate: activate, Drain: drainOwner,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if runtime != nil {
		t.Fatalf("stub runtime = %v, want nil", runtime)
	}
	if captured.RuntimeBuild != "v4.5.6" || captured.RuntimeProtocol != int(rpc.Version) || captured.Wire != server.Wire {
		t.Fatalf("wire identity = build %q protocol %d wire %p", captured.RuntimeBuild, captured.RuntimeProtocol, captured.Wire)
	}
	classifier, ok := captured.Classifier.(daemonrole.Classifier)
	if !ok || classifier.RoleID != "com.github.yasyf.synckit.stop" || classifier.RolePath != alias {
		t.Fatalf("classifier = %#v", captured.Classifier)
	}
	if captured.ReservedProtectedSessions != 1 {
		t.Fatalf("reserved protected sessions = %d", captured.ReservedProtectedSessions)
	}
	verifier, ok := captured.StopVerifier.(wire.StopVerifier)
	if !ok || verifier.Role != "com.github.yasyf.synckit.stop" {
		t.Fatalf("stop verifier = %#v", captured.StopVerifier)
	}
	store, ok := verifier.Store.(*proc.FileStore)
	wantStore := filepath.Join(root, "synckit", "service-processes.db")
	if !ok || store.Path != wantStore {
		t.Fatalf("stop store = %#v, want %q", verifier.Store, wantStore)
	}
	if _, ok := captured.Admission.(*settlingAdmission); !ok {
		t.Fatalf("admission = %T, want *settlingAdmission", captured.Admission)
	}
	if captured.Workers != workers || captured.State != state || captured.Resources != resources || reflect.ValueOf(captured.Activate).Pointer() != reflect.ValueOf(activate).Pointer() {
		t.Fatal("product-owned resources were not passed through exactly")
	}
	if len(captured.Observations) != 1 || captured.Observations[0].Op != rpc.RuntimeHealthOp || !captured.Observations[0].AvailableBeforeReady {
		t.Fatalf("runtime observations = %#v", captured.Observations)
	}
}

func TestSettlingAdmissionDrainsLiveKeepaliveBeforeWaiting(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	admission := &settlingAdmission{
		intake: &drain.Intake{},
		drain: func() error {
			if calls.Add(1) == 1 {
				close(release)
			}
			return nil
		},
	}
	done, err := admission.Admit()
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	keepaliveDone := make(chan struct{})
	go func() {
		<-release
		done()
		close(keepaliveDone)
	}()
	admission.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := admission.Settle(ctx); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	<-keepaliveDone
	if err := admission.Settle(ctx); err != nil {
		t.Fatalf("second Settle: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("drain calls = %d, want 1", calls.Load())
	}
}

func TestSettlingAdmissionPropagatesDrainErrorAfterSettlement(t *testing.T) {
	sentinel := errors.New("drain failed")
	var settled atomic.Bool
	admission := &settlingAdmission{
		intake: &drain.Intake{},
		drain: func() error {
			settled.Store(true)
			return sentinel
		},
	}
	admission.Close()
	if err := admission.Settle(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("Settle = %v, want drain error", err)
	}
	if !settled.Load() {
		t.Fatal("drain callback did not run")
	}
}

func TestNewRejectsNonCanonicalAppName(t *testing.T) {
	for _, name := range []string{"", "CookieSync", "-cookiesync", "cookiesync_"} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(Config{App: App{Name: name}}); err == nil {
				t.Fatalf("New accepted app name %q", name)
			}
		})
	}
}

var (
	_ dkdaemon.Workers   = (*testWorkers)(nil)
	_ io.Closer          = (*testCloser)(nil)
	_ dkdaemon.Resources = (*testCloser)(nil)
)
