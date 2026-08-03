package helperruntime

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/rpctest"
	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

func TestConfigSurfaceIsExact(t *testing.T) {
	typeOf := reflect.TypeFor[Config]()
	want := []string{"App", "Program", "Dispatcher", "MaxFrame", "Prepare"}
	if typeOf.NumField() != len(want) {
		t.Fatalf("Config fields = %d, want %d", typeOf.NumField(), len(want))
	}
	for index, name := range want {
		if got := typeOf.Field(index).Name; got != name {
			t.Fatalf("Config field %d = %q, want %q", index, got, name)
		}
	}
}

func TestNewRejectsIncompleteConfiguration(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted an empty config")
	}
	prepare := func(daemonkit.Ctx) (Product, error) { return &fakeProduct{}, nil }
	if _, err := New(Config{Dispatcher: rpc.NewDispatcher(), Prepare: prepare}); err == nil {
		t.Fatal("New accepted a config naming no app")
	}
}

func TestSpecCarriesTheHelperIdentityAndAWholePayload(t *testing.T) {
	spec, err := Spec("reposync", daemonkit.Program{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	label, err := serviceidentity.HelperLabel("reposync")
	if err != nil {
		t.Fatal(err)
	}
	if string(spec.Label) != label {
		t.Fatalf("label = %q, want %q", spec.Label, label)
	}
	if spec.Restart != daemonkit.RestartAlways {
		t.Fatalf("restart = %v, want RestartAlways", spec.Restart)
	}
	if got := daemonkit.MaxDetail(spec.MaxFrame); got < rpc.MaxPayload {
		t.Fatalf("MaxDetail(spec.MaxFrame) = %d, want >= %d", got, rpc.MaxPayload)
	}
	if err := spec.ValidateForClient(); err != nil {
		t.Fatalf("validate for client: %v", err)
	}
	if err := spec.ValidateForServe(); err != nil {
		t.Fatalf("validate for serve: %v", err)
	}
}

func TestRunServesAndDrainsOneGeneration(t *testing.T) {
	t.Setenv("DAEMONKIT_HOME", shortHome(t))
	dispatcher := rpc.NewDispatcher()
	dispatcher.Register("ping", func(context.Context, map[string]any) (any, error) { return "pong", nil })
	product := &fakeProduct{}
	runtime, err := New(Config{
		App:        App{Name: "helpertest"},
		Dispatcher: dispatcher,
		Prepare:    func(daemonkit.Ctx) (Product, error) { return product, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- runtime.Run(ctx) }()
	defer cancel()

	spec, err := Spec("helpertest", daemonkit.Program{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer readyCancel()
	lane := rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	})
	defer func() { _ = lane.Close() }()
	if err := rpctest.WaitReady(readyCtx, lane); err != nil {
		t.Fatalf("wait ready: %v", err)
	}
	response, err := lane.Call(readyCtx, &rpc.Request{Method: "ping"})
	if err != nil || !response.OK {
		t.Fatalf("ping = %+v, err = %v", response, err)
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("run did not settle after cancellation")
	}
	if !product.drained || !product.closed {
		t.Fatalf("product drained=%t closed=%t", product.drained, product.closed)
	}
}

type fakeProduct struct {
	drained bool
	closed  bool
}

func (p *fakeProduct) Drain(context.Context) error {
	p.drained = true
	return nil
}

func (p *fakeProduct) Close(context.Context) error {
	p.closed = true
	return nil
}

// shortHome is a fresh state root under /tmp: neither t.TempDir() nor the
// default TMPDIR leaves room under the 104-byte sockaddr_un limit once a helper
// label and daemon.sock are joined onto it.
func shortHome(t *testing.T) string {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "hr")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	return base
}
