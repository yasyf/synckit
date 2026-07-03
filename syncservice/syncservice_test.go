package syncservice

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/rpc"
)

// stateJSON is a registry literal with a 16-digit int64 added_at stamp. A correct
// round trip preserves these bytes exactly; decoding through any would render the
// stamp as the float64 "1.7192736e+15".
const stateJSON = `{"x":{"added_at":1719273600000000,"removed_at":0,"value":{"k":"v"}}}`

// fakeConsumer is a deterministic SyncConsumer for the round-trip test. Reconcile and
// Sync stash the origin they saw so the test can assert the client propagated it.
type fakeConsumer struct {
	reconcileOrigin string
	syncOrigin      string
}

func (f *fakeConsumer) Capabilities(_ context.Context) (Capabilities, error) {
	return DefaultCapabilities("fake"), nil
}

func (f *fakeConsumer) List(_ context.Context) ([]WatchItem, error) {
	return []WatchItem{
		{ID: "alpha", WatchDirs: []string{"/a", "/b"}, Fingerprint: "fa"},
		{ID: "beta", WatchDirs: []string{"/c"}, Fingerprint: "fb"},
	}, nil
}

func (f *fakeConsumer) Reconcile(_ context.Context, origin string) (ReconcileResult, error) {
	f.reconcileOrigin = origin
	return ReconcileResult{Converged: len(origin)}, nil
}

func (f *fakeConsumer) Sync(_ context.Context, origin string) (SyncResult, error) {
	f.syncOrigin = origin
	return SyncResult{Converged: len(origin)}, nil
}

func (f *fakeConsumer) GetState(_ context.Context) (RawRegistry, error) {
	return RawRegistry(stateJSON), nil
}

// pipeTransport frames typed rpc over one end of a net.Pipe, reading responses with a
// persistent bufio.Reader so buffered bytes are not dropped between calls.
type pipeTransport struct {
	conn net.Conn
	r    *bufio.Reader
}

func (t *pipeTransport) Do(_ context.Context, req *rpc.Request) (*Response, error) {
	line, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := t.conn.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	respLine, err := rpc.ReadLine(t.r, rpc.MaxLine)
	if err != nil {
		return nil, err
	}
	return decodeEnvelope(respLine)
}

func (t *pipeTransport) Close() error { return t.conn.Close() }

func TestClientServeConnRoundTrip(t *testing.T) {
	fake := &fakeConsumer{}
	d := rpc.NewDispatcher()
	RegisterConsumer(d, fake)

	server, clientConn := net.Pipe()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- rpc.ServeConn(srvCtx, server, d) }()
	t.Cleanup(func() {
		srvCancel()
		_ = server.Close()
		<-srvDone
	})

	c := NewClient(&pipeTransport{conn: clientConn, r: bufio.NewReader(clientConn)})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.ProtocolVersion != ProtocolVersion {
		t.Errorf("protocol version = %d, want %d", caps.ProtocolVersion, ProtocolVersion)
	}
	if caps.Name != "fake" {
		t.Errorf("name = %q, want fake", caps.Name)
	}
	if strings.Join(caps.Methods, ",") != strings.Join(AllMethods, ",") {
		t.Errorf("methods = %v, want %v", caps.Methods, AllMethods)
	}

	items, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []WatchItem{
		{ID: "alpha", WatchDirs: []string{"/a", "/b"}, Fingerprint: "fa"},
		{ID: "beta", WatchDirs: []string{"/c"}, Fingerprint: "fb"},
	}
	if len(items) != len(want) {
		t.Fatalf("list len = %d, want %d", len(items), len(want))
	}
	for i, w := range want {
		got := items[i]
		if got.ID != w.ID || got.Fingerprint != w.Fingerprint || strings.Join(got.WatchDirs, ",") != strings.Join(w.WatchDirs, ",") {
			t.Errorf("item %d = %+v, want %+v", i, got, w)
		}
	}

	rec, err := c.Reconcile(ctx, "h1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fake.reconcileOrigin != "h1" {
		t.Errorf("reconcile origin = %q, want h1", fake.reconcileOrigin)
	}
	if rec.Converged != len("h1") {
		t.Errorf("reconcile converged = %d, want %d", rec.Converged, len("h1"))
	}

	syn, err := c.Sync(ctx, "h2")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if fake.syncOrigin != "h2" {
		t.Errorf("sync origin = %q, want h2", fake.syncOrigin)
	}
	if syn.Converged != len("h2") {
		t.Errorf("sync converged = %d, want %d", syn.Converged, len("h2"))
	}

	state, err := c.GetState(ctx)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if string(state) != stateJSON {
		t.Errorf("get_state = %s, want byte-identical %s", state, stateJSON)
	}
	if !strings.Contains(string(state), "1719273600000000") {
		t.Errorf("get_state %s lost the int64 stamp", state)
	}
	if strings.Contains(string(state), "1.7192736e+15") {
		t.Errorf("get_state %s corrupted the int64 stamp to float64", state)
	}
}

// erroringConsumer is a SyncConsumer whose Reconcile fails, to assert a handler error
// surfaces as a non-nil client error.
type erroringConsumer struct{ fakeConsumer }

func (*erroringConsumer) Reconcile(_ context.Context, _ string) (ReconcileResult, error) {
	return ReconcileResult{}, errBoom
}

var errBoom = boomError("boom")

type boomError string

func (e boomError) Error() string { return string(e) }

func TestHandlerErrorSurfaces(t *testing.T) {
	d := rpc.NewDispatcher()
	RegisterConsumer(d, &erroringConsumer{})

	server, clientConn := net.Pipe()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- rpc.ServeConn(srvCtx, server, d) }()
	t.Cleanup(func() {
		srvCancel()
		_ = server.Close()
		<-srvDone
	})

	c := NewClient(&pipeTransport{conn: clientConn, r: bufio.NewReader(clientConn)})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Reconcile(context.Background(), "h1")
	if err == nil {
		t.Fatal("want a non-nil error from a failing handler, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %v, want it to mention boom", err)
	}
}

func TestStdioTransportClosedRejectsDo(t *testing.T) {
	// A bogus binary so a Do that slipped past the closed guard would visibly
	// fail at spawn instead; the guard must short-circuit before start().
	tx := Stdio(filepath.Join(t.TempDir(), "never-spawned"))
	if err := tx.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := tx.Do(context.Background(), &rpc.Request{Method: MethodCapabilities})
	if err == nil {
		t.Fatal("Do after Close returned nil, want a closed error")
	}
	if !strings.Contains(err.Error(), "transport closed") {
		t.Errorf("error = %v, want it to mention transport closed", err)
	}
}

func TestStdioTransportRealSubprocess(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}

	bin := filepath.Join(t.TempDir(), "rpcstub")
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/rpcstub") //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build rpcstub: %v\n%s", err, out)
	}

	tx := Stdio(bin)
	c := NewClient(tx)
	ctx := context.Background()

	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Name != "stub" {
		t.Errorf("name = %q, want stub", caps.Name)
	}
	if caps.ProtocolVersion != ProtocolVersion {
		t.Errorf("protocol version = %d, want %d", caps.ProtocolVersion, ProtocolVersion)
	}

	items, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(items) != 1 || items[0].ID != "only" || items[0].Fingerprint != "fp" {
		t.Fatalf("list = %+v, want one item id=only fp=fp", items)
	}

	state, err := c.GetState(ctx)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !strings.Contains(string(state), "1719273600000000") {
		t.Errorf("get_state %s lost the int64 stamp byte-exact", state)
	}
	if strings.Contains(string(state), "1.7192736e+15") {
		t.Errorf("get_state %s corrupted the int64 stamp to float64", state)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities}); err == nil {
		t.Fatal("Do after Close returned nil, want a closed error")
	}
}

// TestStdioTransportCtxTimeout drives Do against a child that never answers, so ctx
// expires and reset() tears the child down while the exchange goroutine is still in
// flight; each following Do re-spawns a fresh child. Unfixed, the abandoned goroutine
// read receiver pipe fields reset() had already cleared and crashed the daemon on a
// nil dereference in the field.
func TestStdioTransportCtxTimeout(t *testing.T) {
	tx := Stdio("sleep", "9999")
	t.Cleanup(func() { _ = tx.Close() })

	for range 25 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
		}
	}
}

// TestStdioTransportResetRaceSoak is a canary for the reset()-vs-exchange race: an
// already-expired ctx plus spinner goroutines starving the scheduler delay exchange
// past reset()'s teardown, which unfixed panicked on a nil pipe field and tripped
// the race detector. The starved interleaving is probabilistic per iteration —
// deterministically green on correct code, while a reintroduced race surfaces
// within a few CI runs rather than every run.
func TestStdioTransportResetRaceSoak(t *testing.T) {
	stop := make(chan struct{})
	for range runtime.GOMAXPROCS(0) * 2 {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	defer close(stop)

	tx := Stdio("sleep", "9999")
	t.Cleanup(func() { _ = tx.Close() })

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for range 200 {
		if _, err := tx.Do(ctx, &rpc.Request{Method: MethodCapabilities}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Do = %v, want context.DeadlineExceeded", err)
		}
	}
}

func TestUnknownMethodReturnsErrorResponse(t *testing.T) {
	d := rpc.NewDispatcher()
	RegisterConsumer(d, &fakeConsumer{})

	server, clientConn := net.Pipe()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- rpc.ServeConn(srvCtx, server, d) }()
	t.Cleanup(func() {
		srvCancel()
		_ = server.Close()
		<-srvDone
	})

	tx := &pipeTransport{conn: clientConn, r: bufio.NewReader(clientConn)}
	t.Cleanup(func() { _ = tx.Close() })

	resp, err := tx.Do(context.Background(), &rpc.Request{Method: "svc.bogus"})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.OK {
		t.Fatal("ok = true, want an error response for an unknown method")
	}
	if !strings.Contains(resp.Error, "unknown method") {
		t.Errorf("error = %q, want it to mention unknown method", resp.Error)
	}
}
