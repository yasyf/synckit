package rpc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// serve starts a Serve loop on a fresh unix listener with a short socket path (well
// under the macOS sun_path limit) and returns the socket path. The listener and
// server are torn down when the test ends.
func serve(t *testing.T, d *Dispatcher) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rpcsock")
	if err != nil {
		t.Fatalf("mkdir sock dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")

	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := Serve(ctx, ln, d); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = ln.Close()
	})
	return sock
}

func TestRequestRoundTrips(t *testing.T) {
	req := &Request{Method: "sync", Params: map[string]any{"relpath": "app", "origin": "host"}}
	line, err := EncodeRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasSuffix(string(line), "\n") {
		t.Errorf("encoded request %q does not end in newline", line)
	}
	got, err := DecodeRequest(line)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Method != "sync" {
		t.Errorf("method = %q, want sync", got.Method)
	}
	if got.Params["relpath"] != "app" || got.Params["origin"] != "host" {
		t.Errorf("params = %v, want relpath=app origin=host", got.Params)
	}
}

func TestResponseAlwaysCarriesResultKey(t *testing.T) {
	line, err := EncodeResponse(&Response{OK: false, Error: "boom"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(string(line), `"result":null`) {
		t.Errorf("response %q must carry result:null even on error", line)
	}
	if strings.Contains(string(line), `"error":""`) {
		t.Errorf("response %q must omit an empty error", line)
	}
}

func TestResponseRoundTrips(t *testing.T) {
	got, err := DecodeResponse([]byte(`{"ok":true,"result":{"applied":3},"error":null}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.OK {
		t.Error("ok = false, want true")
	}
	if got.Result.(map[string]any)["applied"] != float64(3) {
		t.Errorf("result = %v, want applied=3", got.Result)
	}
	if got.Error != "" {
		t.Errorf("error = %q, want empty", got.Error)
	}
}

func TestCallRoundTripsRequestToResponse(t *testing.T) {
	d := NewDispatcher()
	d.Register("echo", func(_ context.Context, params map[string]any) (any, error) {
		return map[string]any{"saw": params["x"]}, nil
	})
	sock := serve(t, d)

	resp, err := Call(context.Background(), sock, &Request{Method: "echo", Params: map[string]any{"x": "hi"}})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ok = false, error = %q", resp.Error)
	}
	if resp.Result.(map[string]any)["saw"] != "hi" {
		t.Errorf("result = %v, want saw=hi", resp.Result)
	}
}

func TestUnknownMethodReturnsError(t *testing.T) {
	sock := serve(t, NewDispatcher())
	resp, err := Call(context.Background(), sock, &Request{Method: "bogus"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.OK {
		t.Fatal("ok = true, want error response")
	}
	if !strings.Contains(resp.Error, "unknown method") {
		t.Errorf("error = %q, want it to mention unknown method", resp.Error)
	}
}

func TestHandlerErrorBecomesErrorResponse(t *testing.T) {
	d := NewDispatcher()
	d.Register("fail", func(_ context.Context, _ map[string]any) (any, error) {
		return nil, errBoom
	})
	sock := serve(t, d)

	resp, err := Call(context.Background(), sock, &Request{Method: "fail"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.OK || resp.Error != "boom" {
		t.Errorf("got ok=%v error=%q, want ok=false error=boom", resp.OK, resp.Error)
	}
}

func TestHandlerPanicBecomesErrorResponse(t *testing.T) {
	d := NewDispatcher()
	d.Register("explode", func(_ context.Context, _ map[string]any) (any, error) {
		panic("kaboom")
	})
	sock := serve(t, d)

	resp, err := Call(context.Background(), sock, &Request{Method: "explode"})
	if err != nil {
		t.Fatalf("call (server must survive a handler panic): %v", err)
	}
	if resp.OK || !strings.Contains(resp.Error, "kaboom") {
		t.Errorf("got ok=%v error=%q, want a panic error mentioning kaboom", resp.OK, resp.Error)
	}
}

func TestDispatchIsSerialized(t *testing.T) {
	d := NewDispatcher()
	var concurrent atomic.Int32
	var peak atomic.Int32
	d.Register("slow", func(_ context.Context, _ map[string]any) (any, error) {
		n := concurrent.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		concurrent.Add(-1)
		return nil, nil
	})
	sock := serve(t, d)

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := Call(context.Background(), sock, &Request{Method: "slow"}); err != nil {
				t.Errorf("call: %v", err)
			}
		}()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Errorf("peak concurrent handlers = %d, want 1 (dispatch must serialize)", peak.Load())
	}
}

func TestCallOnMissingSocketIsTransportError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Call(ctx, filepath.Join(t.TempDir(), "absent.sock"), &Request{Method: "sync"})
	if err == nil {
		t.Fatal("want error dialing missing daemon, got nil")
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Errorf("error %v is not a *TransportError", err)
	}
}

func TestServeUnlinksStaleSocket(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "s.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatalf("seed stale socket: %v", err)
	}
	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("listen over stale socket: %v", err)
	}
	_ = ln.Close()
}

func TestServeRejectsOversizedLine(t *testing.T) {
	d := NewDispatcher()
	d.Register("echo", func(_ context.Context, _ map[string]any) (any, error) { return nil, nil })
	sock := serve(t, d)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Stream MaxLine+1 bytes with no newline; the server must reject, not buffer.
	chunk := make([]byte, 1<<16)
	for i := range chunk {
		chunk[i] = 'a'
	}
	sent := 0
	for sent <= MaxLine {
		n, werr := conn.Write(chunk)
		sent += n
		if werr != nil {
			break // server closed the connection on overflow, as intended
		}
	}
	line, err := readLine(bufio.NewReader(conn), MaxLine)
	if err != nil {
		return // connection closed without a response line is acceptable
	}
	resp, derr := DecodeResponse(line)
	if derr != nil {
		t.Fatalf("decode response %q: %v", line, derr)
	}
	if resp.OK || !strings.Contains(resp.Error, "exceeds") {
		t.Errorf("got ok=%v error=%q, want an overflow error", resp.OK, resp.Error)
	}
}

func TestPeerUIDMatchesCurrentUID(t *testing.T) {
	if testing.Short() {
		t.Skip("peer-uid check exercised via Serve")
	}
	d := NewDispatcher()
	d.Register("ping", func(_ context.Context, _ map[string]any) (any, error) { return "pong", nil })
	sock := serve(t, d)

	// The test process and the server share a UID, so the peer check admits the call.
	resp, err := Call(context.Background(), sock, &Request{Method: "ping"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !resp.OK || resp.Result != "pong" {
		t.Errorf("got ok=%v result=%v, want ok=true result=pong", resp.OK, resp.Result)
	}
}
