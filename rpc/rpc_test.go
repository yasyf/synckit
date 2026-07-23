package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

var errBoom = errors.New("boom")

func serve(t *testing.T, dispatcher *Dispatcher, configure func(*Server)) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rpcsock")
	if err != nil {
		t.Fatalf("mkdir sock dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	listener, err := Listen(context.Background(), sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := NewServer(dispatcher)
	if configure != nil {
		configure(server)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return sock
}

func client(sock string) *Client {
	return NewClient(ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: WireBuild})
}

func decodeResult[T any](t *testing.T, response *Response) T {
	t.Helper()
	var result T
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return result
}

func TestPayloadRoundTripsWithoutLineFraming(t *testing.T) {
	req := &Request{Method: "sync", Params: map[string]any{"origin": "host"}}
	payload, err := EncodeRequest(req)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if strings.ContainsRune(string(payload), '\n') {
		t.Fatalf("request payload contains LF: %q", payload)
	}
	got, err := DecodeRequest(payload)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if got.Method != req.Method || got.Params["origin"] != "host" {
		t.Fatalf("request = %+v, want %+v", got, req)
	}

	responsePayload, err := EncodeResponse(&Response{OK: true, Result: json.RawMessage(`{"stamp":1719273600000000}`)})
	if err != nil {
		t.Fatalf("encode response: %v", err)
	}
	response, err := DecodeResponse(responsePayload)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if string(response.Result) != `{"stamp":1719273600000000}` {
		t.Fatalf("result = %s", response.Result)
	}
}

func TestPersistentSessionDispatchesMultipleCalls(t *testing.T) {
	var trusted atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.Register("echo", func(_ context.Context, params map[string]any) (any, error) {
		return map[string]any{"value": params["value"]}, nil
	})
	sock := serve(t, dispatcher, func(server *Server) {
		server.Wire.Trust = func(context.Context, wire.Peer) error {
			trusted.Add(1)
			return nil
		}
	})
	c := client(sock)
	defer func() { _ = c.Close() }()
	for _, value := range []string{"one", "two"} {
		response, err := c.Call(context.Background(), &Request{Method: "echo", Params: map[string]any{"value": value}})
		if err != nil {
			t.Fatalf("call %s: %v", value, err)
		}
		if got := decodeResult[map[string]string](t, response)["value"]; got != value {
			t.Fatalf("value = %q, want %q", got, value)
		}
	}
	if trusted.Load() != 1 {
		t.Fatalf("trust checks = %d, want one persistent session", trusted.Load())
	}
}

func TestUnknownMethodAndHandlerFailureAreBusinessResponses(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.Register("fail", func(context.Context, map[string]any) (any, error) { return nil, errBoom })
	sock := serve(t, dispatcher, nil)
	c := client(sock)
	defer func() { _ = c.Close() }()

	for _, method := range []string{"missing", "fail"} {
		response, err := c.Call(context.Background(), &Request{Method: method})
		if err != nil {
			t.Fatalf("%s transport: %v", method, err)
		}
		if response.OK || response.Error == "" {
			t.Fatalf("%s response = %+v, want business error", method, response)
		}
	}
}

func TestHandlerPanicDoesNotEndSession(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.Register("panic", func(context.Context, map[string]any) (any, error) { panic("kaboom") })
	dispatcher.Register("ok", func(context.Context, map[string]any) (any, error) { return true, nil })
	sock := serve(t, dispatcher, nil)
	c := client(sock)
	defer func() { _ = c.Close() }()
	response, err := c.Call(context.Background(), &Request{Method: "panic"})
	if err != nil || response.OK || !strings.Contains(response.Error, "kaboom") {
		t.Fatalf("panic response=%+v err=%v", response, err)
	}
	response, err = c.Call(context.Background(), &Request{Method: "ok"})
	if err != nil || !response.OK {
		t.Fatalf("call after panic response=%+v err=%v", response, err)
	}
}

func TestWrongWireBuildRejectsBeforeDispatch(t *testing.T) {
	if !strings.HasPrefix(WireBuild, "com.yasyf.synckit.rpc/") || !strings.HasSuffix(WireBuild, "/v1") {
		t.Fatalf("wire build = %q, want fingerprinted v1 suite", WireBuild)
	}
	var calls atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.Register("mutate", func(context.Context, map[string]any) (any, error) {
		calls.Add(1)
		return nil, nil
	})
	sock := serve(t, dispatcher, nil)
	c := NewClient(ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: "com.yasyf.synckit.rpc/wrong/v1"})
	defer func() { _ = c.Close() }()
	_, err := c.Call(context.Background(), &Request{Method: "mutate"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Outcome != wire.Rejected {
		t.Fatalf("error = %v, want rejected TransportError", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want zero", calls.Load())
	}
}

func TestTypedTrustRejectsBeforeHandshakeAndDispatch(t *testing.T) {
	var calls atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.Register("mutate", func(context.Context, map[string]any) (any, error) {
		calls.Add(1)
		return nil, nil
	})
	sock := serve(t, dispatcher, func(server *Server) {
		server.Wire.Trust = func(context.Context, wire.Peer) error { return errors.New("denied by policy") }
	})
	c := client(sock)
	defer func() { _ = c.Close() }()
	_, err := c.Call(context.Background(), &Request{Method: "mutate"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Outcome != wire.PreSendFailure {
		t.Fatalf("error = %v, want pre-send TransportError", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want zero", calls.Load())
	}
}

func TestLegacyLFRequestRejectedBeforeDispatch(t *testing.T) {
	var calls atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.Register("mutate", func(context.Context, map[string]any) (any, error) {
		calls.Add(1)
		return nil, nil
	})
	sock := serve(t, dispatcher, nil)
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(`{"method":"mutate"}` + "\n")); err != nil {
		t.Fatalf("write legacy request: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("legacy LF request received a response")
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want zero", calls.Load())
	}
}

func TestPostSendFailureIsNotReplayedAndNextCallReconnects(t *testing.T) {
	var realCalls atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.Register("mutate", func(context.Context, map[string]any) (any, error) {
		realCalls.Add(1)
		return map[string]bool{"ok": true}, nil
	})
	sock := serve(t, dispatcher, nil)
	var dials atomic.Int32
	dial := func(ctx context.Context) (net.Conn, error) {
		if dials.Add(1) == 1 {
			client, server := net.Pipe()
			go closeAfterRequest(server)
			return client, nil
		}
		return wire.UnixDialer(sock)(ctx)
	}
	c := NewClient(ClientConfig{Dial: dial, WireBuild: WireBuild})
	defer func() { _ = c.Close() }()
	_, err := c.Call(context.Background(), &Request{Method: "mutate"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Outcome != wire.PostSendFailure {
		t.Fatalf("first error = %v, want post-send failure", err)
	}
	if dials.Load() != 1 || realCalls.Load() != 0 {
		t.Fatalf("after uncertain call dials=%d real calls=%d, want 1/0", dials.Load(), realCalls.Load())
	}
	response, err := c.Call(context.Background(), &Request{Method: "mutate"})
	if err != nil || !response.OK {
		t.Fatalf("next call response=%+v err=%v", response, err)
	}
	if dials.Load() != 2 || realCalls.Load() != 1 {
		t.Fatalf("after next call dials=%d real calls=%d, want 2/1", dials.Load(), realCalls.Load())
	}
}

func closeAfterRequest(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	codec := wire.NewCodec(conn)
	hello, err := codec.ReadFrame()
	if err != nil || hello.Kind != wire.FrameHello {
		return
	}
	identity, err := json.Marshal(wire.WireIdentity{
		Protocol:  wire.ProtocolVersion,
		WireBuild: WireBuild,
		Session:   make([]byte, 16),
	})
	if err != nil {
		return
	}
	if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameHelloAck, Flags: wire.FlagEnd, Payload: identity}); err != nil {
		return
	}
	for {
		frame, err := codec.ReadFrame()
		if err != nil {
			return
		}
		if frame.Kind == wire.FrameRequest {
			return
		}
	}
}

func TestPeerIdentityComesFromAcceptedSession(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("session id is Darwin-only")
	}
	dispatcher := NewDispatcher()
	dispatcher.Register("peer", func(ctx context.Context, _ map[string]any) (any, error) {
		pid, pidOK := PeerPID(ctx)
		sid, sidOK := PeerSID(ctx)
		return map[string]any{"pid": pid, "pid_ok": pidOK, "sid": sid, "sid_ok": sidOK}, nil
	})
	sock := serve(t, dispatcher, nil)
	c := client(sock)
	defer func() { _ = c.Close() }()
	response, err := c.Call(context.Background(), &Request{Method: "peer"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	peer := decodeResult[map[string]any](t, response)
	if peer["pid_ok"] != true || int(peer["pid"].(float64)) != os.Getpid() || peer["sid_ok"] != true {
		t.Fatalf("peer = %v", peer)
	}
}

func TestCallOnMissingSocketIsPreSendTransportError(t *testing.T) {
	c := client(filepath.Join(t.TempDir(), "missing.sock"))
	defer func() { _ = c.Close() }()
	_, err := c.Call(context.Background(), &Request{Method: "x"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Outcome != wire.PreSendFailure {
		t.Fatalf("error = %v, want pre-send TransportError", err)
	}
}

func TestListenLeavesSocketOwnershipToCaller(t *testing.T) {
	dir, err := os.MkdirTemp("", "rpclisten")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "rpc.sock")
	first, err := Listen(context.Background(), sock)
	if err != nil {
		t.Fatalf("first listen: %v", err)
	}
	defer func() { _ = first.Close() }()
	second, err := Listen(context.Background(), sock)
	if err == nil {
		_ = second.Close()
		t.Fatal("second listener acquired caller-owned socket")
	}
}

func TestExclusiveDispatchSerializes(t *testing.T) {
	var active atomic.Int32
	var peak atomic.Int32
	dispatcher := NewDispatcher()
	dispatcher.RegisterExclusive("exclusive", func(context.Context, map[string]any) (any, error) {
		current := active.Add(1)
		peak.CompareAndSwap(peak.Load(), current)
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		return nil, nil
	})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if response := dispatcher.Dispatch(context.Background(), &Request{Method: "exclusive"}); !response.OK {
				t.Errorf("dispatch: %s", response.Error)
			}
		}()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatalf("peak = %d, want 1", peak.Load())
	}
}
