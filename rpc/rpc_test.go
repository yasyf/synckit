package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

func TestPayloadRoundTripsWithoutLineFraming(t *testing.T) {
	request := &Request{Method: "sync", Params: map[string]any{"origin": "host"}}
	payload, err := EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(string(payload), '\n') {
		t.Fatalf("request payload contains LF: %q", payload)
	}
	decoded, err := DecodeRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Method != request.Method || decoded.Params["origin"] != "host" {
		t.Fatalf("request = %+v, want %+v", decoded, request)
	}
	responsePayload, err := EncodeResponse(&Response{OK: true, Result: json.RawMessage(`{"stamp":1719273600000000}`)})
	if err != nil {
		t.Fatal(err)
	}
	response, err := DecodeResponse(responsePayload)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Result) != `{"stamp":1719273600000000}` {
		t.Fatalf("result = %s", response.Result)
	}
}

func TestDispatcherBusinessFailuresRemainResponses(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.Register("panic", func(context.Context, map[string]any) (any, error) { panic("kaboom") })
	dispatcher.Register("fail", func(context.Context, map[string]any) (any, error) { return nil, errors.New("boom") })
	for _, method := range []string{"missing", "panic", "fail"} {
		response := dispatcher.Dispatch(context.Background(), &Request{Method: method})
		if response.OK || response.Error == "" {
			t.Fatalf("%s response = %+v", method, response)
		}
	}
}

func TestHandleRoutesTheCallOpThroughTheDispatcher(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatched := false
	dispatcher.Register("status", func(context.Context, map[string]any) (any, error) {
		dispatched = true
		return true, nil
	})
	payload, err := EncodeRequest(&Request{Method: "status"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := dispatcher.Handle(t.Context(), daemonkit.Request{Op: callOp, Body: payload})
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched {
		t.Fatal("dispatcher never ran")
	}
	response, err := DecodeResponse(reply.Body)
	if err != nil || !response.OK {
		t.Fatalf("response = %+v, err = %v", response, err)
	}
}

func TestHandleRefusesAnOpOutsideTheSuite(t *testing.T) {
	dispatcher := NewDispatcher()
	if _, err := dispatcher.Handle(t.Context(), daemonkit.Request{Op: "synckit.rpc.other"}); err == nil {
		t.Fatal("Handle accepted a foreign op")
	}
}

func TestHandleTurnsAnUndecodablePayloadIntoAResponse(t *testing.T) {
	dispatcher := NewDispatcher()
	reply, err := dispatcher.Handle(t.Context(), daemonkit.Request{Op: callOp, Body: []byte("{")})
	if err != nil {
		t.Fatalf("Handle failed the session on a bad payload: %v", err)
	}
	response, err := DecodeResponse(reply.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Error == "" {
		t.Fatalf("response = %+v", response)
	}
}

func TestMaxFrameCarriesAWholeMaxPayload(t *testing.T) {
	if got := daemonkit.MaxDetail(MaxFrame); got < MaxPayload {
		t.Fatalf("MaxDetail(MaxFrame) = %d, want >= %d", got, MaxPayload)
	}
	if Contract().MaxFrame != MaxFrame || SpawnLimits().MaxFrame != MaxFrame {
		t.Fatalf("contract %d and spawn limits %d disagree with MaxFrame %d",
			Contract().MaxFrame, SpawnLimits().MaxFrame, MaxFrame)
	}
}

func TestCallOnAnAbsentDaemonIsUndispatched(t *testing.T) {
	shortDaemonHome(t)
	spec := daemonkit.Daemon{
		Label:   "com.github.yasyf.synckit.rpctest.absent",
		Schemas: []daemonkit.Schema{WireBuild},
		Trust:   daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
	}
	peer, err := daemonkit.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return peer.Business(), nil },
	})
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = client.Call(ctx, &Request{Method: "status"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || !transportErr.Undispatched {
		t.Fatalf("error = %v, want an undispatched TransportError", err)
	}
	if !errors.Is(err, daemonkit.ErrAbsent) {
		t.Fatalf("error = %v, want the absence of a daemon rather than a failure to reach one", err)
	}
}

// shortDaemonHome roots every daemonkit-derived path under /tmp. A t.TempDir()
// path plus a synckit label and daemon.sock overflows the 104-byte sockaddr_un
// limit, which would fail the dial before it ever reached the socket.
func shortDaemonHome(t *testing.T) string {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	t.Setenv("DAEMONKIT_HOME", base)
	return base
}

func TestListenLeavesSocketOwnershipToCaller(t *testing.T) {
	directory, err := os.MkdirTemp("", "rpc-listen-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "rpc.sock")
	listener, err := Listen(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExclusiveDispatchSerializes(t *testing.T) {
	dispatcher := NewDispatcher()
	var mu sync.Mutex
	active := 0
	maxActive := 0
	dispatcher.RegisterExclusive("exclusive", func(context.Context, map[string]any) (any, error) {
		mu.Lock()
		active++
		maxActive = max(maxActive, active)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return true, nil
	})
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := dispatcher.Dispatch(context.Background(), &Request{Method: "exclusive"})
			if !response.OK {
				t.Errorf("response = %+v", response)
			}
		}()
	}
	wait.Wait()
	if maxActive != 1 {
		t.Fatalf("max active = %d, want 1", maxActive)
	}
}
