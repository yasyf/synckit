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

	"github.com/yasyf/daemonkit/wire"
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

func TestCallOnMissingSocketIsPreSendTransportError(t *testing.T) {
	client := NewClient(ClientConfig{Dial: wire.UnixDialer(filepath.Join(t.TempDir(), "missing.sock")), WireBuild: WireBuild})
	defer func() { _ = client.Close() }()
	_, err := client.Call(context.Background(), &Request{Method: "status"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Outcome != wire.PreSendFailure {
		t.Fatalf("error = %v, want pre-send TransportError", err)
	}
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
