package syncservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/yasyf/synckit/rpc"
)

const onceSuiteGolden = "com.yasyf.synckit.rpc-once/1047f76cf32e126fd6414fac2259180c5724012bc2ab12121298a8214a3c0072/v1"

func TestRPCOnceCommandGolden(t *testing.T) {
	if got := "synckit " + RPCOnceCommand; got != "synckit rpc-once-v1" {
		t.Fatalf("command = %q", got)
	}
}

func TestRPCOnceGoldenBytes(t *testing.T) {
	request, err := EncodeRPCOnceRequest()
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	wantRequest := `{"protocol":1,"request":{"method":"svc.get_state","params":null},"suite":"` + onceSuiteGolden + `"}`
	if got := string(request); got != wantRequest {
		t.Fatalf("request = %q, want %q", got, wantRequest)
	}

	tests := []struct {
		name     string
		response *rpc.Response
		want     string
	}{
		{
			name:     "success",
			response: &rpc.Response{OK: true, Result: json.RawMessage(`{"site":{"added_at":1719273600000000}}`)},
			want:     `{"protocol":1,"response":{"ok":true,"result":{"site":{"added_at":1719273600000000}}},"suite":"` + onceSuiteGolden + `"}`,
		},
		{
			name:     "error",
			response: &rpc.Response{Result: json.RawMessage("null"), Error: "load failed"},
			want:     `{"protocol":1,"response":{"error":"load failed","ok":false,"result":null},"suite":"` + onceSuiteGolden + `"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := EncodeRPCOnceResponse(test.response)
			if err != nil {
				t.Fatalf("encode response: %v", err)
			}
			if got := string(encoded); got != test.want {
				t.Fatalf("response = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRPCOnceRoundTrip(t *testing.T) {
	request, err := EncodeRPCOnceRequest()
	if err != nil {
		t.Fatal(err)
	}
	decodedRequest, err := DecodeRPCOnceRequest(request)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if decodedRequest.Method != MethodGetState || decodedRequest.Params != nil {
		t.Fatalf("request = %+v", decodedRequest)
	}

	want := &rpc.Response{OK: true, Result: json.RawMessage(`{"stamp":1719273600000000}`)}
	encoded, err := EncodeRPCOnceResponse(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRPCOnceResponse(encoded)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.OK != want.OK || string(got.Result) != string(want.Result) || got.Error != want.Error {
		t.Fatalf("response = %+v, want %+v", got, want)
	}
}

func TestDecodeRPCOnceRequestRejectsInexactInput(t *testing.T) {
	suite := onceSuiteGolden
	valid := `{"protocol":1,"request":{"method":"svc.get_state","params":null},"suite":"` + suite + `"}`
	tests := []struct {
		name    string
		encoded string
		want    string
	}{
		{"unknown outer field", strings.TrimSuffix(valid, "}") + `,"x":1}`, "unknown field"},
		{"unknown request field", `{"protocol":1,"request":{"method":"svc.get_state","params":null,"x":1},"suite":"` + suite + `"}`, "unknown field"},
		{"duplicate field", `{"protocol":1,"protocol":1,"request":{"method":"svc.get_state","params":null},"suite":"` + suite + `"}`, "duplicate object key"},
		{"trailing value", valid + `{}`, "trailing"},
		{"whitespace", ` {"protocol":1,"request":{"method":"svc.get_state","params":null},"suite":"` + suite + `"}`, "noncanonical"},
		{"wrong key order", `{"suite":"` + suite + `","protocol":1,"request":{"method":"svc.get_state","params":null}}`, "noncanonical"},
		{"missing protocol", `{"request":{"method":"svc.get_state","params":null},"suite":"` + suite + `"}`, "protocol must be"},
		{"wrong protocol", `{"protocol":2,"request":{"method":"svc.get_state","params":null},"suite":"` + suite + `"}`, "protocol must be"},
		{"persistent suite", `{"protocol":1,"request":{"method":"svc.get_state","params":null},"suite":"` + rpc.WireBuild + `"}`, "suite must be"},
		{"missing method", `{"protocol":1,"request":{"params":null},"suite":"` + suite + `"}`, "method must be"},
		{"wrong method", `{"protocol":1,"request":{"method":"svc.list","params":null},"suite":"` + suite + `"}`, "method must be"},
		{"missing params", `{"protocol":1,"request":{"method":"svc.get_state"},"suite":"` + suite + `"}`, "params must be null"},
		{"object params", `{"protocol":1,"request":{"method":"svc.get_state","params":{}},"suite":"` + suite + `"}`, "params must be null"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeRPCOnceRequest([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeRPCOnceRequest() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDecodeRPCOnceResponseRejectsInexactInput(t *testing.T) {
	suite := onceSuiteGolden
	tests := []struct {
		name    string
		encoded string
		want    string
	}{
		{"unknown response field", `{"protocol":1,"response":{"ok":true,"result":{},"x":1},"suite":"` + suite + `"}`, "unknown field"},
		{"duplicate nested field", `{"protocol":1,"response":{"ok":true,"result":{"x":1,"x":2}},"suite":"` + suite + `"}`, "duplicate object key"},
		{"missing response", `{"protocol":1,"suite":"` + suite + `"}`, "required"},
		{"missing result", `{"protocol":1,"response":{"ok":true},"suite":"` + suite + `"}`, "required"},
		{"success null result", `{"protocol":1,"response":{"ok":true,"result":null},"suite":"` + suite + `"}`, "must not be null"},
		{"success scalar result", `{"protocol":1,"response":{"ok":true,"result":1},"suite":"` + suite + `"}`, "must be an object"},
		{"success explicit error", `{"protocol":1,"response":{"error":"","ok":true,"result":{}},"suite":"` + suite + `"}`, "successful response error"},
		{"error missing message", `{"protocol":1,"response":{"ok":false,"result":null},"suite":"` + suite + `"}`, "error is required"},
		{"error with result", `{"protocol":1,"response":{"error":"failed","ok":false,"result":{}},"suite":"` + suite + `"}`, "must be null"},
		{"noncanonical response order", `{"protocol":1,"response":{"result":{},"ok":true},"suite":"` + suite + `"}`, "noncanonical"},
		{"noncanonical result order", `{"protocol":1,"response":{"ok":true,"result":{"z":1,"a":2}},"suite":"` + suite + `"}`, "noncanonical"},
		{"noncanonical escaped html", `{"protocol":1,"response":{"ok":true,"result":{"html":"\u003c"}},"suite":"` + suite + `"}`, "noncanonical"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeRPCOnceResponse([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeRPCOnceResponse() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRPCOnceCanonicalEscaping(t *testing.T) {
	encoded, err := EncodeRPCOnceResponse(&rpc.Response{OK: true, Result: json.RawMessage(`{"html":"<>&/"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"html":"<>&/"`)) || bytes.Contains(encoded, []byte(`\u003c`)) {
		t.Fatalf("response = %s, want canonical unescaped HTML and slash", encoded)
	}
	if _, err := DecodeRPCOnceResponse(encoded); err != nil {
		t.Fatalf("decode canonical response: %v", err)
	}
}

func TestRPCOnceEncodedBounds(t *testing.T) {
	oversized := json.RawMessage(`{"x":"` + strings.Repeat("x", RPCOnceMaxBytes) + `"}`)
	if _, err := EncodeRPCOnceResponse(&rpc.Response{OK: true, Result: oversized}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("EncodeRPCOnceResponse() = %v, want size error", err)
	}
	if _, err := DecodeRPCOnceRequest(bytes.Repeat([]byte{' '}, RPCOnceMaxBytes+1)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("DecodeRPCOnceRequest() = %v, want size error", err)
	}
	if _, err := ReadRPCOnceResponse(bytes.NewReader(bytes.Repeat([]byte{'x'}, RPCOnceMaxBytes+1))); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadRPCOnceResponse() = %v, want size error", err)
	}
}

func TestServeRPCOnce(t *testing.T) {
	request, err := EncodeRPCOnceRequest()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		getState func(context.Context) (RawRegistry, error)
		wantOK   bool
		wantBody string
	}{
		{"success", func(context.Context) (RawRegistry, error) { return json.RawMessage(`{"site":{}}`), nil }, true, `{"site":{}}`},
		{"typed method error", func(context.Context) (RawRegistry, error) { return nil, errors.New("load failed") }, false, "load failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := ServeRPCOnce(context.Background(), bytes.NewReader(request), &output, test.getState); err != nil {
				t.Fatalf("ServeRPCOnce: %v", err)
			}
			response, err := DecodeRPCOnceResponse(output.Bytes())
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.OK != test.wantOK {
				t.Fatalf("OK = %v, want %v", response.OK, test.wantOK)
			}
			if test.wantOK && string(response.Result) != test.wantBody {
				t.Fatalf("result = %s, want %s", response.Result, test.wantBody)
			}
			if !test.wantOK && response.Error != test.wantBody {
				t.Fatalf("error = %q, want %q", response.Error, test.wantBody)
			}
		})
	}
}

func TestServeRPCOnceProtocolFailureWritesNothing(t *testing.T) {
	var output bytes.Buffer
	err := ServeRPCOnce(context.Background(), strings.NewReader(`{}`), &output, func(context.Context) (RawRegistry, error) {
		t.Fatal("handler called for invalid request")
		return nil, nil
	})
	if err == nil || output.Len() != 0 {
		t.Fatalf("ServeRPCOnce() = %v, output %q; want error and no output", err, output.Bytes())
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestServeRPCOnceRejectsShortWrite(t *testing.T) {
	request, err := EncodeRPCOnceRequest()
	if err != nil {
		t.Fatal(err)
	}
	err = ServeRPCOnce(context.Background(), bytes.NewReader(request), shortWriter{}, func(context.Context) (RawRegistry, error) {
		return json.RawMessage(`{}`), nil
	})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("ServeRPCOnce() = %v, want io.ErrShortWrite", err)
	}
}
