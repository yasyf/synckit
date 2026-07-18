package rpc

import (
	"context"
	"strings"
	"testing"
)

// assertFramed asserts line is exactly want followed by a single '\n' terminator, and
// that the frame is newline-delimited with no length prefix: it begins at the JSON
// object and carries exactly one newline (the terminator). These are the wire
// invariants the daemonkit rewire must preserve byte-for-byte.
func assertFramed(t *testing.T, line []byte, want string) {
	t.Helper()
	if got := string(line); got != want+"\n" {
		t.Fatalf("frame = %q, want %q", got, want+"\n")
	}
	if n := strings.Count(string(line), "\n"); n != 1 {
		t.Errorf("frame %q has %d newlines, want exactly one (the terminator)", line, n)
	}
	if line[0] != '{' {
		t.Errorf("frame %q does not begin at the JSON object — a length prefix would precede it", line)
	}
}

// TestRequestFrameGolden pins the exact request wire: field order method then params,
// params encoded as null when Params is nil (there is no omitempty, so an "empty
// params are omitted" assumption is false at the byte level), and LF framing with no
// length prefix.
func TestRequestFrameGolden(t *testing.T) {
	tests := []struct {
		name string
		req  *Request
		want string
	}{
		{"nil params marshals null", &Request{Method: "svc.list"}, `{"method":"svc.list","params":null}`},
		{"empty method still carries params null", &Request{Method: ""}, `{"method":"","params":null}`},
		{"params object", &Request{Method: "svc.sync", Params: map[string]any{"origin": "host-a"}}, `{"method":"svc.sync","params":{"origin":"host-a"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, err := EncodeRequest(tt.req)
			if err != nil {
				t.Fatalf("EncodeRequest: %v", err)
			}
			assertFramed(t, line, tt.want)
		})
	}
}

// TestResponseFrameGolden pins the exact response wire: field order ok, result, error;
// result encoded as null (never omitted) including on an error response; error omitted
// when empty; and LF framing with no length prefix.
func TestResponseFrameGolden(t *testing.T) {
	tests := []struct {
		name string
		resp *Response
		want string
	}{
		{"ok with result object", &Response{OK: true, Result: map[string]any{"converged": float64(2)}}, `{"ok":true,"result":{"converged":2}}`},
		{"ok nil result is null", &Response{OK: true}, `{"ok":true,"result":null}`},
		{"error response carries result null", &Response{OK: false, Error: "boom"}, `{"ok":false,"result":null,"error":"boom"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, err := EncodeResponse(tt.resp)
			if err != nil {
				t.Fatalf("EncodeResponse: %v", err)
			}
			assertFramed(t, line, tt.want)
		})
	}
}

// TestUnknownMethodFrameGolden pins the exact bytes the Dispatcher emits for an unknown
// method, including the %q-quoted method name. reposync string-matches "unknown method"
// on this reply, so the full frame — result:null and the escaped quotes around the
// method — is a consumer contract.
func TestUnknownMethodFrameGolden(t *testing.T) {
	resp := NewDispatcher().Dispatch(context.Background(), &Request{Method: "nope"})
	line, err := EncodeResponse(resp)
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	assertFramed(t, line, `{"ok":false,"result":null,"error":"unknown method \"nope\""}`)
}
