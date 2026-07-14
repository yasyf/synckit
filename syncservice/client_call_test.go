package syncservice

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/synckit/rpc"
)

type callRecordingTransport struct {
	response *Response
	err      error
	request  *rpc.Request
}

func (t *callRecordingTransport) Do(_ context.Context, req *rpc.Request) (*Response, error) {
	t.request = req
	return t.response, t.err
}

func (*callRecordingTransport) Close() error { return nil }

type customCallResult struct {
	Credential string `json:"credential"`
}

func TestClientCall(t *testing.T) {
	errTransport := errors.New("transport failed")
	tests := []struct {
		name          string
		method        string
		params        map[string]any
		response      *Response
		transportErr  error
		out           any
		wantResult    customCallResult
		wantErr       bool
		wantErrIs     error
		wantErrParts  []string
		wantDecodeErr bool
	}{
		{
			name:     "custom method decodes result",
			method:   "ccp.fetch_stripped_credential",
			params:   map[string]any{"uuid": "credential-1"},
			response: &Response{OK: true, Result: json.RawMessage(`{"credential":"secret"}`)},
			out:      &customCallResult{},
			wantResult: customCallResult{
				Credential: "secret",
			},
		},
		{
			name:         "remote error names method",
			method:       "custom.denied",
			params:       map[string]any{"scope": "private"},
			response:     &Response{OK: false, Error: "permission denied"},
			out:          &customCallResult{},
			wantErr:      true,
			wantErrParts: []string{"custom.denied", "permission denied"},
		},
		{
			name:         "transport error is propagated",
			method:       "custom.unavailable",
			params:       map[string]any{"attempt": 1},
			transportErr: errTransport,
			out:          &customCallResult{},
			wantErr:      true,
			wantErrIs:    errTransport,
		},
		{
			name:          "decode error names method and wraps cause",
			method:        "custom.invalid_result",
			response:      &Response{OK: true, Result: json.RawMessage(`{"credential":123}`)},
			out:           &customCallResult{},
			wantErr:       true,
			wantErrParts:  []string{"decode custom.invalid_result result"},
			wantDecodeErr: true,
		},
		{
			name:     "nil output discards result",
			method:   "custom.notify",
			params:   map[string]any{"enabled": true},
			response: &Response{OK: true, Result: json.RawMessage(`{`)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &callRecordingTransport{response: tt.response, err: tt.transportErr}
			err := NewClient(tx).Call(context.Background(), tt.method, tt.params, tt.out)

			if tt.wantErr && err == nil {
				t.Fatal("Call() error = nil, want non-nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Call() error = %v, want nil", err)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Call() error = %v, want errors.Is(_, %v)", err, tt.wantErrIs)
			}
			for _, part := range tt.wantErrParts {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("Call() error = %q, want it to contain %q", err, part)
				}
			}
			if tt.wantDecodeErr {
				var decodeErr *json.UnmarshalTypeError
				if !errors.As(err, &decodeErr) {
					t.Errorf("Call() error = %v, want a wrapped *json.UnmarshalTypeError", err)
				}
			}

			if tx.request == nil {
				t.Fatal("Call() did not send a request")
			}
			if tx.request.Method != tt.method {
				t.Errorf("request method = %q, want %q", tx.request.Method, tt.method)
			}
			if !reflect.DeepEqual(tx.request.Params, tt.params) {
				t.Errorf("request params = %#v, want %#v", tx.request.Params, tt.params)
			}
			if tt.wantResult != (customCallResult{}) {
				if got := *(tt.out.(*customCallResult)); got != tt.wantResult {
					t.Errorf("decoded result = %+v, want %+v", got, tt.wantResult)
				}
			}
		})
	}
}
