package syncservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
)

const (
	// RPCOnceCommand is the fixed one-shot helper subcommand.
	RPCOnceCommand = "rpc-once-v1"
	// RPCOnceMaxBytes bounds each encoded request and response.
	RPCOnceMaxBytes = 16 << 20
)

type rpcOnceRequest struct {
	Protocol *uint16              `json:"protocol"`
	Request  *rpcOnceTypedRequest `json:"request"`
	Suite    *string              `json:"suite"`
}

type rpcOnceTypedRequest struct {
	Method *string         `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcOnceResponse struct {
	Protocol *uint16               `json:"protocol"`
	Response *rpcOnceTypedResponse `json:"response"`
	Suite    *string               `json:"suite"`
}

type rpcOnceTypedResponse struct {
	Error  *string         `json:"error,omitempty"`
	OK     *bool           `json:"ok"`
	Result json.RawMessage `json:"result"`
}

// EncodeRPCOnceRequest returns the canonical get-state request envelope.
func EncodeRPCOnceRequest() ([]byte, error) {
	value := map[string]any{
		"protocol": rpc.OnceVersion,
		"request":  &rpc.Request{Method: MethodGetState},
		"suite":    rpc.OnceWireBuild,
	}
	return encodeRPCOnce(value, "request")
}

// DecodeRPCOnceRequest validates and returns one exact get-state request.
func DecodeRPCOnceRequest(encoded []byte) (*rpc.Request, error) {
	if err := validateRPCOnceBytes(encoded, "request"); err != nil {
		return nil, err
	}
	var envelope rpcOnceRequest
	if err := hostregistry.DecodeExactJSON(encoded, &envelope); err != nil {
		return nil, fmt.Errorf("decode rpc-once request: %w", err)
	}
	if envelope.Protocol == nil || *envelope.Protocol != rpc.OnceVersion {
		return nil, fmt.Errorf("decode rpc-once request: protocol must be %d", rpc.OnceVersion)
	}
	if envelope.Suite == nil || *envelope.Suite != rpc.OnceWireBuild {
		return nil, fmt.Errorf("decode rpc-once request: suite must be %q", rpc.OnceWireBuild)
	}
	if envelope.Request == nil || envelope.Request.Method == nil || *envelope.Request.Method != MethodGetState {
		return nil, fmt.Errorf("decode rpc-once request: method must be %q", MethodGetState)
	}
	if !bytes.Equal(envelope.Request.Params, []byte("null")) {
		return nil, errors.New("decode rpc-once request: params must be null")
	}
	return &rpc.Request{Method: MethodGetState}, nil
}

// EncodeRPCOnceResponse returns the canonical response envelope.
func EncodeRPCOnceResponse(response *rpc.Response) ([]byte, error) {
	if err := validateRPCOnceResponse(response); err != nil {
		return nil, fmt.Errorf("encode rpc-once response: %w", err)
	}
	value := map[string]any{
		"protocol": rpc.OnceVersion,
		"response": response,
		"suite":    rpc.OnceWireBuild,
	}
	return encodeRPCOnce(value, "response")
}

// DecodeRPCOnceResponse validates and returns one exact response envelope.
func DecodeRPCOnceResponse(encoded []byte) (*rpc.Response, error) {
	if err := validateRPCOnceBytes(encoded, "response"); err != nil {
		return nil, err
	}
	var envelope rpcOnceResponse
	if err := hostregistry.DecodeExactJSON(encoded, &envelope); err != nil {
		return nil, fmt.Errorf("decode rpc-once response: %w", err)
	}
	if envelope.Protocol == nil || *envelope.Protocol != rpc.OnceVersion {
		return nil, fmt.Errorf("decode rpc-once response: protocol must be %d", rpc.OnceVersion)
	}
	if envelope.Suite == nil || *envelope.Suite != rpc.OnceWireBuild {
		return nil, fmt.Errorf("decode rpc-once response: suite must be %q", rpc.OnceWireBuild)
	}
	if envelope.Response == nil || envelope.Response.OK == nil || len(envelope.Response.Result) == 0 {
		return nil, errors.New("decode rpc-once response: ok and result are required")
	}
	if *envelope.Response.OK && envelope.Response.Error != nil {
		return nil, errors.New("decode rpc-once response: successful response error must be absent")
	}
	if !*envelope.Response.OK && envelope.Response.Error == nil {
		return nil, errors.New("decode rpc-once response: failed response error is required")
	}
	response := &rpc.Response{OK: *envelope.Response.OK, Result: envelope.Response.Result}
	if envelope.Response.Error != nil {
		response.Error = *envelope.Response.Error
	}
	if err := validateRPCOnceResponse(response); err != nil {
		return nil, fmt.Errorf("decode rpc-once response: %w", err)
	}
	return response, nil
}

// ReadRPCOnceRequest reads one EOF-delimited request.
func ReadRPCOnceRequest(reader io.Reader) (*rpc.Request, error) {
	encoded, err := readRPCOnce(reader, "request")
	if err != nil {
		return nil, err
	}
	return DecodeRPCOnceRequest(encoded)
}

// ReadRPCOnceResponse reads one EOF-delimited response.
func ReadRPCOnceResponse(reader io.Reader) (*rpc.Response, error) {
	encoded, err := readRPCOnce(reader, "response")
	if err != nil {
		return nil, err
	}
	return DecodeRPCOnceResponse(encoded)
}

// ServeRPCOnce serves one EOF-delimited get-state request and response.
func ServeRPCOnce(
	ctx context.Context,
	reader io.Reader,
	writer io.Writer,
	getState func(context.Context) (RawRegistry, error),
) error {
	if getState == nil {
		return errors.New("rpc-once get-state handler is required")
	}
	if _, err := ReadRPCOnceRequest(reader); err != nil {
		return err
	}
	state, methodErr := getState(ctx)
	response := &rpc.Response{OK: methodErr == nil, Result: state}
	if methodErr != nil {
		response.Result = json.RawMessage("null")
		response.Error = methodErr.Error()
	}
	encoded, err := EncodeRPCOnceResponse(response)
	if err != nil {
		return err
	}
	written, err := writer.Write(encoded)
	if err != nil {
		return fmt.Errorf("write rpc-once response: %w", err)
	}
	if written != len(encoded) {
		return fmt.Errorf("write rpc-once response: %w", io.ErrShortWrite)
	}
	return nil
}

func encodeRPCOnce(value any, kind string) ([]byte, error) {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return nil, fmt.Errorf("encode rpc-once %s: %w", kind, err)
	}
	if len(encoded) > RPCOnceMaxBytes {
		return nil, fmt.Errorf("encode rpc-once %s: %d bytes exceeds %d", kind, len(encoded), RPCOnceMaxBytes)
	}
	return encoded, nil
}

func validateRPCOnceBytes(encoded []byte, kind string) error {
	if len(encoded) == 0 {
		return fmt.Errorf("decode rpc-once %s: empty input", kind)
	}
	if len(encoded) > RPCOnceMaxBytes {
		return fmt.Errorf("decode rpc-once %s: %d bytes exceeds %d", kind, len(encoded), RPCOnceMaxBytes)
	}
	if err := hostregistry.DecodeExactJSON(encoded, new(any)); err != nil {
		return fmt.Errorf("decode rpc-once %s: %w", kind, err)
	}
	canonical, err := canonicalJSON(json.RawMessage(encoded))
	if err != nil {
		return fmt.Errorf("decode rpc-once %s: %w", kind, err)
	}
	if !bytes.Equal(encoded, canonical) {
		return fmt.Errorf("decode rpc-once %s: noncanonical JSON", kind)
	}
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	intermediate, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := hostregistry.DecodeExactJSON(intermediate, new(any)); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(intermediate))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(generic); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func validateRPCOnceResponse(response *rpc.Response) error {
	if response == nil {
		return errors.New("response is required")
	}
	if len(response.Result) == 0 {
		return errors.New("result is required")
	}
	if err := validateRPCOnceBytes(response.Result, "result"); err != nil {
		return err
	}
	nullResult := bytes.Equal(response.Result, []byte("null"))
	if response.OK {
		if nullResult {
			return errors.New("successful result must not be null")
		}
		var object map[string]json.RawMessage
		if err := hostregistry.DecodeExactJSON(response.Result, &object); err != nil {
			return fmt.Errorf("successful result must be an object: %w", err)
		}
		if response.Error != "" {
			return errors.New("successful response error must be empty")
		}
		return nil
	}
	if !nullResult {
		return errors.New("failed result must be null")
	}
	if response.Error == "" {
		return errors.New("failed response error is required")
	}
	return nil
}

func readRPCOnce(reader io.Reader, kind string) ([]byte, error) {
	encoded, err := io.ReadAll(io.LimitReader(reader, RPCOnceMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read rpc-once %s: %w", kind, err)
	}
	if len(encoded) > RPCOnceMaxBytes {
		return nil, fmt.Errorf("read rpc-once %s: exceeds %d bytes", kind, RPCOnceMaxBytes)
	}
	return encoded, nil
}
