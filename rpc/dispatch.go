package rpc

import (
	"context"
	"fmt"
	"sync"
)

// Handler runs one method against the params of a Request and returns the result to
// serialize into the Response, or an error to surface to the caller. The ctx carries
// a DispatchTimeout deadline.
type Handler func(ctx context.Context, params map[string]any) (any, error)

// Dispatcher routes a method name to the handler registered for it. Dispatch
// serializes every handler behind a single mutex, so a tool whose handlers take a
// shared cross-process lock never nests it. Register all handlers before serving;
// lookup happens at dispatch time and an unknown method is an error response, not a
// crash.
type Dispatcher struct {
	mu       sync.Mutex
	handlers map[string]Handler
}

// NewDispatcher returns an empty Dispatcher ready for Register.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{handlers: map[string]Handler{}}
}

// Register binds method to handler. The daemon calls handler with the request params
// when it dispatches that method.
func (d *Dispatcher) Register(method string, handler Handler) {
	d.handlers[method] = handler
}

// Dispatch runs the handler for req under the serialization lock with a hard
// DispatchTimeout, and turns an unknown method, a handler error, or a handler panic
// into an error Response so a bad request never crashes the server.
func (d *Dispatcher) Dispatch(ctx context.Context, req *Request) *Response {
	handler, ok := d.handlers[req.Method]
	if !ok {
		return &Response{OK: false, Error: fmt.Sprintf("unknown method %q", req.Method)}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, DispatchTimeout)
	defer cancel()

	result, err := invoke(ctx, handler, req.Params)
	if err != nil {
		return &Response{OK: false, Error: err.Error()}
	}
	return &Response{OK: true, Result: result}
}

func invoke(ctx context.Context, handler Handler, params map[string]any) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, params)
}
