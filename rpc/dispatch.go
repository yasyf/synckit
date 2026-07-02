package rpc

import (
	"context"
	"fmt"
	"sync"
)

// Handler runs one method against the params of a Request and returns the result to
// serialize into the Response, or an error to surface to the caller. The ctx carries
// a DispatchTimeout deadline, is canceled when the requesting connection closes, and
// dies the moment Dispatch returns, so state that must outlive the response — a
// background goroutine, a rebuilt watch generation — must parent to a longer-lived
// context, never this ctx.
type Handler func(ctx context.Context, params map[string]any) (any, error)

// Dispatcher routes a method name to the handler registered for it. Handlers
// dispatch concurrently, so a long-running method never queues the rest of the
// surface; a method registered via RegisterExclusive instead queues behind a single
// per-dispatcher mutex, for handlers that take a shared non-reentrant cross-process
// lock. Register all handlers before serving; lookup happens at dispatch time and an
// unknown method is an error response, not a crash.
type Dispatcher struct {
	mu        sync.Mutex
	handlers  map[string]Handler
	exclusive map[string]bool
}

// NewDispatcher returns an empty Dispatcher ready for Register.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{handlers: map[string]Handler{}, exclusive: map[string]bool{}}
}

// Register binds method to handler, dispatched concurrently with every other
// handler. The daemon calls handler with the request params when it dispatches that
// method.
func (d *Dispatcher) Register(method string, handler Handler) {
	d.handlers[method] = handler
}

// RegisterExclusive binds method to handler and serializes its dispatches behind the
// dispatcher's exclusive mutex: one exclusive handler runs at a time, so handlers
// that take a shared non-reentrant lock (a state flock) queue on the mutex instead
// of contending on the lock.
func (d *Dispatcher) RegisterExclusive(method string, handler Handler) {
	d.handlers[method] = handler
	d.exclusive[method] = true
}

// Dispatch runs the handler for req with a hard DispatchTimeout — exclusive methods
// behind the exclusive mutex, everything else concurrently — and turns an unknown
// method, a handler error, or a handler panic into an error Response so a bad
// request never crashes the server.
func (d *Dispatcher) Dispatch(ctx context.Context, req *Request) *Response {
	handler, ok := d.handlers[req.Method]
	if !ok {
		return &Response{OK: false, Error: fmt.Sprintf("unknown method %q", req.Method)}
	}

	if d.exclusive[req.Method] {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

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
