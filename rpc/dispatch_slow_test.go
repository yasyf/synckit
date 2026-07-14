package rpc

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuf is a concurrency-safe io.Writer: the AfterFunc WARN fires on its own
// goroutine while the test reads the buffer, so both sides need the lock.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestDispatchWarnsWhileHandlerStillRunning proves the slow-dispatch WARN names the
// method while the handler is still blocked — not only after it returns — so a wedged
// handler that never completes is still surfaced.
func TestDispatchWarnsWhileHandlerStillRunning(t *testing.T) {
	prev := slowDispatchThreshold
	slowDispatchThreshold = 20 * time.Millisecond
	t.Cleanup(func() { slowDispatchThreshold = prev })

	buf := &syncBuf{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	entered := make(chan struct{})
	release := make(chan struct{})
	d := NewDispatcher()
	d.Register("svc.slow", func(_ context.Context, _ map[string]any) (any, error) {
		close(entered)
		<-release
		return "done", nil
	})

	done := make(chan *Response, 1)
	go func() {
		done <- d.Dispatch(context.Background(), &Request{Method: "svc.slow", Params: map[string]any{}})
	}()

	<-entered

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "handler still running") {
		select {
		case <-done:
			t.Fatalf("handler completed before the in-flight WARN; log: %s", buf.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no in-flight WARN appeared; log: %s", buf.String())
		}
		time.Sleep(2 * time.Millisecond)
	}

	if logged := buf.String(); !strings.Contains(logged, "svc.slow") {
		t.Fatalf("in-flight WARN did not name the method: %s", logged)
	}

	close(release)
	select {
	case resp := <-done:
		if !resp.OK {
			t.Fatalf("dispatch failed: %s", resp.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not complete after the handler was released")
	}
}

// TestDispatchLogsCompletionWarnAfterInFlight proves the completion WARN: a handler that
// runs past the threshold logs "slow handler completed" naming the method and its
// elapsed, and that line is always ordered after the in-flight WARN — never before it.
func TestDispatchLogsCompletionWarnAfterInFlight(t *testing.T) {
	prev := slowDispatchThreshold
	slowDispatchThreshold = 20 * time.Millisecond
	t.Cleanup(func() { slowDispatchThreshold = prev })

	buf := &syncBuf{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	entered := make(chan struct{})
	release := make(chan struct{})
	d := NewDispatcher()
	d.Register("svc.slow", func(_ context.Context, _ map[string]any) (any, error) {
		close(entered)
		<-release
		return "done", nil
	})

	done := make(chan *Response, 1)
	go func() {
		done <- d.Dispatch(context.Background(), &Request{Method: "svc.slow", Params: map[string]any{}})
	}()

	<-entered

	// Hold the handler until the in-flight WARN is observed, so it is deterministically
	// ordered before the completion WARN.
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "handler still running") {
		if time.Now().After(deadline) {
			t.Fatalf("no in-flight WARN appeared; log: %s", buf.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(release)

	select {
	case resp := <-done:
		if !resp.OK {
			t.Fatalf("dispatch failed: %s", resp.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not complete after the handler was released")
	}

	logged := buf.String()
	inFlightAt := strings.Index(logged, "handler still running")
	completedAt := strings.Index(logged, "slow handler completed")
	if completedAt < 0 {
		t.Fatalf("no completion WARN; log: %s", logged)
	}
	if inFlightAt < 0 || inFlightAt > completedAt {
		t.Fatalf("completion WARN did not follow the in-flight WARN; log: %s", logged)
	}
	completionLine := logged[completedAt:]
	if !strings.Contains(completionLine, "svc.slow") {
		t.Fatalf("completion WARN did not name the method; log: %s", logged)
	}
	if !strings.Contains(completionLine, "elapsed=") {
		t.Fatalf("completion WARN missing elapsed; log: %s", logged)
	}
}
