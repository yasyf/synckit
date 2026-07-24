package synctransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestBoundedCaptureTruncatesAndDrains(t *testing.T) {
	reader, writer := io.Pipe()
	capture := newBoundedCapture(reader)
	payload := bytes.Repeat([]byte("x"), maxStderrBytes+4096)
	go func() {
		_, _ = writer.Write(payload)
		_ = writer.Close()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := capture.wait(ctx); err != nil {
		t.Fatal(err)
	}
	got, truncated := capture.snapshot()
	if !truncated || len(got) != maxStderrBytes || !bytes.Equal(got, payload[:maxStderrBytes]) {
		t.Fatalf("capture = %d bytes, truncated=%t", len(got), truncated)
	}
}

func TestBoundedCaptureDeadlineClosesBlockedReader(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	capture := newBoundedCapture(reader)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := capture.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want deadline exceeded", err)
	}
}
