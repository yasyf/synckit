package rpc_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yasyf/synckit/internal/rpctest"
	"github.com/yasyf/synckit/rpc"
)

func TestCallEndsWithItsContextWhileTheHandlerRuns(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("DAEMONKIT_HOME", home)
	release := make(chan struct{})
	dispatcher := rpc.NewDispatcher()
	dispatcher.Register("busy", func(context.Context, map[string]any) (any, error) {
		<-release
		return true, nil
	})
	dispatcher.Register("ping", func(context.Context, map[string]any) (any, error) {
		return true, nil
	})
	server, err := rpctest.Start(t.Context(), "com.github.yasyf.synckit.rpctest.busy", dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	t.Cleanup(func() { close(release) })

	tests := []struct {
		name  string
		scope func(context.Context) (context.Context, context.CancelFunc)
		want  error
	}{
		{
			name: "deadline",
			scope: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(ctx, 50*time.Millisecond)
			},
			want: context.DeadlineExceeded,
		},
		{
			name: "cancelled",
			scope: func(ctx context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(ctx)
				time.AfterFunc(50*time.Millisecond, cancel)
				return ctx, cancel
			},
			want: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := server.Client()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := tt.scope(t.Context())
			defer cancel()
			started := time.Now()
			_, err = client.Call(ctx, &rpc.Request{Method: "busy"})
			elapsed := time.Since(started)
			var transport *rpc.TransportError
			if !errors.As(err, &transport) || !errors.Is(err, tt.want) {
				t.Fatalf("Call() error = %v, want a TransportError wrapping %v", err, tt.want)
			}
			if elapsed > time.Second {
				t.Fatalf("Call() returned %s after its context ended at 50ms, want under 1s", elapsed)
			}
			pingCtx, cancelPing := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelPing()
			resp, err := client.Call(pingCtx, &rpc.Request{Method: "ping"})
			if err != nil || !resp.OK {
				t.Fatalf("Call() after the retired lane = %+v, %v, want OK on a fresh lane", resp, err)
			}
		})
	}
}
