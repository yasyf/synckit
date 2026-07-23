package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

func TestServeSessionUsesSpawnedParentIdentityAndJoins(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.Register("peer", func(ctx context.Context, _ map[string]any) (any, error) {
		pid, ok := PeerPID(ctx)
		if !ok {
			return nil, errors.New("handler has no authenticated peer PID")
		}
		return pid, nil
	})
	clientToServerReader, clientToServerWriter := io.Pipe()
	serverToClientReader, serverToClientWriter := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- NewServer(dispatcher).ServeSession(
			context.Background(), clientToServerReader, serverToClientWriter,
		)
	}()
	conn, err := wire.NewDuplexConn(serverToClientReader, clientToServerWriter)
	if err != nil {
		t.Fatalf("NewDuplexConn: %v", err)
	}
	client := NewClient(ClientConfig{
		WireBuild: WireBuild,
		Dial:      func(context.Context) (net.Conn, error) { return conn, nil },
	})
	response, err := client.Call(context.Background(), &Request{Method: "peer"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got := decodeResult[int](t, response); got != os.Getppid() {
		t.Fatalf("peer PID = %d, want spawned parent %d", got, os.Getppid())
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeSession: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeSession did not join")
	}
}
