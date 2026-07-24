package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
)

func TestRemoteHelloExactEcho(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	nonce := [RemoteHelloNonceBytes]byte{1, 2, 3}
	done := make(chan error, 1)
	go func() { done <- ServeRemoteHello(server, server) }()
	if err := VerifyRemoteHello(t.Context(), client, nonce); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRemoteServeCommandLineIsFixedAndQuoted(t *testing.T) {
	got, err := RemoteServeCommandLine("/Applications/Synckit Runtime/synckitd", "cookie-sync")
	if err != nil {
		t.Fatal(err)
	}
	want := "'/Applications/Synckit Runtime/synckitd' 'rpc-serve-v1' 'cookie-sync'"
	if got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
	for _, invalid := range [][2]string{
		{"synckitd", "svc"},
		{"/bin/synckitd/../other", "svc"},
		{"/bin/synckitd", "../svc"},
	} {
		if _, err := RemoteServeCommandLine(invalid[0], invalid[1]); err == nil {
			t.Fatalf("RemoteServeCommandLine(%q, %q) succeeded", invalid[0], invalid[1])
		}
	}
}

func TestRemoteHelloRejectsWrongLengthBeforePayload(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	done := make(chan error, 1)
	go func() { done <- ServeRemoteHello(server, server) }()
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], RemoteHelloNonceBytes+1)
	if _, err := client.Write(length[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("oversized hello accepted")
	}
}

func TestVerifyRemoteHelloRejectsWrongNonce(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	nonce := [RemoteHelloNonceBytes]byte{1}
	go func() {
		got, err := readRemoteHello(server)
		if err != nil {
			return
		}
		got[0]++
		_ = writeRemoteHello(server, got)
	}()
	if err := VerifyRemoteHello(context.Background(), client, nonce); err == nil {
		t.Fatal("wrong nonce echo accepted")
	}
}

func TestVerifyRemoteHelloRejectsZeroNonce(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	if err := VerifyRemoteHello(context.Background(), client, [RemoteHelloNonceBytes]byte{}); err == nil {
		t.Fatal("zero nonce accepted")
	}
}

func TestVerifyRemoteHelloHonorsCanceledContext(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := VerifyRemoteHello(ctx, client, [RemoteHelloNonceBytes]byte{1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyRemoteHello = %v, want context canceled", err)
	}
}
