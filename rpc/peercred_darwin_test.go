//go:build darwin

package rpc

import (
	"context"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// TestPeerPIDReachesHandler proves the handler ctx on the unix-socket serve path
// carries the client's PID: the client is this test process, so PeerPID must report
// os.Getpid().
func TestPeerPIDReachesHandler(t *testing.T) {
	type seen struct {
		pid int
		ok  bool
	}
	got := make(chan seen, 1)
	d := NewDispatcher()
	d.Register("whoami", func(ctx context.Context, _ map[string]any) (any, error) {
		pid, ok := PeerPID(ctx)
		got <- seen{pid, ok}
		return nil, nil
	})
	sock := serve(t, d)

	resp, err := Call(context.Background(), sock, &Request{Method: "whoami"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ok = false, error = %q", resp.Error)
	}
	s := <-got
	if !s.ok {
		t.Fatal("PeerPID ok = false, want the serve path to capture the peer pid")
	}
	if s.pid != os.Getpid() {
		t.Errorf("peer pid = %d, want %d (the client is this test process)", s.pid, os.Getpid())
	}
}

// TestPeerSIDReachesHandler proves the handler ctx on the unix-socket serve path
// carries the client's session ID, derived via getsid(2) from the peer PID. The
// client, the server, and the test process are all this one process, so it validates
// the plumbing only — it does not exercise a cross-session getsid.
func TestPeerSIDReachesHandler(t *testing.T) {
	type seen struct {
		sid int
		ok  bool
	}
	got := make(chan seen, 1)
	d := NewDispatcher()
	d.Register("whoami", func(ctx context.Context, _ map[string]any) (any, error) {
		sid, ok := PeerSID(ctx)
		got <- seen{sid, ok}
		return nil, nil
	})
	sock := serve(t, d)

	resp, err := Call(context.Background(), sock, &Request{Method: "whoami"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ok = false, error = %q", resp.Error)
	}
	s := <-got
	if !s.ok {
		t.Fatal("PeerSID ok = false, want the serve path to capture the peer session id")
	}
	want, err := unix.Getsid(os.Getpid())
	if err != nil {
		t.Fatalf("getsid: %v", err)
	}
	if s.sid != want {
		t.Errorf("peer sid = %d, want %d (the client is this test process)", s.sid, want)
	}
}
