package netpolicy

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type fakeMonitor struct {
	*observed
}

func (m fakeMonitor) Close() error {
	return m.close()
}

func newFakeMonitor(t *testing.T, s State) (fakeMonitor, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), manualFileName)
	o, err := newObserved(path)
	if err != nil {
		t.Fatalf("newObserved: %v", err)
	}
	t.Cleanup(func() {
		if err := o.close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	o.publish(s)
	return fakeMonitor{o}, path
}

func staticRemote(s State) RemoteFunc {
	return func(context.Context) (State, error) { return s, nil }
}

type waitResult struct {
	remote State
	err    error
}

func startWait(ctx context.Context, g *Gate, remote RemoteFunc) <-chan waitResult {
	done := make(chan waitResult, 1)
	go func() {
		peer, err := g.Wait(ctx, remote)
		done <- waitResult{peer, err}
	}()
	return done
}

func TestGateCheckPausesMidLoop(t *testing.T) {
	m, _ := newFakeMonitor(t, connected)
	g := NewGate(m, time.Hour)
	sent := 0
	var err error
	for chunk := range 5 {
		if chunk == 2 {
			m.publish(State{Status: StatusConnected, Cellular: true})
		}
		if err = g.Check(context.Background(), connected); err != nil {
			break
		}
		sent++
	}
	var paused *PausedError
	if !errors.As(err, &paused) {
		t.Fatalf("Check error = %v, want *PausedError", err)
	}
	if paused.Reason != "local: cellular" {
		t.Errorf("Reason = %q, want %q", paused.Reason, "local: cellular")
	}
	if sent != 2 {
		t.Errorf("sent %d chunks, want 2", sent)
	}
}

func TestGateCheck(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name       string
		ctx        context.Context
		local      State
		remote     State
		wantErr    error
		wantReason string
	}{
		{"allowed", context.Background(), connected, connected, nil, ""},
		{"remote expensive", context.Background(), connected, State{Status: StatusConnected, Expensive: true}, nil, "remote: expensive"},
		{"remote absent", context.Background(), connected, State{}, nil, "remote: unknown"},
		{"cancelled wins", cancelled, connected, connected, context.Canceled, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newFakeMonitor(t, tt.local)
			err := NewGate(m, time.Hour).Check(tt.ctx, tt.remote)
			var paused *PausedError
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Check() = %v, want %v", err, tt.wantErr)
				}
			case tt.wantReason != "":
				if !errors.As(err, &paused) || paused.Reason != tt.wantReason {
					t.Fatalf("Check() = %v, want paused %q", err, tt.wantReason)
				}
			case err != nil:
				t.Fatalf("Check() = %v, want nil", err)
			}
		})
	}
}

func TestGateCheckHonorsManualOverride(t *testing.T) {
	m, path := newFakeMonitor(t, connected)
	saveManual(t, path, Manual{Metered: true})
	var paused *PausedError
	if err := NewGate(m, time.Hour).Check(context.Background(), connected); !errors.As(err, &paused) || paused.Reason != "local: manual metered" {
		t.Fatalf("Check() = %v, want paused local: manual metered", err)
	}
}

func TestGateWaitResumesOnLocalChange(t *testing.T) {
	m, _ := newFakeMonitor(t, State{Status: StatusDisconnected})
	done := startWait(context.Background(), NewGate(m, time.Hour), staticRemote(connected))
	select {
	case r := <-done:
		t.Fatalf("Wait returned while local disconnected: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	m.publish(connected)
	select {
	case r := <-done:
		if r.err != nil || r.remote != connected {
			t.Fatalf("Wait() = %+v, want connected remote", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not resume after the local path changed")
	}
}

func TestGateWaitRepollsRemote(t *testing.T) {
	m, _ := newFakeMonitor(t, connected)
	var calls atomic.Int32
	remote := func(context.Context) (State, error) {
		if calls.Add(1) <= 3 {
			return State{Status: StatusConnected, Constrained: true}, nil
		}
		return connected, nil
	}
	peer, err := NewGate(m, 5*time.Millisecond).Wait(context.Background(), remote)
	if err != nil || peer != connected {
		t.Fatalf("Wait() = %+v, %v; want connected, nil", peer, err)
	}
	if got := calls.Load(); got != 4 {
		t.Errorf("remote polled %d times, want 4", got)
	}
}

func TestGateWaitResumesOnManualEdit(t *testing.T) {
	m, path := newFakeMonitor(t, connected)
	saveManual(t, path, Manual{Metered: true})
	done := startWait(context.Background(), NewGate(m, time.Hour), staticRemote(connected))
	select {
	case r := <-done:
		t.Fatalf("Wait returned while manually metered: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	saveManual(t, path, Manual{})
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Wait() error = %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not resume after the manual override cleared")
	}
}

func TestGateWaitErrors(t *testing.T) {
	errUnreachable := errors.New("peer unreachable")
	tests := []struct {
		name    string
		remote  RemoteFunc
		wantErr error
	}{
		{"remote fetch fails", func(context.Context) (State, error) { return State{}, errUnreachable }, errUnreachable},
		{"deadline while blocked", staticRemote(State{Status: StatusDisconnected}), context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newFakeMonitor(t, connected)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if _, err := NewGate(m, time.Hour).Wait(ctx, tt.remote); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Wait() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
