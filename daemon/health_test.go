package daemon

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

// TestServeChildPublishesReadyHealth pins the successor to synckit's deleted
// health lane: daemonkit's own Client.WaitReady over the control lane, against
// a real synckitd serve process. It cannot run in process — the control lane
// pins the serving process and refuses a peer PID equal to the caller's.
func TestServeChildPublishesReadyHealth(t *testing.T) {
	shortConfigHome(t)
	shortDaemonHome(t)

	daemonProcess := exec.Command(buildServeStub(t), "serve") //nolint:gosec // fixed test binary and argument
	var stderr bytes.Buffer
	daemonProcess.Stderr = &stderr
	if err := daemonProcess.Start(); err != nil {
		t.Fatalf("start serve daemon: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- daemonProcess.Wait() }()
	t.Cleanup(func() {
		_ = daemonProcess.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("serve daemon: %v\n%s", err, stderr.String())
			}
		case <-time.After(60 * time.Second):
			_ = daemonProcess.Process.Kill()
			t.Errorf("serve daemon did not exit on SIGTERM\n%s", stderr.String())
		}
	})

	client, err := daemonkit.Open(clientSpec())
	if err != nil {
		t.Fatalf("open serve spec: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	health, err := client.WaitReady(ctx)
	if err != nil {
		t.Fatalf("WaitReady: %v\n%s", err, stderr.String())
	}
	if health.Phase != daemonkit.PhaseReady {
		t.Errorf("phase = %v, want ready", health.Phase)
	}
	if health.PID != daemonProcess.Process.Pid {
		t.Errorf("pid = %d, want the serving process %d", health.PID, daemonProcess.Process.Pid)
	}
	if health.Build == "" || health.Protocol == 0 || health.Generation == 0 {
		t.Errorf("health = %+v, want a stated build, protocol, and owner generation", health)
	}
}

func buildServeStub(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), "servestub")
	build := exec.Command(goBin, "build", "-o", bin, "./testdata/servestub") //nolint:gosec // G204: fixed test args, no untrusted input.
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build servestub: %v\n%s", err, out)
	}
	return bin
}
