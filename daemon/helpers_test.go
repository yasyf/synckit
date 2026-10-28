package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/internal/rpctest"
	"github.com/yasyf/synckit/rpc"
)

func writeVersionedBinary(t *testing.T, dir, name, build string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	script := fmt.Sprintf("#!/bin/sh\necho %s version %s\n", name, build)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { //nolint:gosec // test-owned temp dir
		t.Fatal(err)
	}
	return path
}

func serveHelper(t *testing.T, name, build string) <-chan error {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	bin := filepath.Join(t.TempDir(), "helperstub")
	compile := exec.Command(goBin, "build", "-o", bin, "./testdata/helperstub") //nolint:gosec // G204: fixed test args, no untrusted input.
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("build helperstub: %v\n%s", err, out)
	}
	helper := exec.Command(bin, name, build) //nolint:gosec // G204: the test-built stub, fixed argv.
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = helper.Process.Kill() })
	exited := make(chan error, 1)
	go func() { exited <- helper.Wait() }()

	spec, err := helperruntime.Spec(name, daemonkit.Program{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer readyCancel()
	lane := rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	})
	defer func() { _ = lane.Close() }()
	if err := rpctest.WaitReady(readyCtx, lane); err != nil {
		t.Fatalf("wait ready: %v", err)
	}
	return exited
}

func TestReplaceOutdatedHelperDrainsOnlyAnotherBuild(t *testing.T) {
	for _, tc := range []struct {
		name      string
		installed string
		drained   bool
	}{
		{name: "same build", installed: "1.0.0"},
		{name: "upgraded build", installed: "1.0.1", drained: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortDaemonHome(t)
			exited := serveHelper(t, "reposync", "1.0.0")
			writeVersionedBinary(t, usePathBinaries(t), "reposync", tc.installed)

			if err := replaceOutdatedHelper(t.Context(), helperManifest("reposync", "")); err != nil {
				t.Fatalf("replaceOutdatedHelper: %v", err)
			}

			select {
			case err := <-exited:
				if !tc.drained {
					t.Fatalf("helper serving the installed build exited: %v", err)
				}
			case <-time.After(time.Second):
				if tc.drained {
					t.Fatal("helper serving another build was not drained")
				}
			}
		})
	}
}

func TestReplaceOutdatedHelperLeavesAnAbsentHelperAlone(t *testing.T) {
	shortDaemonHome(t)
	writeVersionedBinary(t, usePathBinaries(t), "reposync", "1.0.1")

	if err := replaceOutdatedHelper(t.Context(), helperManifest("reposync", "")); err != nil {
		t.Fatalf("replaceOutdatedHelper: %v", err)
	}
}
