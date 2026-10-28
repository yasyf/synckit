package daemon

import (
	"bytes"
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

func useStartInstall(t *testing.T) *[]string {
	t.Helper()
	var started []string
	startInstall = func(program string) error {
		started = append(started, program)
		return nil
	}
	t.Cleanup(func() { startInstall = launchInstall })
	return &started
}

func TestRestageUpgradeInstallsOnlyADifferentInstalledBuild(t *testing.T) {
	for _, tc := range []struct {
		name      string
		installed string
		want      bool
	}{
		{name: "absent"},
		{name: "current", installed: "1.2.3"},
		{name: "upgraded", installed: "1.2.4", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useHome(t)
			useMesh(t)
			dir := t.TempDir()
			t.Setenv("PATH", dir)
			program := filepath.Join(dir, daemonBinary)
			if tc.installed != "" {
				writeVersionedBinary(t, dir, daemonBinary, tc.installed)
			}
			started := useStartInstall(t)

			restaged, err := restageUpgrade(t.Context(), "1.2.3")
			if err != nil {
				t.Fatalf("restageUpgrade: %v", err)
			}
			if restaged != tc.want {
				t.Fatalf("restaged = %v, want %v", restaged, tc.want)
			}
			wantStarted := []string(nil)
			if tc.want {
				wantStarted = []string{program}
			}
			if fmt.Sprint(*started) != fmt.Sprint(wantStarted) {
				t.Fatalf("started install of %v, want %v", *started, wantStarted)
			}
		})
	}
}

func TestRestageUpgradeInstallsForADriftedHelperAndKeepsReconciling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish func(desired []byte) []byte
		binary  bool
		want    bool
	}{
		{name: "matching plist", publish: func(desired []byte) []byte { return desired }, binary: true},
		{name: "drifted plist", publish: func(desired []byte) []byte { return bytes.Repeat(desired, 2) }, binary: true, want: true},
		{name: "never installed", binary: true},
		{name: "uninstalled binary", publish: func(desired []byte) []byte { return desired }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useHome(t)
			useMesh(t)
			dir := usePathBinaries(t, "reposync")
			writeVersionedBinary(t, dir, daemonBinary, "1.2.3")
			writeManifest(t, "reposync")
			if tc.publish != nil {
				agent, err := helperAgent(helperManifest("reposync", ""))
				if err != nil {
					t.Fatal(err)
				}
				desired, err := agent.Plist()
				if err != nil {
					t.Fatal(err)
				}
				path, err := agent.PlistPath()
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, tc.publish(desired), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.binary {
				if err := os.Remove(filepath.Join(dir, "reposync")); err != nil {
					t.Fatal(err)
				}
			}
			started := useStartInstall(t)

			restaged, err := restageUpgrade(t.Context(), "1.2.3")
			if err != nil {
				t.Fatalf("restageUpgrade: %v", err)
			}
			if restaged {
				t.Fatal("restaged = true, want the tick to keep reconciling")
			}
			if got := len(*started) == 1; got != tc.want {
				t.Fatalf("started install of %v, want install %v", *started, tc.want)
			}
		})
	}
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
