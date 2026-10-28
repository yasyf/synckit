package daemon

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRestageUpgradeInstallsOnlyADifferentInstalledBuild(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(self) //nolint:gosec // the test binary itself
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		installed []byte
		want      bool
	}{
		{name: "absent"},
		{name: "current", installed: current},
		{name: "upgraded", installed: []byte("#!/bin/sh\n"), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useHome(t)
			useMesh(t)
			dir := t.TempDir()
			t.Setenv("PATH", dir)
			program := filepath.Join(dir, daemonBinary)
			if tc.installed != nil {
				if err := os.WriteFile(program, tc.installed, 0o700); err != nil { //nolint:gosec // test-owned temp dir
					t.Fatal(err)
				}
			}
			var started []string
			startInstall = func(program string) error {
				started = append(started, program)
				return nil
			}
			t.Cleanup(func() { startInstall = launchInstall })

			restaged, err := restageUpgrade()
			if err != nil {
				t.Fatalf("restageUpgrade: %v", err)
			}
			if restaged != tc.want {
				t.Fatalf("restaged = %v, want %v", restaged, tc.want)
			}
			if !tc.want {
				if len(started) != 0 {
					t.Fatalf("started install of %v, want none", started)
				}
				return
			}
			resolved, err := filepath.EvalSymlinks(program)
			if err != nil {
				t.Fatal(err)
			}
			if len(started) != 1 || started[0] != resolved {
				t.Fatalf("started install of %v, want [%s]", started, resolved)
			}
		})
	}
}

func TestRestageUpgradeInstallsForADriftedHelperAndKeepsReconciling(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(self) //nolint:gosec // the test binary itself
	if err != nil {
		t.Fatal(err)
	}
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
			useResidentStaging(t)
			dir := usePathBinaries(t, "reposync")
			if err := os.WriteFile(filepath.Join(dir, daemonBinary), current, 0o700); err != nil { //nolint:gosec // test-owned temp dir
				t.Fatal(err)
			}
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
			var started []string
			startInstall = func(program string) error {
				started = append(started, program)
				return nil
			}
			t.Cleanup(func() { startInstall = launchInstall })

			restaged, err := restageUpgrade()
			if err != nil {
				t.Fatalf("restageUpgrade: %v", err)
			}
			if restaged {
				t.Fatal("restaged = true, want the tick to keep reconciling")
			}
			if got := len(started) == 1; got != tc.want {
				t.Fatalf("started install of %v, want install %v", started, tc.want)
			}
		})
	}
}
